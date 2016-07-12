# Copyright (C) 2014 Cloudablity
# 
# Licensed under the Apache License, Version 2.0 (the "License"); 
# you may not use this file except in compliance with the License. 
# You may obtain a copy of the License at 
# 
#      http://www.apache.org/licenses/LICENSE-2.0 
# 
# Unless required by applicable law or agreed to in writing, software 
# distributed under the License is distributed on an "AS IS" BASIS, 
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
# See the License for the specific language governing permissions and 
# limitations under the License.

from croniter import croniter
from datetime import datetime
import argparse
import boto.ec2
import logging
 
logger = None

# Use a time tolerance (epsilon) to allow some fuzzy matching to the schedule
TIME_EPSILON = 300 # 5 minutes
DRY_RUN = False

class InstanceMeta:
  """InstanceMeta holds interesting metadata about an instance
  """

  def __init__(self, instance_id, name, state, cron_schedules):
    self.instance_id    =  instance_id
    self.name           =  name
    self.state          =  state
    self.cron_schedules =  cron_schedules


def setup_logging(log_path, debug=False):
  """Setup logging configuration

  Args:
    log_path: string
    debug: boolean
  """

  default_level = logging.DEBUG if debug else logging.INFO

  if log_path is None:
    logging.basicConfig(level=default_level)
  else:
    log_file = '%s/valet.log' % log_path
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=log_file, filemode='a', format=log_format, level=logging.INFO)

  global logger
  logger = logging.getLogger(__name__)

def parse_instances(instances):
  """Load instances with schedules from tagged EC2 instances

  Args:
    instances: list
  Returns:
    A list of InstanceMeta objects
  """
  logger.info("Checking %s instances" % len(instances))

  schedules = []

  for instance in instances:
    instance_id    = instance.id
    running_state  = instance.state
    cron_schedules = None
    name = None

    for tag_name, tag_value in instance.tags.iteritems():
      if tag_name.lower() == "schedule":
        cron_schedules = tag_value.split('\n|\\n')
      elif tag_name.lower() == "name":
        name = tag_value
      if cron_schedules is not None and name is not None:
        break

    schedules.append(InstanceMeta(instance_id, name, running_state, cron_schedules))
  return schedules

def start_instances(ec2, to_start=None):
  """Ensure instances are running

  Args:
    ec2: boto ec2 object
    to_start: list, instances to start
  """
  if to_start:
    logger.info('Starting instances: %s' % to_start)

    if not DRY_RUN:
      ec2.start_instances(to_start)
  else:
    logger.debug('Nothing to start')

def stop_instances(ec2, to_stop=None):
  """Ensure instances are stopped

  Args:
    ec2: boto ec2 object
    to_stop: list, instances to stop
  """
  if to_stop:
    logger.info('Stopping instances: %s' % to_stop)

    if not DRY_RUN:
      ec2.stop_instances(to_stop)
  else:
    logger.debug('Nothing to stop')

def manage_instances(region):
  """Manager instances findable by AWS API

  Args:
    region: list, regions to query for instances
  """
  logging.debug("Checking %s" % region)

  ec2 = boto.ec2.connect_to_region(region)

  # AWS is case sensitive, so check for the common cases manually
  filters = { 'tag-key': ['Schedule', 'schedule'], 'instance-state-name': ['running', 'stopped'] }
  scheduled_instances = ec2.get_only_instances(filters=filters)
  parsed_instances = parse_instances(scheduled_instances)

  base = datetime.now()
  base = base.replace(second=0, microsecond=0)

  to_start = []
  to_stop  = []

  for instance in parsed_instances:
    should_be_running = False

    for cron_schedule in instance.cron_schedules:
      logger.debug("Checking schedule: %s" % cron_schedule)
      next_run = croniter(cron_schedule, base).get_next(datetime)
      logger.debug("Next run: %s" % next_run)

      gap = (next_run - base).total_seconds()
      if gap < TIME_EPSILON:
        should_be_running = True
        logger.debug("%s (%s) - Fire it up!" % (instance.name, instance.instance_id))

    if should_be_running:
      if instance.state == 'stopped':
        to_start.append(instance.instance_id)
    elif instance.state == 'running':
      to_stop.append(instance.instance_id)
      logger.debug("%s (%s)  - That's all folks!" % (instance.name, instance.instance_id))

  start_instances(ec2, to_start)
  stop_instances(ec2, to_stop)

def main():
  parser = argparse.ArgumentParser(description='Check for scheduled AWS instances to start and stop.')
  parser.add_argument('--dry-run', help='Perform a dry run', action='store_true')
  parser.add_argument('--debug', help='Turn on debug logging', action='store_true')
  parser.add_argument('-l', '--log', help='Directory where logs should be placed')
  parser.add_argument('-r', '--regions', nargs='*', help='Which regions to check', default=['us-east-1'])
  args = parser.parse_args()

  if args.dry_run:
    global DRY_RUN
    DRY_RUN = True

  setup_logging(log_path=args.log, debug=args.debug)

  for region in args.regions:
    manage_instances(region)

if __name__ == "__main__":
  main()

