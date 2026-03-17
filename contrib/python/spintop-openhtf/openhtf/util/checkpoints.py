# Copyright 2014 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""A simple utility to check whether all previous phases have passed.

In general test execution stops on a raised Exception but will continue if a
phase has a failed measurement.  This is desirable behavior in many cases
because you want to acquire additional information before the test stops.

However, in some cases it's less desirable because downstream long-running
phases will be executed only to FAIL at test completion due to the failing
measurements.  This can have negative implications for tact time.

Checkpoints allow test authors to have their cake and eat it too.  A checkpoint
checks the result of all previous phases and will stop test execution if a
previous phase has failed.
"""

from openhtf.core import phase_descriptor
from openhtf.core import test_record

def checkpoint(checkpoint_name=None):
  name = checkpoint_name if checkpoint_name else 'Checkpoint'

  @phase_descriptor.PhaseOptions(name=name)
  def _checkpoint(test_run):
    failed_phases = []
    for phase_record in test_run.test_record.phases:
      if phase_record.outcome == test_record.PhaseOutcome.FAIL:
        failed_phases.append(phase_record.name)

    if failed_phases:
      test_run.logger.error('Stopping execution because phases failed: %s',
                            failed_phases)
      return phase_descriptor.PhaseResult.STOP

  return _checkpoint
