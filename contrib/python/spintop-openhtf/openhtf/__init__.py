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


"""The main OpenHTF entry point."""

import pkg_resources
import signal

from openhtf import plugs
from openhtf.core import phase_executor
from openhtf.core import test_record
from openhtf.core.measurements import Dimension
from openhtf.core.measurements import Measurement
from openhtf.core.measurements import measures
from openhtf.core.monitors import monitors
from openhtf.core.phase_descriptor import PhaseDescriptor
from openhtf.core.phase_descriptor import PhaseOptions
from openhtf.core.phase_descriptor import PhaseResult
from openhtf.core.phase_group import PhaseGroup
from openhtf.core.test_descriptor import Test
from openhtf.core.test_descriptor import TestApi
from openhtf.core.test_descriptor import TestDescriptor
from openhtf.plugs import plug
from openhtf.util import conf
from openhtf.util import console_output
from openhtf.util import data
from openhtf.util import functions
from openhtf.util import logs
from openhtf.util import units

# TODO: TestPhase is used for legacy reasons and should be deprecated.
TestPhase = PhaseOptions  # pylint: disable=invalid-name


def get_version():
  """Returns the version string of the 'openhtf' package.

  Note: the version number doesn't seem to get properly set when using ipython.
  """
  try:
    return pkg_resources.get_distribution('openhtf')
  except pkg_resources.DistributionNotFound:
    return 'Unknown - Perhaps openhtf was not installed via setup.py or pip.'

__version__ = get_version()

# Register signal handler to stop all tests on SIGINT.
signal.signal(signal.SIGINT, Test.handle_sig_int)
