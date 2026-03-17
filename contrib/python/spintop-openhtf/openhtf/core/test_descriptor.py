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

# Modified by Tack Verification, 2020


"""Tests in OpenHTF.

Tests are main entry point for OpenHTF tests.  In its simplest form a
test is a series of Phases that are executed by the OpenHTF framework.

"""
import argparse
import collections
import logging
import os
import sys
import textwrap
import threading
from types import LambdaType
import uuid
import weakref

import colorama
import mutablerecords

from openhtf import util
from openhtf.core import phase_descriptor
from openhtf.core import phase_executor
from openhtf.core import phase_group
from openhtf.core import test_executor
from openhtf.core import test_record

from openhtf.util import conf
from openhtf.util import console_output
from openhtf.util import logs

import six

_LOG = logging.getLogger(__name__)

conf.declare('capture_source', description=textwrap.dedent(
    '''Whether to capture the source of phases and the test module.  This
    defaults to False since this potentially reads many files and makes large
    string copies. If True, will capture docstring also.

    Set to 'true' if you want to capture your test's source.'''),
             default_value=False)

conf.declare('capture_docstring', description=textwrap.dedent(
    '''Whether to capture the docstring of phases and the test module. 
    If True, will capture docstring.

    Set to 'true' if you want to capture your test's docstring.'''),
             default_value=False)

# TODO(arsharma): Deprecate this configuration after removing the old teardown
# specification.
conf.declare('teardown_timeout_s', default_value=30, description=
             'Default timeout (in seconds) for test teardown functions; '
             'this option is deprecated and only applies to the deprecated '
             'Test level teardown function.')

def should_create_code_info():
  return conf.capture_docstring or conf.capture_source

def should_capture_source():
  return conf.capture_source

class UnrecognizedTestUidError(Exception):
  """Raised when information is requested about an unknown Test UID."""


class InvalidTestPhaseError(Exception):
  """Raised when an invalid method is decorated."""


class InvalidTestStateError(Exception):
  """Raised when an operation is attempted in an invalid state."""


def create_arg_parser(add_help=False):
  """Creates an argparse.ArgumentParser for parsing command line flags.

  If you want to add arguments, create your own with this as a parent:

  >>> parser = argparse.ArgumentParser(
          'My args title', parents=[openhtf.create_arg_parser()])
  >>> parser.parse_args()

  Args:
    add_help: boolean option passed through to arg parser.

  Returns:
    an `argparse.ArgumentParser`

  """
  parser = argparse.ArgumentParser(
      'OpenHTF-based testing',
      parents=[
          conf.ARG_PARSER,
          console_output.ARG_PARSER,
          logs.ARG_PARSER,
          phase_executor.ARG_PARSER,
      ],
      add_help=add_help)
  parser.add_argument(
      '--config-help', action='store_true',
      help='Instead of executing the test, simply print all available config '
      'keys and their description strings.')
  return parser


class Test(object):
  """An object that represents an OpenHTF test.

  Example:

    def PhaseOne(test):
      # Integrate more widgets

    def PhaseTwo(test):
      # Analyze widget integration status

    Test(PhaseOne, PhaseTwo).execute()

  Note that Test() objects *must* be created in the main thread, but can be
  .execute()'d in a separate thread.
  """

  TEST_INSTANCES = weakref.WeakValueDictionary()
  HANDLED_SIGINT_ONCE = False
  ARG_PARSER_FACTORY = create_arg_parser

  def __init__(self, *phases, _code_info_after_file=None, **metadata):
    # Some sanity checks on special metadata keys we automatically fill in.
    if 'config' in metadata:
      raise KeyError(
          'Invalid metadata key "config", it will be automatically populated.')

    self.created_time_millis = util.time_millis()
    self.last_run_time_millis = None
    self._test_options = TestOptions()
    self._lock = threading.Lock()
    self._executor = None
    self._test_desc = TestDescriptor(
        phases, test_record.CodeInfo.uncaptured(), metadata)

    if should_create_code_info():
      # First, we copy the phases with the real CodeInfo for them.
      group = self._test_desc.phase_group.load_code_info(with_source=should_capture_source())

      # Then we replace the TestDescriptor with one that stores the test
      # module's CodeInfo as well as our newly copied phases.
      if _code_info_after_file is None:
        _code_info_after_file = __file__
        
      # Extract the module level doc from the first file in the stack after _code_info_after_file
      # Unless specified, this is file calling this constructor.
      code_info = test_record.CodeInfo.for_module_from_stack(after_file=_code_info_after_file, with_source=should_capture_source())
      self._test_desc = self._test_desc._replace(
          code_info=code_info, phase_group=group)

    # Make sure configure() gets called at least once before Execute().  The
    # user might call configure() again to override options, but we don't want
    # to force them to if they want to use defaults.  For default values, see
    # the class definition of TestOptions.
    if 'test_name' in metadata:
      # Allow legacy metadata key for specifying test name.
      self.configure(name=metadata['test_name'])
    else:
      self.configure()

  @classmethod
  def from_uid(cls, test_uid):
    """Get Test by UID.

    Args:
      test_uid:  uuid for desired test.

    Returns:
      Test object for given by UID.

    Raises:
      UnrecognizedTestUidError: If the test_uid is not recognized.
    """
    test = cls.TEST_INSTANCES.get(test_uid)
    if not test:
      raise UnrecognizedTestUidError('Test UID %s not recognized' % test_uid)
    return test

  @property
  def uid(self):
    if self._executor is not None:
      return self._executor.uid

  def make_uid(self):
    """Returns the next test execution's UID.

    This identifier must be unique but trackable across invocations of
    execute(). Therefore, it's made of four parts separated by ':'
    * Process-specific (decided on process start up)
    * Test descriptor-specific (decided on descriptor creation)
    * Execution-specific (decided on test start)
    """
    return '%s:%s:%s:%s' % (os.getpid(), self.descriptor.uid,
                            uuid.uuid4().hex[:16], util.time_millis())

  @property
  def descriptor(self):
    """Static data about this test, does not change across Execute() calls."""
    return self._test_desc

  @property
  def state(self):
    """Transient state info about the currently executing test, or None."""
    with self._lock:
      if self._executor:
        return self._executor.test_state

  def get_option(self, option):
    return getattr(self._test_options, option)

  def add_output_callbacks(self, *callbacks):
    """Add the given function as an output module to this test."""
    self._test_options.output_callbacks.extend(callbacks)

  def configure(self, **kwargs):
    """Update test-wide configuration options. See TestOptions for docs."""
    # These internally ensure they are safe to call multiple times with no weird
    # side effects.
    known_args, _ = self.__class__.ARG_PARSER_FACTORY(add_help=True).parse_known_args()
    self.known_args_hook(known_args)
    logs.configure_logging()
    for key, value in six.iteritems(kwargs):
      setattr(self._test_options, key, value)

  def known_args_hook(self, known_args):
    if known_args.config_help:
      sys.stdout.write(conf.help_text)
      sys.exit(0)

  @classmethod
  def handle_sig_int(cls, *_):
    if cls.TEST_INSTANCES:
      _LOG.error('Received SIGINT, stopping all tests.')
      for test in cls.TEST_INSTANCES.values():
        test.abort_from_sig_int()
    if not cls.HANDLED_SIGINT_ONCE:
      cls.HANDLED_SIGINT_ONCE = True
      raise KeyboardInterrupt
    # Otherwise, does not raise KeyboardInterrupt to ensure that the tests are
    # cleaned up.

  def abort_from_sig_int(self):
    """Abort test execution abruptly, only in response to SIGINT."""
    with self._lock:
      _LOG.error('Aborting %s due to SIGINT', self)
      if self._executor:
        # TestState str()'s nicely to a descriptive string, so let's log that
        # just for good measure.
        _LOG.error('Test state: %s', self._executor.test_state)
        self._executor.abort()

  # TODO(arsharma): teardown_function test option is deprecated; remove this.
  def _get_running_test_descriptor(self):
    """If there is a teardown_function, wrap current descriptor with it."""
    if not self._test_options.teardown_function:
      return self._test_desc

    teardown_phase = phase_descriptor.PhaseDescriptor.wrap_or_copy(
        self._test_options.teardown_function)
    if not teardown_phase.options.timeout_s:
      teardown_phase.options.timeout_s = conf.teardown_timeout_s
    return TestDescriptor(
        phase_group.PhaseGroup(main=[self._test_desc.phase_group],
                               teardown=[teardown_phase]),
        self._test_desc.code_info, self._test_desc.metadata)

  def execute(self, test_start=None):
    """Starts the framework and executes the given test.

    Args:
      test_start: Either a trigger phase for starting the test, or a function
                  that returns a DUT ID. If neither is provided, defaults to not
                  setting the DUT ID.
    Returns:
      Boolean indicating whether the test failed (False) or passed (True).

    Raises:
      InvalidTestStateError: if this test is already being executed.
    """
    # Lock this section so we don't .stop() the executor between instantiating
    # it and .Start()'ing it, doing so does weird things to the executor state.
    with self._lock:
      # Sanity check to make sure someone isn't doing something weird like
      # trying to Execute() the same test twice in two separate threads.  We
      # hold the lock between here and Start()'ing the executor to guarantee
      # that only one thread is successfully executing the test.
      if self._executor:
        raise InvalidTestStateError('Test already running', self._executor)

      # Snapshot some things we care about and store them.
      self._test_desc.metadata['test_name'] = self._test_options.name
      self._test_desc.metadata['config'] = conf._asdict()
      self.last_run_time_millis = util.time_millis()

      if isinstance(test_start, LambdaType):
        @phase_descriptor.PhaseOptions()
        def trigger_phase(test):
          test.test_record.dut_id = test_start()
        trigger = trigger_phase
      else:
        trigger = test_start

      if should_create_code_info() and trigger:
        trigger.code_info = test_record.CodeInfo.for_function(trigger.func, with_source=should_capture_source())

      test_desc = self._get_running_test_descriptor()
      self._executor = test_executor.TestExecutor(
          test_desc, self.make_uid(), trigger, self._test_options)

      _LOG.info('Executing test: %s', self.descriptor.code_info.name)
      self.TEST_INSTANCES[self.uid] = self
      self._executor.start()

    try:
      self._executor.wait()
    except KeyboardInterrupt:
      # The SIGINT handler only raises the KeyboardInterrupt once, so only retry
      # that once.
      self._executor.wait()
      raise
    finally:
      try:
        final_state = self._executor.finalize()

        _LOG.debug('Test completed for %s, outputting now.',
                   final_state.test_record.metadata['test_name'])
        for output_cb in self._test_options.output_callbacks:
          try:
            output_cb(final_state.test_record)
          except Exception:  # pylint: disable=broad-except
            _LOG.exception(
                'Output callback %s raised; continuing anyway', output_cb)
        # Make sure the final outcome of the test is printed last and in a
        # noticeable color so it doesn't get scrolled off the screen or missed.
        if final_state.test_record.outcome == test_record.Outcome.ERROR:
          for detail in final_state.test_record.outcome_details:
            console_output.error_print(detail.description)
        else:
          colors = collections.defaultdict(lambda: colorama.Style.BRIGHT)
          colors[test_record.Outcome.PASS] = ''.join((colorama.Style.BRIGHT,
                                                      colorama.Fore.GREEN))
          colors[test_record.Outcome.FAIL] = ''.join((colorama.Style.BRIGHT,
                                                      colorama.Fore.RED))
          msg_template = 'test: {name}  outcome: {color}{outcome}{rst}'
          console_output.banner_print(msg_template.format(
              name=final_state.test_record.metadata['test_name'],
              color=colors[final_state.test_record.outcome],
              outcome=final_state.test_record.outcome.name,
              rst=colorama.Style.RESET_ALL))
      finally:
        del self.TEST_INSTANCES[self.uid]
        self._executor.close()
        self._executor = None

    return final_state.test_record.outcome == test_record.Outcome.PASS


# TODO(arsharma): Deprecate the teardown_function in favor of PhaseGroups.
class TestOptions(mutablerecords.Record('TestOptions', [], {
    'name': 'openhtf_test',
    'output_callbacks': list,
    'teardown_function': None,
    'failure_exceptions': list,
    'default_dut_id': 'UNKNOWN_DUT',
    'stop_on_first_failure': False
})):
  """Class encapsulating various tunable knobs for Tests and their defaults.

  name: The name of the test to be put into the metadata.
  output_callbacks: List of output callbacks to run, typically it's better to
      use add_output_callbacks(), but you can pass [] here to reset them.
  teardown_function: Function to run at teardown.  We pass the same arguments to
      it as a phase.
  failure_exceptions: Exceptions to cause a test FAIL instead of ERROR. When a
      test run exits early due to an exception, the run will be marked as a FAIL
      if the raised exception matches one of the types in this list. Otherwise,
      the run is marked as ERROR.
  default_dut_id: The DUT ID that will be used if the start trigger and all
      subsequent phases fail to set one.
  stop_on_first_failure: Stop Test on first failed measurement.
  """


class TestDescriptor(collections.namedtuple(
    'TestDescriptor', ['phase_group', 'code_info', 'metadata', 'uid'])):
  """An object that represents the reusable portions of an OpenHTF test.

  This object encapsulates the static test information that is set once and used
  by the framework along the way.

  Attributes:
    phase_group: The top level phase group to execute for this Test.
    metadata: Any metadata that should be associated with test records.
    code_info: Information about the module that created the Test.
    uid: UID for this test.
  """

  def __new__(cls, phases, code_info, metadata):
    group = phase_group.PhaseGroup.convert_if_not(phases)
    return super(TestDescriptor, cls).__new__(
        cls, group, code_info, metadata, uid=uuid.uuid4().hex[:16])

  @property
  def plug_types(self):
    """Returns set of plug types required by this test."""
    return {plug.cls
            for phase in self.phase_group
            for plug in phase.plugs}


class TestApi(collections.namedtuple('TestApi', [
    'logger', 'state', 'test_record', 'measurements', 'attachments',
    'attach', 'attach_from_file', 'get_measurement', 'get_attachment',
    'notify_update'])):
  """Class passed to test phases as the first argument.

  Attributes:
    dut_id: This attribute provides getter and setter access to the DUT ID
        of the device under test by the currently running openhtf.Test.  A
        non-empty DUT ID *must* be set by the end of a test, or no output
        will be produced.  It may be set via return value from a callable
        test_start argument to openhtf.Test.Execute(), or may be set in a
        test phase via this attribute.

    logger: A Python Logger instance that can be used to log to the resulting
        TestRecord.  This object supports all the usual log levels, and
        outputs to stdout (configurable) and the frontend via the Station
        API, if it's enabled, in addition to the 'log_records' attribute
        of the final TestRecord output by the running test.

    measurements: A measurements.Collection object used to get/set
        measurement values.  See util/measurements.py for more implementation
        details, but in the simple case, set measurements directly as
        attributes on this object (see examples/measurements.py for examples).

    state: A dict (initially empty) that is persisted across test phases (but
        resets for every invocation of Execute() on an openhtf.Test).  This
        can be used for any test-wide state you need to persist across phases.
        Use this with caution, however, as it is not persisted in the output
        TestRecord or displayed on the web frontend in any way.

    test_record: A reference to the output TestRecord for the currently
        running openhtf.Test.  Direct access to this attribute is *strongly*
        discouraged, but provided as a catch-all for interfaces not otherwise
        provided by TestApi.  If you find yourself using this, please file a
        feature request for an alternative at:
          https://github.com/google/openhtf/issues/new

  Callable Attributes:
    attach: Attach binary data to the test, see TestState.attach().

    attach_from_file: Attach binary data from a file, see
        TestState.attach_from_file().

    get_attachment:  Get copy of attachment contents from current or previous
        phase, see TestState.get_attachement.

    get_measurement: Get copy of a measurement from a current or previous phase,
        see TestState.get_measurement().

    notify_update: Notify any frontends of an interesting update. Typically
        this is automatically called internally when interesting things happen,
        but it can be called by the user (takes no args), for instance if
        modifying test_record directly.



  Read-only Attributes:
    attachments: Dict mapping attachment name to test_record.Attachment
        instance containing the data that was attached (and the MIME type
        that was assumed based on extension, if any).  Only attachments
        that have been attached in the current phase show up here, and this
        attribute should not be modified directly; use TestApi.attach() or
        TestApi.attach_from_file() instead.
  """

  @property
  def dut_id(self):
    return self.test_record.dut_id

  @dut_id.setter
  def dut_id(self, dut_id):
    if self.test_record.dut_id:
      self.logger.warning('Overriding previous DUT ID "%s" with "%s".',
                          self.test_record.dut_id, dut_id)
    self.test_record.dut_id = dut_id
    self.notify_update()
