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


"""PhaseExecutor module for handling the phases of a test.

Each phase is an instance of openhtf.PhaseDescriptor and therefore has
relevant options. Each option is taken into account when executing a phase,
such as checking options.run_if as soon as possible and timing out at the
appropriate time.

A phase must return an openhtf.PhaseResult, one of CONTINUE, REPEAT, or STOP.
A phase may also return None, or have no return statement, which is the same as
returning openhtf.PhaseResult.CONTINUE.  These results are then acted upon
accordingly and a new test run status is returned.

Phases are always run in order and not allowed to loop back, though a phase may
choose to repeat itself by returning REPEAT. Returning STOP will cause a test
to stop early, allowing a test to detect a bad state and not waste any further
time. A phase should not return TIMEOUT or ABORT, those are handled by the
framework.
"""

import collections
import logging
import sys
import threading
import time
import traceback

import openhtf
from openhtf.util import argv
from openhtf.util import threads
from openhtf.util import timeouts

DEFAULT_PHASE_TIMEOUT_S = 365 * 24 * 60 * 60

ARG_PARSER = argv.ModuleParser()
ARG_PARSER.add_argument(
    '--phase_default_timeout_s', default=DEFAULT_PHASE_TIMEOUT_S,
    action=argv.StoreInModule, target='%s.DEFAULT_PHASE_TIMEOUT_S' % __name__,
    help='Test phase timeout in seconds')

_LOG = logging.getLogger(__name__)


class ExceptionInfo(collections.namedtuple(
    'ExceptionInfo', ['exc_type', 'exc_val', 'exc_tb'])):
  """Wrap the description of a raised exception and its traceback."""

  def _asdict(self):
    return {
        'exc_type': str(self.exc_type),
        'exc_val': self.exc_val,
        'exc_tb': ''.join(traceback.format_exception(*self)),
    }

  def __str__(self):
    return self.exc_type.__name__


class InvalidPhaseResultError(Exception):
  """Raised when PhaseExecutionOutcome is created with invalid phase result."""


class PhaseExecutionOutcome(collections.namedtuple(
    'PhaseExecutionOutcome', 'phase_result')):
  """Provide some utility and sanity around phase return values.

  This should not be confused with openhtf.PhaseResult.  PhaseResult is an
  enumeration to provide user-facing valid phase return values.  This tuple
  is used internally to track other possible outcomes (timeout, exception),
  and to perform some sanity checking (weird return values from phases).

  If phase_result is None, that indicates the phase timed out (this makes
  sense if you think about it, it timed out, so there was no result).  If
  phase_result is an instance of Exception, then that is the Exception that
  was raised by the phase.  The raised_exception attribute can be used as
  a convenience to test for that condition, and the is_timeout attribute can
  similarly be used to check for the timeout case.

  The only accepted values for phase_result are None (timeout), an instance
  of Exception (phase raised), or an instance of openhtf.PhaseResult.  Any
  other value will raise an InvalidPhaseResultError.
  """

  def __new__(cls, phase_result):
    if (phase_result is not None and
        not isinstance(phase_result, (openhtf.PhaseResult, ExceptionInfo)) and
        not isinstance(phase_result, threads.ThreadTerminationError)):
      raise InvalidPhaseResultError('Invalid phase result', phase_result)
    self = super(PhaseExecutionOutcome, cls).__new__(cls, phase_result)
    return self

  @property
  def is_fail_and_continue(self):
    return self.phase_result is openhtf.PhaseResult.FAIL_AND_CONTINUE

  @property
  def is_repeat(self):
    return self.phase_result is openhtf.PhaseResult.REPEAT

  @property
  def is_skip(self):
    return self.phase_result is openhtf.PhaseResult.SKIP

  @property
  def is_terminal(self):
    """True if this result will stop the test."""
    return (self.raised_exception or self.is_timeout or
            self.phase_result == openhtf.PhaseResult.STOP)

  @property
  def is_timeout(self):
    """True if this PhaseExecutionOutcome indicates a phase timeout."""
    return self.phase_result is None

  @property
  def raised_exception(self):
    """True if the phase in question raised an exception."""
    return isinstance(self.phase_result, (
        ExceptionInfo, threads.ThreadTerminationError))


class PhaseExecutorThread(threads.KillableThread):
  """Handles the execution and result of a single test phase.

  The phase outcome will be stored in the _phase_execution_outcome attribute
  once it is known (_phase_execution_outcome is None until then), and it will be
  a PhaseExecutionOutcome instance.
  """
  daemon = True

  def __init__(self, phase_desc, test_state):
    super(PhaseExecutorThread, self).__init__(
        name='<PhaseExecutorThread: (phase_desc.name)>')
    self._phase_desc = phase_desc
    self._test_state = test_state
    self._phase_execution_outcome = None

  def _thread_proc(self):
    """Execute the encompassed phase and save the result."""
    # Call the phase, save the return value, or default it to CONTINUE.
    phase_return = self._phase_desc(self._test_state)
    if phase_return is None:
      phase_return = openhtf.PhaseResult.CONTINUE

    # If phase_return is invalid, this will raise, and _phase_execution_outcome
    # will get set to the InvalidPhaseResultError in _thread_exception instead.
    self._phase_execution_outcome = PhaseExecutionOutcome(phase_return)

  def _log_exception(self, *args):
    """Log exception, while allowing unit testing to override."""
    self._test_state.state_logger.critical(*args)

  def _thread_exception(self, *args):
    self._phase_execution_outcome = PhaseExecutionOutcome(ExceptionInfo(*args))
    self._log_exception('Phase %s raised an exception', self._phase_desc.name)
    return True  # Never propagate exceptions upward.

  def join_or_die(self):
    """Wait for thread to finish, returning a PhaseExecutionOutcome instance."""
    if self._phase_desc.options.timeout_s is not None:
      threads.join_timeout_increments(self, self._phase_desc.options.timeout_s)
    else:
      threads.join_timeout_increments(self, DEFAULT_PHASE_TIMEOUT_S)

    # We got a return value or an exception and handled it.
    if isinstance(self._phase_execution_outcome, PhaseExecutionOutcome):
      return self._phase_execution_outcome

    # Check for timeout, indicated by None for
    # PhaseExecutionOutcome.phase_result.
    if self.is_alive():
      self.kill()
      return PhaseExecutionOutcome(None)

    # Phase was killed.
    return PhaseExecutionOutcome(threads.ThreadTerminationError())

  @property
  def name(self):
    return str(self)

  def __str__(self):
    return '<%s: (%s)>' % (type(self).__name__, self._phase_desc.name)


class PhaseExecutor(object):
  """Encompasses the execution of the phases of a test."""

  def __init__(self, test_state):
    self.test_state = test_state
    # This lock exists to prevent stop() calls from being ignored if called when
    # _execute_phase_once is setting up the next phase thread.
    self._current_phase_thread_lock = threading.Lock()
    self._current_phase_thread = None
    self._stopping = threading.Event()

  def execute_phase(self, phase):
    """Executes a phase or skips it, yielding PhaseExecutionOutcome instances.

    Args:
      phase: Phase to execute.

    Returns:
      The final PhaseExecutionOutcome that wraps the phase return value
      (or exception) of the final phase run. All intermediary results, if any,
      are REPEAT and handled internally. Returning REPEAT here means the phase
      hit its limit for repetitions.
    """
    repeat_count = 1
    repeat_limit = phase.options.repeat_limit or sys.maxsize
    while not self._stopping.is_set():
      is_last_repeat = repeat_count >= repeat_limit
      phase_execution_outcome = self._execute_phase_once(phase, is_last_repeat)

      if phase_execution_outcome.is_repeat and not is_last_repeat:
        repeat_count += 1
        continue

      return phase_execution_outcome
    # We've been cancelled, so just 'timeout' the phase.
    return PhaseExecutionOutcome(None)

  def _execute_phase_once(self, phase_desc, is_last_repeat):
    """Executes the given phase, returning a PhaseExecutionOutcome."""
    # Check this before we create a PhaseState and PhaseRecord.
    if not phase_desc.options.call_run_if(self.test_state.user_defined_state):
      _LOG.debug('Phase %s skipped due to run_if returning falsey.',
                 phase_desc.name)
      return PhaseExecutionOutcome(openhtf.PhaseResult.SKIP)

    override_result = None
    with self.test_state.running_phase_context(phase_desc) as phase_state:
      _LOG.debug('Executing phase %s', phase_desc.name)
      with self._current_phase_thread_lock:
        # Checking _stopping must be in the lock context, otherwise there is a
        # race condition: this thread checks _stopping and then switches to
        # another thread where stop() sets _stopping and checks
        # _current_phase_thread (which would not be set yet).  In that case, the
        # new phase thread will be still be started.
        if self._stopping.is_set():
          # PhaseRecord will be written at this point, so ensure that it has a
          # Killed result.
          result = PhaseExecutionOutcome(threads.ThreadTerminationError())
          phase_state.result = result
          return result
        phase_thread = PhaseExecutorThread(phase_desc, self.test_state)
        phase_thread.start()
        self._current_phase_thread = phase_thread

      phase_state.result = phase_thread.join_or_die()
      if phase_state.result.is_repeat and is_last_repeat:
        _LOG.error('Phase returned REPEAT, exceeding repeat_limit.')
        phase_state.hit_repeat_limit = True
        override_result = PhaseExecutionOutcome(openhtf.PhaseResult.STOP)
      self._current_phase_thread = None

    # Refresh the result in case a validation for a partially set measurement
    # raised an exception.
    result = override_result or phase_state.result
    _LOG.debug('Phase %s finished with result %s', phase_desc.name,
               result.phase_result)
    return result

  def reset_stop(self):
    self._stopping.clear()

  def stop(self, timeout_s=None):
    """Stops execution of the current phase, if any.

    It will raise a ThreadTerminationError, which will cause the test to stop
    executing and terminate with an ERROR state.

    Args:
      timeout_s: int or None, timeout in seconds to wait for the phase to stop.
    """
    self._stopping.set()
    with self._current_phase_thread_lock:
      phase_thread = self._current_phase_thread
      if not phase_thread:
        return

    if phase_thread.is_alive():
      phase_thread.kill()

      _LOG.debug('Waiting for cancelled phase to exit: %s', phase_thread)
      timeout = timeouts.PolledTimeout.from_seconds(timeout_s)
      while phase_thread.is_alive() and not timeout.has_expired():
        time.sleep(0.1)
      _LOG.debug('Cancelled phase %s exit',
                 "didn't" if phase_thread.is_alive() else 'did')
    # Clear the currently running phase, whether it finished or timed out.
    self.test_state.stop_running_phase()
