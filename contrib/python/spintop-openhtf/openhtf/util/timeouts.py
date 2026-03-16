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


"""A simple utility to do timeout checking."""

import contextlib
import functools
import logging
import threading
import time

_LOG = logging.getLogger(__name__)

class PolledTimeout(object):
  """An object which tracks if a timeout has expired."""

  def __init__(self, timeout_s):
    """Construct a PolledTimeout object.

    Args:
      timeout_s: This may either be a number or None. If a number, this object
        will consider to be expired after number seconds after construction. If
        None, this object never expires.
    """
    self.start = time.time()
    self.timeout_s = timeout_s

  @classmethod
  def from_millis(cls, timeout_ms):
    """Create a new PolledTimeout if needed.

    If timeout_ms is already a PolledTimeout, just return it, otherwise create a
    new PolledTimeout with the given timeout in milliseconds.

    Args:
      timeout_ms: PolledTimeout object, or number of milliseconds to use for
        creating a new one.

    Returns:
      A PolledTimeout object that will expire in timeout_ms milliseconds, which
    may be timeout_ms itself, or a newly allocated PolledTimeout.
    """
    if hasattr(timeout_ms, 'has_expired'):
      return timeout_ms
    if timeout_ms is None:
      return cls(None)
    return cls(timeout_ms / 1000.0)

  @classmethod
  def from_seconds(cls, timeout_s):
    """Create a new PolledTimeout if needed.

    If timeout_s is already a PolledTimeout, just return it, otherwise create a
    new PolledTimeout with the given timeout in seconds.

    Args:
      timeout_s: PolledTimeout object, or number of seconds to use for creating
    a new one.

    Returns:
      A PolledTimeout object that will expire in timeout_s seconds, which may
    be timeout_s itself, or a newly allocated PolledTimeout.
    """
    if hasattr(timeout_s, 'has_expired'):
      return timeout_s
    return cls(timeout_s)

  def restart(self):
    """Restarts the timeout. Initializing the start time to now."""
    self.start = time.time()

  def expire(self):
    """Expire the timeout immediately."""
    self.timeout_s = 0

  def has_expired(self):
    """Returns True if the timeout has expired."""
    if self.timeout_s is None:
      return False
    return self.seconds >= self.timeout_s

  # Bad API. alusco is sometimes bad at naming.
  Poll = has_expired  # pylint: disable=invalid-name

  # pylint: disable=missing-docstring
  @property
  def seconds(self):
    return time.time() - self.start

  @property
  def remaining(self):
    if self.timeout_s is None:
      return None
    # We max() against 0 to ensure we don't return a (slightly) negative number.
    # This reduces races between .has_expired() calls and sleeping/waiting
    # .remaining seconds.
    return max(0, self.timeout_s - self.seconds)

  @property
  def remaining_ms(self):
    if self.timeout_s is None:
      return None
    return self.remaining * 1000

  # pylint: disable=missing-docstring


# There's now no way to tell if a timeout occurred generically
# which sort of sucks (for generic validation fn)
def loop_until_timeout_or_valid(timeout_s, function, validation_fn, sleep_s=1):  # pylint: disable=invalid-name
  """Loops until the specified function returns valid or a timeout is reached.

  Note: The function may return anything which, when passed to validation_fn,
  evaluates to implicit True.  This function will loop calling the function as
  long as the result of validation_fn(function_result) returns something which
  evaluates to False. We ensure function is called at least once regardless
  of timeout.

  Args:
    timeout_s: The number of seconds to wait until a timeout condition is
        reached. As a convenience, this accepts None to mean never timeout.  Can
        also be passed a PolledTimeout object instead of an integer.
    function: The function to call each iteration.
    validation_fn: The validation function called on the function result to
        determine whether to keep looping.
    sleep_s: The number of seconds to wait after calling the function.

  Returns:
    Whatever the function returned last.
  """
  if timeout_s is None or not hasattr(timeout_s, 'has_expired'):
    timeout_s = PolledTimeout(timeout_s)

  while True:
    # Calls the function at least once
    result = function()
    if validation_fn(result) or timeout_s.has_expired():
      return result
    time.sleep(sleep_s)


def loop_until_timeout_or_true(timeout_s, function, sleep_s=1):  # pylint: disable=invalid-name
  """Loops until the specified function returns True or a timeout is reached.

  Note: The function may return anything which evaluates to implicit True.  This
  function will loop calling it as long as it continues to return something
  which evaluates to False.  We ensure this method is called at least once
  regardless of timeout.

  Args:
    timeout_s: The number of seconds to wait until a timeout condition is
        reached. As a convenience, this accepts None to mean never timeout.  Can
        also be passed a PolledTimeout object instead of an integer.
    function: The function to call each iteration.
    sleep_s: The number of seconds to wait after calling the function.

  Returns:
    Whatever the function returned last.
  """
  return loop_until_timeout_or_valid(timeout_s, function, lambda x: x, sleep_s)


def loop_until_timeout_or_not_none(timeout_s, function, sleep_s=1):  # pylint: disable=invalid-name
  """Loops until the specified function returns non-None or until a timeout.

  Args:
    timeout_s: The number of seconds to wait until a timeout condition is
        reached. As a convenience, this accepts None to mean never timeout.  Can
        also be passed a PolledTimeout object instead of an integer.
    function: The function to call each iteration.
    sleep_s: The number of seconds to wait after calling the function.

  Returns:
    Whatever the function returned last.
  """
  return loop_until_timeout_or_valid(
      timeout_s, function, lambda x: x is not None, sleep_s)


def loop_until_true_else_raise(timeout_s,
                               function,
                               invert=False,
                               message=None,
                               sleep_s=1):
  """Repeatedly call the given function until truthy, or raise on a timeout.

  Args:
    timeout_s: The number of seconds to wait until a timeout condition is
        reached. As a convenience, this accepts None to mean never timeout. Can
        also be passed a PolledTimeout object instead of an integer.
    function: The function to call each iteration.
    invert: If True, wait for the callable to return falsey instead of truthy.
    message: Optional custom error message to use on a timeout.
    sleep_s: Seconds to sleep between call attempts.

  Returns:
    The final return value of the function.

  Raises:
    RuntimeError if the timeout is reached before the function returns truthy.
  """
  def validate(x):
    return bool(x) != invert

  result = loop_until_timeout_or_valid(timeout_s, function, validate, sleep_s=1)
  if validate(result):
    return result

  if message is not None:
    raise RuntimeError(message)

  name = '(unknown)'
  if hasattr(function, '__name__'):
    name = function.__name__
  elif (isinstance(function, functools.partial)
        and hasattr(function.func, '__name__')):
    name = function.func.__name__
  raise RuntimeError(
      'Function %s failed to return %s within %d seconds.'
      % (name, 'falsey' if invert else 'truthy', timeout_s)) 


class Interval(object):
  """An object which can execute a method on an interval."""

  def __init__(self, method, stop_if_false=False):
    """Initializes the Interval.

    Args:
      method: A callable to execute, it should take no arguments.
      stop_if_false: If True, the interval will exit if the method returns
      False.
    """
    self.method = method
    self.stopped = threading.Event()
    self.thread = None
    self.stop_if_false = stop_if_false

  @property
  def running(self):
    if self.thread:
      return self.thread.isAlive()
    return False

  def start(self, interval_s):
    """Starts executing the method at the specified interval.

    Args:
      interval_s: The amount of time between executions of the method.
    Returns:
      False if the interval was already running.
    """
    if self.running:
      return False

    self.stopped.clear()

    def _execute():
      # Always execute immediately once
      if not self.method() and self.stop_if_false:
        return
      while not self.stopped.wait(interval_s):
        if not self.method() and self.stop_if_false:
          return

    self.thread = threading.Thread(target=_execute)
    self.thread.daemon = True
    self.thread.start()
    return True

  def stop(self, timeout_s=None):
    """Stops the interval.

    If a timeout is provided and stop returns False then the thread is
    effectively abandoned in whatever state it was in (presumably dead-locked).

    Args:
      timeout_s: The time in seconds to wait on the thread to finish.  By
          default it's forever.
    Returns:
      False if a timeout was provided and we timed out.
    """
    self.stopped.set()
    if self.thread:
      self.thread.join(timeout_s)
      return not self.thread.isAlive()
    else:
      return True

  def join(self, timeout_s=None):
    """Joins blocking until the interval ends or until timeout is reached.

    Args:
      timeout_s: The time in seconds to wait, defaults to forever.
    Returns:
      True if the interval is still running and we reached the timeout.
    """
    if not self.thread:
      return False
    self.thread.join(timeout_s)
    return self.running


def execute_forever(method, interval_s):  # pylint: disable=invalid-name
  """Executes a method forever at the specified interval.

  Args:
    method: The callable to execute.
    interval_s: The number of seconds to start the execution after each method
        finishes.
  Returns:
    An Interval object.
  """
  interval = Interval(method)
  interval.start(interval_s)
  return interval


def execute_until_false(method, interval_s):  # pylint: disable=invalid-name
  """Executes a method forever until the method returns a false value.

  Args:
    method: The callable to execute.
    interval_s: The number of seconds to start the execution after each method
        finishes.
  Returns:
    An Interval object.
  """
  interval = Interval(method, stop_if_false=True)
  interval.start(interval_s)
  return interval


# pylint: disable=invalid-name
def retry_until_true_or_limit_reached(method, limit, sleep_s=1,
                                      catch_exceptions=()):
  """Executes a method until the retry limit is hit or True is returned."""
  return retry_until_valid_or_limit_reached(
      method, limit, lambda x: x, sleep_s, catch_exceptions)


def retry_until_not_none_or_limit_reached(method, limit, sleep_s=1,
                                          catch_exceptions=()):
  """Executes a method until the retry limit is hit or not None is returned."""
  return retry_until_valid_or_limit_reached(
      method, limit, lambda x: x is not None, sleep_s, catch_exceptions)


def retry_until_valid_or_limit_reached(method, limit, validation_fn, sleep_s=1,
                                       catch_exceptions=()):
  """Executes a method until the retry limit or validation_fn returns True.

  The method is always called once so the effective lower limit for 'limit' is
  1.  Passing in a number less than 1 will still result it the method being
  called once.

  Args:
    method: The method to execute should take no arguments.
    limit: The number of times to try this method.  Must be >0.
    validation_fn: The validation function called on the function result to
        determine whether to keep looping.
    sleep_s: The time to sleep in between invocations.
    catch_exceptions: Tuple of exception types to catch and count as failures.
  Returns:
      Whatever the method last returned, implicit False would indicate the
        method never succeeded.
  """
  assert limit > 0, 'Limit must be greater than 0'

  def _execute_method(helper):
    try:
      return method()
    except catch_exceptions:
      if not helper.remaining:
        raise
      return None

  helper = RetryHelper(limit - 1)
  result = _execute_method(helper)
  while not validation_fn(result) and helper.retry_if_possible():
    time.sleep(sleep_s)
    result = _execute_method(helper)
  return result

# pylint: disable=invalid-name


@contextlib.contextmanager
def take_at_least_n_seconds(time_s):
  """A context manager which ensures it takes at least time_s to execute.

  Example:
    with take_at_least_n_seconds(5):
      do.Something()
      do.SomethingElse()
    # if Something and SomethingElse took 3 seconds, the with block with sleep
    # for 2 seconds before exiting.
  Args:
    time_s: The number of seconds this block should take.  If it doesn't take at
      least this time, then this method blocks during __exit__.
  Yields:
    To do some actions then on completion waits the remaining time.
  """
  timeout = PolledTimeout(time_s)
  yield
  while not timeout.has_expired():
    time.sleep(timeout.remaining)


def take_at_most_n_seconds(time_s, func, *args, **kwargs):
  """A function that returns whether a function call took less than time_s.

  NOTE: The function call is not killed and will run indefinitely if hung.

  Args:
    time_s: Maximum amount of time to take.
    func: Function to call.
    *args: Arguments to call the function with.
    **kwargs: Keyword arguments to call the function with.
  Returns:
    True if the function finished in less than time_s seconds.
  """
  thread = threading.Thread(target=func, args=args, kwargs=kwargs)
  thread.start()
  thread.join(time_s)
  if thread.is_alive():
    return False
  return True


def execute_after_delay(time_s, func, *args, **kwargs):
  """A function that executes the given function after a delay.

  Executes func in a separate thread after a delay, so that this function
  returns immediately.  Note that any exceptions raised by func will be
  ignored (but logged).  Also, if time_s is a PolledTimeout with no expiration,
  then this method simply returns immediately and does nothing.

  Args:
    time_s: Delay in seconds to wait before executing func, may be a
      PolledTimeout object.
    func: Function to call.
    *args: Arguments to call the function with.
    **kwargs: Keyword arguments to call the function with.
  """
  timeout = PolledTimeout.from_seconds(time_s)

  def target():
    time.sleep(timeout.remaining)
    try:
      func(*args, **kwargs)
    except Exception:  # pylint: disable=broad-except
      _LOG.exception('Error executing %s after %s expires.', func, timeout)
  if timeout.remaining is not None:
    thread = threading.Thread(target=target)
    thread.start()


class RetryHelper(object):
  """A helper with to simplify retrying.

  Attributes:
    remaining: The remaining number of retries.
  """

  def __init__(self, retries):
    """Initializes this object.

    Args:
      retries: The number of retries to allow.
    """
    self.remaining = retries

  def retry_if_possible(self):
    """Decrements a retry.

    Returns:
      True if you should proceed, or False if you're out of retries.
    """
    self.remaining -= 1
    return self.remaining >= 0
