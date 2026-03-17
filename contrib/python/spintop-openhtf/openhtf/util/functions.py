# Copyright 2016 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities for functions."""

import collections
import functools
import inspect
import time


if not hasattr(inspect, "ArgSpec"):
    ArgSpec = collections.namedtuple('ArgSpec', 'args varargs keywords defaults')
else:
    ArgSpec = inspect.ArgSpec


def getargspec(func):
  fullargspec = inspect.getfullargspec(func)
  return ArgSpec(
    args=fullargspec.args, 
    varargs=fullargspec.varargs,
    keywords=fullargspec.varkw,
    defaults=fullargspec.defaults)
  

def call_once(func):
  """Decorate a function to only allow it to be called once.

  Note that it doesn't make sense to only call a function once if it takes
  arguments (use @functools.lru_cache for that sort of thing), so this only
  works on callables that take no args.
  """
  argspec = getargspec(func)
  if argspec.args or argspec.varargs or argspec.keywords:
    raise ValueError('Can only decorate functions with no args', func, argspec)

  @functools.wraps(func)
  def _wrapper():
    # If we haven't been called yet, actually invoke func and save the result.
    if not _wrapper.HasRun():
      _wrapper.MarkAsRun()
      _wrapper.return_value = func()
    return _wrapper.return_value

  _wrapper.has_run = False
  _wrapper.HasRun = lambda: _wrapper.has_run
  _wrapper.MarkAsRun = lambda: setattr(_wrapper, 'has_run', True)
  return _wrapper

def call_at_most_every(seconds, count=1):
  """Call the decorated function at most count times every seconds seconds.

  The decorated function will sleep to ensure that at most count invocations
  occur within any 'seconds' second window.
  """
  def decorator(func):
    try:
      call_history = getattr(func, '_call_history')
    except AttributeError:
      call_history = collections.deque(maxlen=count)
      setattr(func, '_call_history', call_history)

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
      current_time = time.time()
      window_count = sum(ts > current_time - seconds for ts in call_history)
      if window_count >= count:
        # We need to sleep until the relevant call is outside the window.  This
        # should only ever be the the first entry in call_history, but if we
        # somehow ended up with extra calls in the window, this recovers.
        time.sleep(call_history[window_count - count] - current_time + seconds)
      # Append this call, deque will automatically trim old calls using maxlen.
      call_history.append(time.time())
      return func(*args, **kwargs)
    return _wrapper
  return decorator
