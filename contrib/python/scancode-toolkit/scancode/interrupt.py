#
# Copyright (c) nexB Inc. and others. All rights reserved.
# http://nexb.com and https://github.com/nexB/scancode-toolkit/
# The ScanCode software is licensed under the Apache License version 2.0.
#
# You may not use this software except in compliance with the License.
# You may obtain a copy of the License at: http://apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
#


from traceback import format_exc as traceback_format_exc

from commoncode.system import on_windows

"""
This module povides an interruptible() function to run a callable and stop it if
it does not return after a timeout for both Windows (using threads) and
POSIX/Linux/macOS (using signals).

interruptible() calls the `func` function with `args` and `kwargs` arguments and
return a tuple of (error, value). `func` is invoked through an OS-specific
wrapper and will be interrupted if it does not return within `timeout` seconds.

- `func` returned `value` MUST BE pickable.
- `timeout` is an int of seconds defaults to DEFAULT_TIMEOUT.
- `args` and `kwargs` are passed to `func` as *args and **kwarg.

In the returned tuple of (`error`, `value`) we can have these two cases:

 - if the function did run correctly and completed within `timeout` seconds then
   `error` is None and `value` contains the returned value.

 - if the function raised en exception or did not complete within `timeout`
   seconds then `error` is a verbose error message that contains a full
   traceback and `value` is None.
"""


class TimeoutError(Exception):  # NOQA
    pass


DEFAULT_TIMEOUT = 120  # seconds

TIMEOUT_MSG = 'ERROR: Processing interrupted: timeout after %(timeout)d seconds.'
ERROR_MSG = 'ERROR: Unknown error:\n'
NO_ERROR = None
NO_VALUE = None

if not on_windows:
    """
    Some code based in part and inspired from the RobotFramework and
    heavily modified.

    Copyright 2008-2015 Nokia Networks
    Copyright 2016-     Robot Framework Foundation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
    implied. See the License for the specific language governing
    permissions and limitations under the License.
    """

    from signal import ITIMER_REAL
    from signal import SIGALRM
    from signal import setitimer
    from signal import signal as create_signal

    def interruptible(func, args=None, kwargs=None, timeout=DEFAULT_TIMEOUT):
        """
        POSIX, signals-based interruptible runner.
        """

        def handler(signum, frame):
            raise TimeoutError

        try:
            create_signal(SIGALRM, handler)
            setitimer(ITIMER_REAL, timeout)
            return NO_ERROR, func(*(args or ()), **(kwargs or {}))

        except TimeoutError:
            return TIMEOUT_MSG % locals(), NO_VALUE

        except Exception:
            return ERROR_MSG + traceback_format_exc(), NO_VALUE

        finally:
            setitimer(ITIMER_REAL, 0)

elif on_windows:
    """
    Run a function in an interruptible thread with a timeout.
    Based on an idea of dano "Dan O'Reilly"
    http://stackoverflow.com/users/2073595/dano
    But no code has been reused from this post.
    """

    from ctypes import c_long
    from ctypes import py_object
    from ctypes import pythonapi
    from multiprocessing import TimeoutError as MpTimeoutError

    from queue import Empty as Queue_Empty
    from queue import Queue
    from _thread import start_new_thread

    def interruptible(func, args=None, kwargs=None, timeout=DEFAULT_TIMEOUT):
        """
        Windows, threads-based interruptible runner. It can work also on
        POSIX, but is not reliable and works only if everything is pickable.
        """
        # We run `func` in a thread and block on a queue until timeout
        results = Queue()

        def runner():
            """
            Run the func and send results back in a queue as a tuple of
            (`error`, `value`)
            """
            try:
                _res = func(*(args or ()), **(kwargs or {}))
                results.put((NO_ERROR, _res,))
            except Exception:
                results.put((ERROR_MSG + traceback_format_exc(), NO_VALUE,))

        tid = start_new_thread(runner, ())

        try:
            # wait for the queue results up to timeout
            err_res = results.get(timeout=timeout)

            if not err_res:
                return ERROR_MSG, NO_VALUE

            return err_res

        except (Queue_Empty, MpTimeoutError):
            return TIMEOUT_MSG % locals(), NO_VALUE

        except Exception:
            return ERROR_MSG + traceback_format_exc(), NO_VALUE

        finally:
            try:
                async_raise(tid, Exception)
            except (SystemExit, ValueError):
                pass

    def async_raise(tid, exctype=Exception):
        """
        Raise an Exception in the Thread with id `tid`. Perform cleanup if
        needed.

        Based on Killable Threads By Tomer Filiba
        from http://tomerfiliba.com/recipes/Thread2/
        license: public domain.
        """
        assert isinstance(tid, int), 'Invalid  thread id: must an integer'

        tid = c_long(tid)
        exception = py_object(Exception)
        res = pythonapi.PyThreadState_SetAsyncExc(tid, exception)
        if res == 0:
            raise ValueError('Invalid thread id.')
        elif res != 1:
            # if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect
            pythonapi.PyThreadState_SetAsyncExc(tid, 0)
            raise SystemError('PyThreadState_SetAsyncExc failed.')


def fake_interruptible(func, args=None, kwargs=None, timeout=DEFAULT_TIMEOUT):
    """
    Fake interruptible that is not interruptible and has no timeout and is using
    no threads and no signals This implementation is used for debugging. This
    ignores the timeout and just runs the function as-is.
    """

    try:
        return NO_ERROR, func(*(args or ()), **(kwargs or {}))
    except Exception:
        return ERROR_MSG + traceback_format_exc(), NO_VALUE
