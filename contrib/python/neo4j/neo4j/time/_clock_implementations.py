# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import time
from ctypes import (
    byref,
    c_long,
    c_longlong,
    CDLL,
    Structure,
)
from platform import uname

from . import (
    _Clock,
    _ClockTime,
)
from ._arithmetic import nano_divmod


__all__ = [
    "LibCClock",
    "PEP564Clock",
    "SafeClock",
]


class SafeClock(_Clock):
    """
    Clock implementation that should work for any variant of Python.

    This clock is guaranteed microsecond precision.
    """

    @classmethod
    def precision(cls):
        return 6

    @classmethod
    def available(cls):
        return True

    def utc_time(self):
        seconds, nanoseconds = nano_divmod(int(time.time() * 1000000), 1000000)
        return _ClockTime(seconds, nanoseconds * 1000)


class PEP564Clock(_Clock):
    """
    Clock implementation based on the PEP564 additions to Python 3.7.

    This clock is guaranteed nanosecond precision.
    """

    @classmethod
    def precision(cls):
        return 9

    @classmethod
    def available(cls):
        return hasattr(time, "time_ns")

    def utc_time(self):
        t = time.time_ns()
        seconds, nanoseconds = divmod(t, 1000000000)
        return _ClockTime(seconds, nanoseconds)


class LibCClock(_Clock):
    """
    Clock implementation backed by libc.

    Only works on platforms that provide libc.
    This clock is guaranteed nanosecond precision.
    """

    __libc = "libc.dylib" if uname()[0] == "Darwin" else "libc.so.6"

    class _TimeSpec(Structure):
        _fields_ = (
            ("seconds", c_longlong),
            ("nanoseconds", c_long),
        )

    @classmethod
    def precision(cls):
        return 9

    @classmethod
    def available(cls):
        try:
            _ = CDLL(cls.__libc)
        except OSError:
            return False
        else:
            return True

    def utc_time(self):
        libc = CDLL(self.__libc)
        ts = self._TimeSpec()
        status = libc.clock_gettime(0, byref(ts))
        if status == 0:
            return _ClockTime(ts.seconds, ts.nanoseconds)
        else:
            raise RuntimeError(f"clock_gettime failed with status {status}")
