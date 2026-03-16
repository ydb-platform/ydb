# Copyright 2018-2023 Jetperch LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
The Python time standard uses POSIX (UNIX) time which is defined relative
to Jan 1, 1970.  Python uses floating point seconds to express time.
This module defines a much simpler 64-bit fixed point integer for
representing time which is much friendlier for microcontrollers.
The value is 34Q30 with the upper 34 bits
to represent whole seconds and the lower 30 bits to represent fractional
seconds.  A value of 2**30 (1 << 30) represents 1 second.  This
representation gives a resolution of 2 ** -30 (approximately 1 nanosecond)
and a range of +/- 2 ** 33 (approximately 272 years).  The value is
signed to allow for simple arithmetic on the time either as a fixed value
or as deltas.

For more details, see jsdrv/time.h.
"""


import datetime
import numpy as np
import time as pytime


EPOCH = datetime.datetime(2018, 1, 1, tzinfo=datetime.timezone.utc).timestamp()  # seconds
SCALE = (1 << 30)
SECOND = SCALE
MINUTE = SECOND * 60
HOUR = MINUTE * 60
DAY = HOUR * 24
YEAR = DAY * 365  # common year
GREGORIAN_YEAR = int(365.2425 * DAY)   # https://en.wikipedia.org/wiki/Year

MILLISECOND = SCALE // 1_000
MICROSECOND = SCALE // 1_000_000
NANOSECOND = SCALE // 1_000_000_000  # bad precision!


def now():
    """Get the current timestamp.

    :return: The time64 representation of the current time.
    """
    return as_time64(pytime.time())


def as_time64(t):
    """Convert a time to a Joulescope timestamp.

    :param t: The time which can be:
        * datetime.datetime
        * python timestamp
        * integer Joulescope timestamp
    """
    if isinstance(t, int) or isinstance(t, np.int64):
        return t
    if isinstance(t, datetime.datetime):
        t = t.timestamp()
    else:
        t = float(t)
    return int((t - EPOCH) * SCALE)


def as_timestamp(t):
    """Convert a time to a python UTC timestamp.

    :param t: The time which can be:
        * datetime.datetime
        * python timestamp
        * integer Joulescope timestamp
    """
    t64 = as_time64(t)
    return (t64 / SCALE) + EPOCH


def as_datetime(t):
    """Convert a time to a python UTC timestamp.

    :param t: The time which can be:
        * datetime.datetime
        * python timestamp
        * integer Joulescope timestamp
    """
    t = as_timestamp(t)
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)


def as_isostr(t):
    """Convert a time to an ISO 8601 string.

    :param t: The time in any format.
    :return: The time string.

    @see https://en.wikipedia.org/wiki/ISO_8601
    """
    t = as_datetime(t)
    return t.isoformat(timespec='microseconds').replace('+00:00', 'Z')


def filename(extension=None, t=None):
    """Construct a filename from the time.

    :param extension: The filename extension, such as '.png'.
        None (default) uses '.jls'.
    :param t: The time.  None (default) uses the current time.
    :return: The filename.
    """

    extension = '.jls' if extension is None else str(extension)
    if t is None:
        t = now()
    time_start = as_datetime(t)
    timestamp_str = time_start.strftime('%Y%m%d_%H%M%S')
    return f'{timestamp_str}{extension}'


def duration_to_seconds(d):
    """Convert a duration to float seconds.
    :param d: The duration specification, which is one of:

        * A string formatted as fz where f is a valid floating-point value
          and z is either omitted, 's', 'm', 'h', 'd'.
        * An integer in seconds.
        * A floating-point value in seconds.
    """
    if d is None:
        raise ValueError('cannot convert None')
    if isinstance(d, str):
        if not len(d):
            raise ValueError('cannot convert empty string')
        if d[-1] == 's':
            return float(d[:-1])
        elif d[-1] == 'm':
            return 60 * float(d[:-1])
        elif d[-1] == 'h':
            return 60 * 60 * float(d[:-1])
        elif d[-1] == 'd':
            return 60 * 60 * 24 * float(d[:-1])
        else:
            return float(d)
    else:
        return float(d)


def _local_offset():
    now = datetime.datetime.now()
    local_now = now.astimezone()
    local_tz = local_now.tzinfo
    print('hi')
    print(local_tz)


if __name__ == '__main__':
    _local_offset()
