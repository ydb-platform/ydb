from __future__ import annotations

import datetime
import time as _time
from datetime import tzinfo, timedelta

ZERO = timedelta(0)
HOUR = timedelta(hours=1)


# A class building tzinfo objects for fixed-offset time zones.
# Note that FixedOffset(0, "UTC") is a different way to build a
# UTC tzinfo object.


class FixedOffsetTimezone(tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset: float, name: str | None=None) -> None:
        self.__offset = timedelta(minutes=offset)
        self.__name = name

    def utcoffset(self, dt: datetime.datetime | None) -> timedelta:
        return self.__offset

    def tzname(self, dt: datetime.datetime | None) -> str | None:
        return self.__name

    def dst(self, dt: datetime.datetime | None) -> timedelta:
        return ZERO


utc = FixedOffsetTimezone(offset=0, name="UTC")


STDOFFSET = timedelta(seconds=-_time.timezone)
if _time.daylight:
    DSTOFFSET = timedelta(seconds=-_time.altzone)
else:
    DSTOFFSET = STDOFFSET

DSTDIFF = DSTOFFSET - STDOFFSET


class LocalTimezone(tzinfo):
    def utcoffset(self, dt: datetime.datetime | None) -> timedelta:
        if self._isdst(dt):
            return DSTOFFSET
        else:
            return STDOFFSET

    def dst(self, dt: datetime.datetime | None) -> timedelta:
        if self._isdst(dt):
            return DSTDIFF
        else:
            return ZERO

    def tzname(self, dt: datetime.datetime | None) -> str:
        return _time.tzname[self._isdst(dt)]

    def _isdst(self, dt: datetime.datetime | None) -> bool:
        if not dt:
            return False
        tt = (
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.weekday(),
            0,
            0,
        )
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0


local = LocalTimezone()
