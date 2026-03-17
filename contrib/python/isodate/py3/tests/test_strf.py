"""Test cases for the isodate module."""

import time
from datetime import datetime, timedelta

import pytest

from isodate import DT_EXT_COMPLETE, LOCAL, strftime, tzinfo

TEST_CASES = (
    (
        datetime(2012, 12, 25, 13, 30, 0, 0, LOCAL),
        DT_EXT_COMPLETE,
        "2012-12-25T13:30:00+10:00",
    ),
    # DST ON
    (
        datetime(1999, 12, 25, 13, 30, 0, 0, LOCAL),
        DT_EXT_COMPLETE,
        "1999-12-25T13:30:00+11:00",
    ),
    # microseconds
    (
        datetime(2012, 10, 12, 8, 29, 46, 69178),
        "%Y-%m-%dT%H:%M:%S.%f",
        "2012-10-12T08:29:46.069178",
    ),
    (
        datetime(2012, 10, 12, 8, 29, 46, 691780),
        "%Y-%m-%dT%H:%M:%S.%f",
        "2012-10-12T08:29:46.691780",
    ),
)


@pytest.fixture
def tz_patch(monkeypatch):
    # local time zone mock function
    localtime_orig = time.localtime

    def localtime_mock(secs):
        """Mock time to fixed date.

        Mock time.localtime so that it always returns a time_struct with tm_dst=1
        """
        tt = localtime_orig(secs)
        # before 2000 everything is dst, after 2000 no dst.
        if tt.tm_year < 2000:
            dst = 1
        else:
            dst = 0
        tt = (
            tt.tm_year,
            tt.tm_mon,
            tt.tm_mday,
            tt.tm_hour,
            tt.tm_min,
            tt.tm_sec,
            tt.tm_wday,
            tt.tm_yday,
            dst,
        )
        return time.struct_time(tt)

    monkeypatch.setattr(time, "localtime", localtime_mock)
    # assume LOC = +10:00
    monkeypatch.setattr(tzinfo, "STDOFFSET", timedelta(seconds=36000))
    # assume DST = +11:00
    monkeypatch.setattr(tzinfo, "DSTOFFSET", timedelta(seconds=39600))
    monkeypatch.setattr(tzinfo, "DSTDIFF", tzinfo.DSTOFFSET - tzinfo.STDOFFSET)


@pytest.mark.parametrize("dt, format, expectation", TEST_CASES)
def test_format(tz_patch, dt, format, expectation):
    """Take date object and create ISO string from it.

    This is the reverse test to test_parse.
    """
    if expectation is None:
        with pytest.raises(AttributeError):
            strftime(dt, format)
    else:
        assert strftime(dt, format) == expectation
