import pickle
import re
from copy import copy, deepcopy

import pytest

from whenever import Date, MonthDay

from .common import AlwaysEqual, AlwaysLarger, AlwaysSmaller, NeverEqual


class TestInit:

    def test_valid(self):
        assert MonthDay(12, 3) is not None
        assert MonthDay(1, 1) is not None
        assert MonthDay(12, 31) is not None
        assert MonthDay(2, 29) is not None

    @pytest.mark.parametrize(
        "month, day",
        [
            (13, 1),
            (2, 30),
            (8, 32),
            (0, 3),
            (10_000, 3),
        ],
    )
    def test_invalid_combinations(self, month, day):
        with pytest.raises(ValueError):
            MonthDay(month, day)

    def test_invalid(self):
        with pytest.raises(TypeError):
            MonthDay(2)  # type: ignore[call-arg]

        with pytest.raises(TypeError):
            MonthDay("20", "SEP")  # type: ignore[arg-type]

        with pytest.raises(TypeError):
            MonthDay()  # type: ignore[call-arg]


def test_properties():
    md = MonthDay(12, 14)
    assert md.month == 12
    assert md.day == 14


def test_is_leap():
    assert MonthDay(2, 29).is_leap()
    assert not MonthDay(2, 28).is_leap()
    assert not MonthDay(3, 1).is_leap()
    assert not MonthDay(1, 1).is_leap()
    assert not MonthDay(12, 31).is_leap()


def test_eq():
    md = MonthDay(10, 12)
    same = MonthDay(10, 12)
    different = MonthDay(10, 11)

    assert md == same
    assert not md == different
    assert not md == NeverEqual()
    assert md == AlwaysEqual()

    assert not md != same
    assert md != different
    assert md != NeverEqual()
    assert not md != AlwaysEqual()
    assert md != None  # noqa: E711
    assert None != md  # noqa: E711
    assert not md == None  # noqa: E711
    assert not None == md  # noqa: E711

    assert hash(md) == hash(same)


def test_comparison():
    md = MonthDay(7, 5)
    same = MonthDay(7, 5)
    bigger = MonthDay(8, 2)
    smaller = MonthDay(6, 12)

    assert md <= same
    assert md <= bigger
    assert not md <= smaller
    assert md <= AlwaysLarger()
    assert not md <= AlwaysSmaller()

    assert not md < same
    assert md < bigger
    assert not md < smaller
    assert md < AlwaysLarger()
    assert not md < AlwaysSmaller()

    assert md >= same
    assert not md >= bigger
    assert md >= smaller
    assert not md >= AlwaysLarger()
    assert md >= AlwaysSmaller()

    assert not md > same
    assert not md > bigger
    assert md > smaller
    assert not md > AlwaysLarger()
    assert md > AlwaysSmaller()


def test_format_common_iso():
    assert MonthDay(11, 12).format_common_iso() == "--11-12"
    assert MonthDay(2, 1).format_common_iso() == "--02-01"


def test_str():
    assert str(MonthDay(10, 31)) == "--10-31"
    assert str(MonthDay(2, 1)) == "--02-01"


def test_repr():
    assert repr(MonthDay(11, 12)) == "MonthDay(--11-12)"
    assert repr(MonthDay(2, 1)) == "MonthDay(--02-01)"


class TestParseCommonIso:

    @pytest.mark.parametrize(
        "s, expected",
        [
            ("--08-21", MonthDay(8, 21)),
            ("--10-02", MonthDay(10, 2)),
            # basic format
            ("--1002", MonthDay(10, 2)),
        ],
    )
    def test_valid(self, s, expected):
        assert MonthDay.parse_common_iso(s) == expected

    @pytest.mark.parametrize(
        "s",
        [
            "--2A-01",  # non-digit
            "--11-01T03:04:05",  # with a time
            "2021-01-02",  # with a year
            "--11-1",  # no padding
            "--1-13",  # no padding
            "W12-04",  # week date
            "03-12",  # no dashes
            "-10-12",  # not enough dashes
            "---12-03",  # negative month
            "--1🧨-12",  # non-ASCII
            "--1𝟙-11",  # non-ascii
            # invalid components
            "--00-01",
            "--13-01",
            "--11-00",
            "--11-31",
        ],
    )
    def test_invalid(self, s):
        with pytest.raises(
            ValueError,
            match=r"Invalid format.*" + re.escape(repr(s)),
        ):
            MonthDay.parse_common_iso(s)

    def test_no_string(self):
        with pytest.raises((TypeError, AttributeError), match="(int|str)"):
            MonthDay.parse_common_iso(20210102)  # type: ignore[arg-type]


def test_replace():
    md = MonthDay(12, 31)
    assert md.replace(month=8) == MonthDay(8, 31)
    assert md.replace(day=8) == MonthDay(12, 8)
    assert md == MonthDay(12, 31)  # original is unchanged

    with pytest.raises(ValueError, match="(day|month|date)"):
        md.replace(month=2)

    with pytest.raises(ValueError, match="(date|day)"):
        md.replace(day=32)

    with pytest.raises(TypeError):
        md.replace(3)  # type: ignore[misc]

    with pytest.raises(TypeError, match="foo"):
        md.replace(foo=3)  # type: ignore[call-arg]

    with pytest.raises(TypeError, match="year"):
        md.replace(year=2000)  # type: ignore[call-arg]

    with pytest.raises(TypeError, match="foo"):
        md.replace(foo="blabla")  # type: ignore[call-arg]

    with pytest.raises(ValueError, match="(date|month)"):
        md.replace(month=13)


def test_in_year():
    md = MonthDay(12, 28)
    assert md.in_year(2000) == Date(2000, 12, 28)
    assert md.in_year(4) == Date(4, 12, 28)

    with pytest.raises(ValueError):
        md.in_year(0)

    with pytest.raises(ValueError):
        md.in_year(10_000)

    with pytest.raises(ValueError):
        md.in_year(-1)

    leap_day = MonthDay(2, 29)
    assert leap_day.in_year(2000) == Date(2000, 2, 29)
    with pytest.raises(ValueError):
        leap_day.in_year(2001)


def test_copy():
    md = MonthDay(5, 1)
    assert copy(md) is md
    assert deepcopy(md) is md


def test_singletons():
    assert MonthDay.MIN == MonthDay(1, 1)
    assert MonthDay.MAX == MonthDay(12, 31)


def test_pickling():
    d = MonthDay(11, 1)
    dumped = pickle.dumps(d)
    assert pickle.loads(dumped) == d


def test_unpickle_compatibility():
    dumped = (
        b"\x80\x04\x95#\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\t_unpkl_m"
        b"d\x94\x93\x94C\x02\x0b\x01\x94\x85\x94R\x94."
    )
    assert pickle.loads(dumped) == MonthDay(11, 1)


def test_cannot_subclass():
    with pytest.raises(TypeError):

        class SubclassDate(MonthDay):  # type: ignore[misc]
            pass
