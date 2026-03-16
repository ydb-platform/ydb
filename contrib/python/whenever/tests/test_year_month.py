import pickle
import re
from copy import copy, deepcopy

import pytest

from whenever import Date, YearMonth

from .common import AlwaysEqual, AlwaysLarger, AlwaysSmaller, NeverEqual


class TestInit:

    def test_valid(self):
        assert YearMonth(2021, 12) is not None
        assert YearMonth(1, 1) is not None
        assert YearMonth(9999, 12) is not None
        assert YearMonth(year=2002, month=2) is not None

    @pytest.mark.parametrize(
        "year, month",
        [
            (2021, 13),
            (3000, 0),
            (2000, -1),
            (0, 3),
            (10_000, 3),
        ],
    )
    def test_invalid_combinations(self, year, month):
        with pytest.raises(ValueError):
            YearMonth(year, month)

    def test_invalid(self):
        with pytest.raises(TypeError):
            YearMonth(2000)  # type: ignore[call-arg]

        with pytest.raises(TypeError):
            YearMonth("2001", "SEP")  # type: ignore[arg-type]

        with pytest.raises(TypeError):
            YearMonth()  # type: ignore[call-arg]


def test_properties():
    ym = YearMonth(2021, 12)
    assert ym.year == 2021
    assert ym.month == 12


def test_eq():
    ym = YearMonth(2021, 12)
    same = YearMonth(2021, 12)
    different = YearMonth(2021, 11)

    assert ym == same
    assert not ym == different
    assert not ym == NeverEqual()
    assert ym == AlwaysEqual()

    assert not ym != same
    assert ym != different
    assert ym != NeverEqual()
    assert not ym != AlwaysEqual()
    assert ym != None  # noqa: E711
    assert None != ym  # noqa: E711
    assert not ym == None  # noqa: E711
    assert not None == ym  # noqa: E711

    assert hash(ym) == hash(same)


def test_comparison():
    ym = YearMonth(2021, 5)
    same = YearMonth(2021, 5)
    bigger = YearMonth(2022, 2)
    smaller = YearMonth(2020, 12)

    assert ym <= same
    assert ym <= bigger
    assert not ym <= smaller
    assert ym <= AlwaysLarger()
    assert not ym <= AlwaysSmaller()

    assert not ym < same
    assert ym < bigger
    assert not ym < smaller
    assert ym < AlwaysLarger()
    assert not ym < AlwaysSmaller()

    assert ym >= same
    assert not ym >= bigger
    assert ym >= smaller
    assert not ym >= AlwaysLarger()
    assert ym >= AlwaysSmaller()

    assert not ym > same
    assert not ym > bigger
    assert ym > smaller
    assert not ym > AlwaysLarger()
    assert ym > AlwaysSmaller()


def test_format_common_iso():
    assert YearMonth(2021, 12).format_common_iso() == "2021-12"
    assert YearMonth(2, 1).format_common_iso() == "0002-01"


def test_str():
    assert str(YearMonth(2021, 12)) == "2021-12"
    assert str(YearMonth(2, 1)) == "0002-01"


def test_repr():
    assert repr(YearMonth(2021, 12)) == "YearMonth(2021-12)"
    assert repr(YearMonth(2, 1)) == "YearMonth(0002-01)"


class TestParseCommonIso:

    @pytest.mark.parametrize(
        "s, expected",
        [
            ("2021-01", YearMonth(2021, 1)),
            ("0014-12", YearMonth(14, 12)),
            ("001412", YearMonth(14, 12)),
        ],
    )
    def test_valid(self, s, expected):
        assert YearMonth.parse_common_iso(s) == expected

    @pytest.mark.parametrize(
        "s",
        [
            "202A-01",  # non-digit
            "2021-01T03:04:05",  # with a time
            "2021-01-02",  # with a day
            "2021-1",  # no padding
            "21-01",  # no padding
            "2020-123",  # ordinal date
            "2020-003",  # ordinal date
            "2020-W12",  # week date
            "20-12",  # two-digit year
            "120-12",  # three-digit year
            "-012-12",  # negative year
            "312🧨-12",  # non-ASCII
            "202𝟙-11",  # non-ascii
        ],
    )
    def test_invalid(self, s):
        with pytest.raises(
            ValueError,
            match=r"Invalid format.*" + re.escape(repr(s)),
        ):
            YearMonth.parse_common_iso(s)

    def test_no_string(self):
        with pytest.raises((TypeError, AttributeError), match="(int|str)"):
            YearMonth.parse_common_iso(20210102)  # type: ignore[arg-type]


def test_replace():
    ym = YearMonth(2021, 1)
    assert ym.replace(year=2022) == YearMonth(2022, 1)
    assert ym.replace(month=2) == YearMonth(2021, 2)
    assert ym == YearMonth(2021, 1)  # original is unchanged

    with pytest.raises(TypeError):
        ym.replace(3)  # type: ignore[misc]

    with pytest.raises(TypeError, match="foo"):
        ym.replace(foo=3)  # type: ignore[call-arg]

    with pytest.raises(TypeError, match="day"):
        ym.replace(day=3)  # type: ignore[call-arg]

    with pytest.raises(TypeError, match="foo"):
        ym.replace(foo="blabla")  # type: ignore[call-arg]

    with pytest.raises(ValueError, match="(date|year)"):
        ym.replace(year=10_000)


def test_on_day():
    ym = YearMonth(2021, 1)
    assert ym.on_day(3) == Date(2021, 1, 3)
    assert ym.on_day(31) == Date(2021, 1, 31)

    with pytest.raises(ValueError):
        ym.on_day(0)

    with pytest.raises(ValueError):
        ym.on_day(-9)

    with pytest.raises(ValueError):
        ym.on_day(10_000)

    ym2 = YearMonth(2009, 2)
    assert ym2.on_day(28) == Date(2009, 2, 28)
    with pytest.raises(ValueError):
        ym2.on_day(29)

    ym3 = YearMonth(2016, 2)
    assert ym3.on_day(29) == Date(2016, 2, 29)


def test_copy():
    ym = YearMonth(2021, 1)
    assert copy(ym) is ym
    assert deepcopy(ym) is ym


def test_singletons():
    assert YearMonth.MIN == YearMonth(1, 1)
    assert YearMonth.MAX == YearMonth(9999, 12)


def test_pickling():
    d = YearMonth(2021, 1)
    dumped = pickle.dumps(d)
    assert pickle.loads(dumped) == d


def test_unpickle_compatibility():
    dumped = (
        b"\x80\x04\x95$\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\t_unpkl_y"
        b"m\x94\x93\x94C\x03\xe5\x07\x01\x94\x85\x94R\x94."
    )
    assert pickle.loads(dumped) == YearMonth(2021, 1)


def test_cannot_subclass():
    with pytest.raises(TypeError):

        class SubclassDate(YearMonth):  # type: ignore[misc]
            pass
