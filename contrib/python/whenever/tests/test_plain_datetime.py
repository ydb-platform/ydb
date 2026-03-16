import pickle
import re
from datetime import datetime as py_datetime, timezone

import pytest
from hypothesis import given
from hypothesis.strategies import floats, integers, text

from whenever import (
    Date,
    ImplicitlyIgnoringDST,
    Instant,
    OffsetDateTime,
    PlainDateTime,
    RepeatedTime,
    SkippedTime,
    SystemDateTime,
    Time,
    ZonedDateTime,
    days,
    hours,
    months,
    nanoseconds,
    seconds,
    weeks,
    years,
)

from .common import (
    AlwaysEqual,
    AlwaysLarger,
    AlwaysSmaller,
    NeverEqual,
    system_tz_ams,
)


def test_minimal():
    d = PlainDateTime(2020, 8, 15, 5, 12, 30, nanosecond=450)

    assert d.year == 2020
    assert d.month == 8
    assert d.day == 15
    assert d.hour == 5
    assert d.minute == 12
    assert d.second == 30
    assert d.nanosecond == 450

    assert (
        PlainDateTime(2020, 8, 15, 12)
        == PlainDateTime(2020, 8, 15, 12, 0)
        == PlainDateTime(2020, 8, 15, 12, 0, 0)
        == PlainDateTime(2020, 8, 15, 12, 0, 0, nanosecond=0)
    )


def test_components():
    d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_123)
    assert d.date() == Date(2020, 8, 15)
    assert d.time() == Time(23, 12, 9, nanosecond=987_654_123)


def test_assume_utc():
    assert PlainDateTime(2020, 8, 15, 23).assume_utc() == Instant.from_utc(
        2020, 8, 15, 23
    )


def test_assume_fixed_offset():
    assert (
        PlainDateTime(2020, 8, 15, 23)
        .assume_fixed_offset(hours(5))
        .exact_eq(OffsetDateTime(2020, 8, 15, 23, offset=5))
    )
    assert (
        PlainDateTime(2020, 8, 15, 23)
        .assume_fixed_offset(-2)
        .exact_eq(OffsetDateTime(2020, 8, 15, 23, offset=-2))
    )


class TestAssumeTz:
    def test_typical(self):
        d = PlainDateTime(2020, 8, 15, 23)
        assert d.assume_tz("Asia/Tokyo", disambiguate="raise").exact_eq(
            ZonedDateTime(2020, 8, 15, 23, tz="Asia/Tokyo")
        )
        assert d.assume_tz("Asia/Tokyo").exact_eq(
            ZonedDateTime(2020, 8, 15, 23, tz="Asia/Tokyo")
        )

    def test_ambiguous(self):
        d = PlainDateTime(2023, 10, 29, 2, 15)

        with pytest.raises(RepeatedTime, match="02:15.*Europe/Amsterdam"):
            d.assume_tz("Europe/Amsterdam", disambiguate="raise")

        assert d.assume_tz(
            "Europe/Amsterdam", disambiguate="earlier"
        ).exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                2,
                15,
                tz="Europe/Amsterdam",
                disambiguate="earlier",
            )
        )
        assert d.assume_tz("Europe/Amsterdam", disambiguate="later").exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                2,
                15,
                tz="Europe/Amsterdam",
                disambiguate="later",
            )
        )

    def test_nonexistent(self):
        d = PlainDateTime(2023, 3, 26, 2, 15)

        with pytest.raises(SkippedTime, match="02:15.*Europe/Amsterdam"):
            d.assume_tz("Europe/Amsterdam", disambiguate="raise")

        assert d.assume_tz(
            "Europe/Amsterdam", disambiguate="earlier"
        ).exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                2,
                15,
                tz="Europe/Amsterdam",
                disambiguate="earlier",
            )
        )


class TestAssumeSystemTz:
    @system_tz_ams()
    def test_typical(self):
        assert (
            PlainDateTime(2020, 8, 15, 23)
            .assume_system_tz(disambiguate="raise")
            .exact_eq(SystemDateTime(2020, 8, 15, 23))
        )

    @system_tz_ams()
    def test_ambiguous(self):
        d = PlainDateTime(2023, 10, 29, 2, 15)

        with pytest.raises(RepeatedTime, match="02:15.*system"):
            d.assume_system_tz(disambiguate="raise")

        assert d.assume_system_tz(disambiguate="earlier").exact_eq(
            SystemDateTime(2023, 10, 29, 2, 15, disambiguate="earlier")
        )
        assert d.assume_system_tz(disambiguate="compatible").exact_eq(
            SystemDateTime(2023, 10, 29, 2, 15, disambiguate="earlier")
        )
        assert d.assume_system_tz(disambiguate="later").exact_eq(
            SystemDateTime(2023, 10, 29, 2, 15, disambiguate="later")
        )

    @system_tz_ams()
    def test_nonexistent(self):
        d = PlainDateTime(2023, 3, 26, 2, 15)

        with pytest.raises(SkippedTime, match="02:15.*system"):
            d.assume_system_tz(disambiguate="raise")

        assert d.assume_system_tz(disambiguate="earlier").exact_eq(
            SystemDateTime(2023, 3, 26, 2, 15, disambiguate="earlier")
        )
        assert d.assume_system_tz(disambiguate="later").exact_eq(
            SystemDateTime(2023, 3, 26, 2, 15, disambiguate="later")
        )
        assert d.assume_system_tz(disambiguate="compatible").exact_eq(
            SystemDateTime(2023, 3, 26, 2, 15, disambiguate="compatible")
        )


def test_immutable():
    d = PlainDateTime(2020, 8, 15)
    with pytest.raises(AttributeError):
        d.year = 2021  # type: ignore[misc]


class TestParseCommonIso:
    @pytest.mark.parametrize(
        "s, expected",
        [
            # typical ISO format, perhaps with fractions
            ("2020-08-15T12:08:30", (2020, 8, 15, 12, 8, 30, 0)),
            (
                "2020-08-15T12:08:30.349",
                (2020, 8, 15, 12, 8, 30, 349_000_000),
            ),
            (
                "2020-08-15T12:08:30.3491239",
                (2020, 8, 15, 12, 8, 30, 349_123_900),
            ),
            # "Bacic" ISO format
            (
                "20200815T120830.3491239",
                (2020, 8, 15, 12, 8, 30, 349_123_900),
            ),
            # other separators
            ("2020-08-15 120830", (2020, 8, 15, 12, 8, 30, 0)),
            ("20200815t120830", (2020, 8, 15, 12, 8, 30, 0)),
            # basic/mixed formats
            ("12340815T12:08:30", (1234, 8, 15, 12, 8, 30, 0)),
            ("1234-08-15T120830", (1234, 8, 15, 12, 8, 30, 0)),
            ("12340815 120830", (1234, 8, 15, 12, 8, 30, 0)),
            # Partial time component
            ("2020-08-15T12:08", (2020, 8, 15, 12, 8, 0, 0)),
            ("20200815T02", (2020, 8, 15, 2, 0, 0, 0)),
            ("20200815T0215", (2020, 8, 15, 2, 15, 0, 0)),
            ("1234-01-03T23", (1234, 1, 3, 23, 0, 0, 0)),
        ],
    )
    def test_valid(self, s, expected):
        assert PlainDateTime.parse_common_iso(s) == PlainDateTime(
            *expected[:6], nanosecond=expected[6]
        )

    @pytest.mark.parametrize(
        "s",
        [
            # decimal issues
            "2020-08-15T12:08:30.1234567890",  # too many
            "2020-08-15T12:08:30.1234 ",
            "2020-08-15T12:08:30.123_5",
            "2020-08-15T12:08:30.123.5",
            "2020-08-15T12:08:30.",
            "2020-08-15T12:08:300",
            "2020-08-15T12:08:30:00",
            "2020-08-15T12:08.28",
            # incomplete date
            "2020-11",
            "-020-08-15T12:08",
            # invalid separators
            "2020-03-13T12:08.30",
            "2020-03-14Z12:08",
            "20200314\xc3120830",
            "2020-03-14112:08:30",
            "2020-03-14+12:08",
            "2020-03-1412:08",
            # no date
            "12:08:30.1234567890",
            "T12:08:30",
            "2020-11   T12:08:30.1234567890",
            # offsets not allowed
            "2020-08-15T12:08:30Z",
            "2020-08-15T12:08:30.45+0500",
            "2020-08-15T12:08:30+05:00",
            # incorrect padding
            "2020-08-15T12:8:30",
            "2020-08-15T2",
            # garbage strings
            "",
            "*",
            "garbage",  # garbage
            # non-ascii
            "2020-08-15T12:08:30.349𝟙239",
            # separator, but incomplete time
            "2020-08-15T",
            "2020-08-15T1",
            # invalid component values
            "0000-12-15T12:08:30",
            "2020-18-15T12:08:30",
            "2020-11-31T12:08:30",
            "2020-11-21T24:08:30",
            "2020-11-21T22:68:30",
            "2020-11-21T22:48:62",
            # ordinal and week days
            "2020-W08-1T12:08:30",
            "2020W081T12:08:30",
            "2020081T12:08:30",
            "2020-081T12:08:30",
        ],
    )
    def test_invalid(self, s):
        with pytest.raises(ValueError, match=re.escape(repr(s))):
            PlainDateTime.parse_common_iso(s)

    @given(text())
    def test_fuzzing(self, s: str):
        with pytest.raises(ValueError, match=re.escape(repr(s))):
            PlainDateTime.parse_common_iso(s)


def test_equality():
    d = PlainDateTime(2020, 8, 15)
    different = PlainDateTime(2020, 8, 16)
    different2 = PlainDateTime(2020, 8, 15, nanosecond=1)
    same = PlainDateTime(2020, 8, 15)
    assert d == same
    assert d != different
    assert not d == different
    assert d != different2
    assert not d == different2
    assert not d != same

    assert hash(d) == hash(same)
    assert hash(d) != hash(different)
    assert hash(d) != hash(different2)

    assert d == AlwaysEqual()
    assert d != NeverEqual()
    assert not d == NeverEqual()
    assert not d != AlwaysEqual()

    assert d != 42  # type: ignore[comparison-overlap]
    assert not d == 42  # type: ignore[comparison-overlap]

    # Ambiguity in system timezone doesn't affect equality
    with system_tz_ams():
        assert PlainDateTime(
            2023, 10, 29, 2, 15
        ) == PlainDateTime.from_py_datetime(
            py_datetime(2023, 10, 29, 2, 15, fold=1)
        )


def test_repr():
    d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
    assert repr(d) == "PlainDateTime(2020-08-15 23:12:09.000987654)"
    # no fractional seconds
    assert (
        repr(PlainDateTime(2020, 8, 15, 23, 12))
        == "PlainDateTime(2020-08-15 23:12:00)"
    )


def test_format_common_iso():
    d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
    assert str(d) == "2020-08-15T23:12:09.000987654"
    assert d.format_common_iso() == "2020-08-15T23:12:09.000987654"


def test_comparison():
    d = PlainDateTime(2020, 8, 15, 23, 12, 9)
    later = PlainDateTime(2020, 8, 16, 0, 0, 0)
    later2 = d.replace(nanosecond=1)
    assert d < later
    assert d <= later
    assert later > d
    assert later >= d

    assert d < later2
    assert d <= later2
    assert later2 > d
    assert later2 >= d

    assert d < AlwaysLarger()
    assert d <= AlwaysLarger()
    assert not d > AlwaysLarger()
    assert not d >= AlwaysLarger()
    assert not d < AlwaysSmaller()
    assert not d <= AlwaysSmaller()
    assert d > AlwaysSmaller()
    assert d >= AlwaysSmaller()

    with pytest.raises(TypeError):
        d < 42  # type: ignore[operator]


def test_py_datetime():
    d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_823)
    assert d.py_datetime() == py_datetime(2020, 8, 15, 23, 12, 9, 987_654)


def test_from_py_datetime():
    d = py_datetime(2020, 8, 15, 23, 12, 9, 987_654)
    assert PlainDateTime.from_py_datetime(d) == PlainDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=987_654_000
    )

    with pytest.raises(ValueError, match="utc"):
        PlainDateTime.from_py_datetime(
            py_datetime(2020, 8, 15, 23, 12, 9, 987_654, tzinfo=timezone.utc)
        )

    class MyDateTime(py_datetime):
        pass

    assert PlainDateTime.from_py_datetime(
        MyDateTime(2020, 8, 15, 23, 12, 9, 987_654)
    ) == PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_000)


def test_min_max():
    assert PlainDateTime.MIN == PlainDateTime(1, 1, 1)
    assert PlainDateTime.MAX == PlainDateTime(
        9999, 12, 31, 23, 59, 59, nanosecond=999_999_999
    )


def test_replace():
    d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
    assert d.replace(year=2021) == PlainDateTime(
        2021, 8, 15, 23, 12, 9, nanosecond=987_654
    )
    assert d.replace(month=9) == PlainDateTime(
        2020, 9, 15, 23, 12, 9, nanosecond=987_654
    )
    assert d.replace(day=16) == PlainDateTime(
        2020, 8, 16, 23, 12, 9, nanosecond=987_654
    )
    assert d.replace(hour=0) == PlainDateTime(
        2020, 8, 15, 0, 12, 9, nanosecond=987_654
    )
    assert d.replace(minute=0) == PlainDateTime(
        2020, 8, 15, 23, 0, 9, nanosecond=987_654
    )
    assert d.replace(second=0) == PlainDateTime(
        2020, 8, 15, 23, 12, 0, nanosecond=987_654
    )
    assert d.replace(nanosecond=0) == PlainDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=0
    )

    with pytest.raises(ValueError, match="nano|time"):
        d.replace(nanosecond=1_000_000_000)

    with pytest.raises(ValueError, match="nano|time"):
        d.replace(nanosecond=-4)

    with pytest.raises(TypeError, match="tzinfo"):
        d.replace(tzinfo=timezone.utc)  # type: ignore[call-arg]


class TestShiftMethods:

    def test_valid(self):
        d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
        shifted = PlainDateTime(2020, 5, 27, 23, 12, 14, nanosecond=987_651)

        assert d.add(ignore_dst=True) == d

        assert (
            d.add(
                months=-3,
                days=10,
                hours=48,
                seconds=5,
                nanoseconds=-3,
                ignore_dst=True,
            )
            == shifted
        )

        # same result with deltas
        assert (
            d.add(hours(48) + seconds(5) + nanoseconds(-3), ignore_dst=True)
            .add(months(-3))
            .add(days(10))
        ) == shifted

        # same result with subtract()
        assert (
            d.subtract(
                months=3,
                days=-10,
                hours=-48,
                seconds=-5,
                nanoseconds=3,
                ignore_dst=True,
            )
            == shifted
        )

        # same result with deltas
        assert (
            d.subtract(
                hours(-48) + seconds(-5) + nanoseconds(3), ignore_dst=True
            )
            .subtract(months(3))
            .subtract(days(-10))
        ) == shifted

        # date units don't require ignore_dst
        assert d.subtract(months=3) == d.add(months=-3)

    def test_invalid(self):
        d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(hours=24 * 365 * 8000, ignore_dst=True)

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(hours=-24 * 365 * 3000, ignore_dst=True)

        with pytest.raises((TypeError, AttributeError)):
            d.add(4, ignore_dst=True)  # type: ignore[call-overload]

        # ignore_dst is required
        with pytest.raises(ImplicitlyIgnoringDST):
            d.add(hours=48, seconds=5)  # type: ignore[call-overload]

        # ignore_dst is required
        with pytest.raises(ImplicitlyIgnoringDST):
            d.add(hours(48))  # type: ignore[call-overload]

        # mixing args/kwargs
        with pytest.raises(TypeError):
            d.add(hours(48), seconds=5, ignore_dst=True)  # type: ignore[call-overload]

        # tempt an i128 overflow
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(nanoseconds=1 << 127 - 1, ignore_dst=True)

    @given(
        years=integers(),
        months=integers(),
        days=integers(),
        hours=floats(),
        minutes=floats(),
        seconds=floats(),
        milliseconds=floats(),
        microseconds=floats(),
        nanoseconds=integers(),
    )
    def test_fuzzing(self, **kwargs):
        d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_321)
        try:
            d.add(**kwargs, ignore_dst=True)
        except (ValueError, OverflowError):
            pass


class TestShiftOperators:

    def test_calendar_units(self):
        d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
        shifted = d.replace(year=2021, day=19)
        assert d + (years(1) + weeks(1) + days(-3)) == shifted

        # same results with subtraction
        assert d - (years(-1) + weeks(-1) + days(3)) == shifted

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d + years(8_000)

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d + days(366 * 8_000)

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d + years(-3_000)

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d + days(-366 * 8_000)

    def test_invalid(self):
        d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
        with pytest.raises(TypeError, match="unsupported operand type"):
            d + 42  # type: ignore[operator]
        with pytest.raises(TypeError, match="unsupported operand type"):
            42 + d  # type: ignore[operator]
        with pytest.raises(TypeError, match="unsupported operand type"):
            seconds(4) + d  # type: ignore[operator]

        with pytest.raises(ImplicitlyIgnoringDST, match="add"):
            d + hours(24)  # type: ignore[operator]

        with pytest.raises(ImplicitlyIgnoringDST, match="add"):
            d - (hours(24) + months(3))  # type: ignore[operator]


class TestDifference:
    def test_same(self):
        d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_000)
        other = PlainDateTime(2020, 8, 14, 23, 12, 4, nanosecond=987_654_321)
        assert d.difference(d, ignore_dst=True) == hours(0)
        assert d.difference(other, ignore_dst=True) == hours(24) + seconds(
            5
        ) - nanoseconds(321)

    def test_invalid(self):
        d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
        with pytest.raises(ImplicitlyIgnoringDST):
            d.difference(d)  # type: ignore[call-arg]

        with pytest.raises(ImplicitlyIgnoringDST):
            d - d  # type: ignore[operator]

        with pytest.raises(TypeError):
            d - 43  # type: ignore[operator]


class TestRound:

    @pytest.mark.parametrize(
        "d, increment, unit, floor, ceil, half_floor, half_ceil, half_even",
        [
            (
                PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=459_999_999),
                1,
                "nanosecond",
                PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=459_999_999),
                PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=459_999_999),
                PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=459_999_999),
                PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=459_999_999),
                PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=459_999_999),
            ),
            (
                PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=459_999_999),
                1,
                "second",
                PlainDateTime(2023, 7, 14, 1, 2, 3),
                PlainDateTime(2023, 7, 14, 1, 2, 4),
                PlainDateTime(2023, 7, 14, 1, 2, 3),
                PlainDateTime(2023, 7, 14, 1, 2, 3),
                PlainDateTime(2023, 7, 14, 1, 2, 3),
            ),
            (
                PlainDateTime(2023, 7, 14, 1, 2, 21, nanosecond=459_999_999),
                4,
                "second",
                PlainDateTime(2023, 7, 14, 1, 2, 20),
                PlainDateTime(2023, 7, 14, 1, 2, 24),
                PlainDateTime(2023, 7, 14, 1, 2, 20),
                PlainDateTime(2023, 7, 14, 1, 2, 20),
                PlainDateTime(2023, 7, 14, 1, 2, 20),
            ),
            (
                PlainDateTime(2023, 7, 14, 23, 52, 29, nanosecond=999_999_999),
                10,
                "minute",
                PlainDateTime(2023, 7, 14, 23, 50, 0),
                PlainDateTime(2023, 7, 15),
                PlainDateTime(2023, 7, 14, 23, 50, 0),
                PlainDateTime(2023, 7, 14, 23, 50, 0),
                PlainDateTime(2023, 7, 14, 23, 50, 0),
            ),
            (
                PlainDateTime(2023, 7, 14, 23, 52, 29, nanosecond=999_999_999),
                60,
                "minute",
                PlainDateTime(2023, 7, 14, 23),
                PlainDateTime(2023, 7, 15),
                PlainDateTime(2023, 7, 15),
                PlainDateTime(2023, 7, 15),
                PlainDateTime(2023, 7, 15),
            ),
            (
                PlainDateTime(2023, 7, 14, 11, 59, 29, nanosecond=999_999_999),
                12,
                "hour",
                PlainDateTime(2023, 7, 14),
                PlainDateTime(2023, 7, 14, 12, 0, 0),
                PlainDateTime(2023, 7, 14, 12, 0, 0),
                PlainDateTime(2023, 7, 14, 12, 0, 0),
                PlainDateTime(2023, 7, 14, 12, 0, 0),
            ),
            (
                PlainDateTime(2023, 7, 14, 12),
                1,
                "day",
                PlainDateTime(2023, 7, 14),
                PlainDateTime(2023, 7, 15),
                PlainDateTime(2023, 7, 14),
                PlainDateTime(2023, 7, 15),
                PlainDateTime(2023, 7, 14),
            ),
            (
                PlainDateTime(2023, 7, 14),
                1,
                "day",
                PlainDateTime(2023, 7, 14),
                PlainDateTime(2023, 7, 14),
                PlainDateTime(2023, 7, 14),
                PlainDateTime(2023, 7, 14),
                PlainDateTime(2023, 7, 14),
            ),
        ],
    )
    def test_round(
        self,
        d: PlainDateTime,
        increment,
        unit,
        floor,
        ceil,
        half_floor,
        half_ceil,
        half_even,
    ):
        assert d.round(unit, increment=increment) == half_even
        assert d.round(unit, increment=increment, mode="floor") == floor
        assert d.round(unit, increment=increment, mode="ceil") == ceil
        assert (
            d.round(unit, increment=increment, mode="half_floor") == half_floor
        )
        assert (
            d.round(unit, increment=increment, mode="half_ceil") == half_ceil
        )
        assert (
            d.round(unit, increment=increment, mode="half_even") == half_even
        )

    def test_default(self):
        d = PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=500_000_000)
        assert d.round() == PlainDateTime(2023, 7, 14, 1, 2, 4)
        assert d.replace(second=8).round() == PlainDateTime(
            2023, 7, 14, 1, 2, 8
        )

    def test_invalid_mode(self):
        d = PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000)
        with pytest.raises(ValueError, match="Invalid.*mode.*foo"):
            d.round("second", mode="foo")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "unit, increment",
        [
            ("minute", 8),
            ("second", 14),
            ("millisecond", 15),
            ("day", 2),
            ("hour", 48),
            ("microsecond", 2000),
        ],
    )
    def test_invalid_increment(self, unit, increment):
        d = PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000)
        with pytest.raises(ValueError, match="[Ii]ncrement"):
            d.round(unit, increment=increment)

    def test_default_increment(self):
        d = PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=800_000)
        assert d.round("millisecond") == PlainDateTime(
            2023, 7, 14, 1, 2, 3, nanosecond=1_000_000
        )

    def test_invalid_unit(self):
        d = PlainDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000)
        with pytest.raises(ValueError, match="Invalid.*unit.*foo"):
            d.round("foo")  # type: ignore[arg-type]

    def test_out_of_range(self):
        d = PlainDateTime.MAX.replace(nanosecond=0)
        with pytest.raises((ValueError, OverflowError), match="range"):
            d.round("second", increment=5)


def test_replace_date():
    d = PlainDateTime(2020, 8, 15, 3, 12, 9)
    assert d.replace_date(Date(1996, 2, 19)) == PlainDateTime(
        1996, 2, 19, 3, 12, 9
    )
    with pytest.raises((TypeError, AttributeError)):
        d.replace_date(42)  # type: ignore[arg-type]


def test_replace_time():
    d = PlainDateTime(2020, 8, 15, 3, 12, 9)
    assert d.replace_time(Time(1, 2, 3)) == PlainDateTime(2020, 8, 15, 1, 2, 3)
    with pytest.raises((TypeError, AttributeError)):
        d.replace_time(42)  # type: ignore[arg-type]


def test_pickle():
    d = PlainDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
    dumped = pickle.dumps(d)
    assert len(dumped) <= len(pickle.dumps(d.py_datetime())) + 10
    assert pickle.loads(pickle.dumps(d)) == d


def test_old_pickle_data_remains_unpicklable():
    # Don't update this value -- the whole idea is that it's a pickle at
    # a specific version of the library.
    dumped = (
        b"\x80\x04\x95/\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\x0c_unp"
        b"kl_local\x94\x93\x94C\x0b\xe4\x07\x08\x0f\x17\x0c\t\x06\x12\x0f\x00"
        b"\x94\x85\x94R\x94."
    )
    assert pickle.loads(dumped) == PlainDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=987_654
    )


class TestParseStrptime:

    def test_strptime(self):
        assert PlainDateTime.parse_strptime(
            "2020-08-15 23:12", format="%Y-%m-%d %H:%M"
        ) == PlainDateTime(2020, 8, 15, 23, 12)

    def test_strptime_invalid(self):
        # offset now allowed
        with pytest.raises(ValueError):
            PlainDateTime.parse_strptime(
                "2020-08-15 23:12:09+0500", format="%Y-%m-%d %H:%M:%S%z"
            )

        # format is keyword-only
        with pytest.raises(TypeError, match="format|argument"):
            OffsetDateTime.parse_strptime(
                "2020-08-15 23:12:09", "%Y-%m-%d %H:%M:%S"  # type: ignore[misc]
            )


def test_cannot_subclass():
    with pytest.raises(TypeError):

        class Subclass(PlainDateTime):  # type: ignore[misc]
            pass


def test_deprecated_old_names():
    with pytest.deprecated_call(match="PlainDateTime"):
        from whenever import (  # type: ignore[attr-defined]  # noqa
            NaiveDateTime,
        )
    with pytest.deprecated_call(match="PlainDateTime"):
        from whenever import (  # type: ignore[attr-defined]  # noqa
            LocalDateTime,
        )
