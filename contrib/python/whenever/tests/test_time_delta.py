import pickle
import re
from copy import copy, deepcopy
from datetime import timedelta as py_timedelta

import pytest
from pytest import approx

from whenever import (
    DateDelta,
    PlainDateTime,
    TimeDelta,
    hours,
    microseconds,
    milliseconds,
    minutes,
    nanoseconds,
    seconds,
)

from .common import AlwaysEqual, AlwaysLarger, AlwaysSmaller, NeverEqual

MAX_HOURS = 9999 * 366 * 24


class TestInit:

    @pytest.mark.parametrize(
        "kwargs, expected_nanos",
        [
            (dict(), 0),
            # simplest cases
            (dict(hours=2), 2 * 3_600_000_000_000),
            (dict(minutes=3), 3 * 60_000_000_000),
            (dict(seconds=4), 4 * 1_000_000_000),
            (dict(milliseconds=5), 5 * 1_000_000),
            (dict(microseconds=6), 6 * 1_000),
            (dict(nanoseconds=7), 7),
            # all components
            (
                dict(
                    hours=1,
                    minutes=2,
                    seconds=90,
                    microseconds=4,
                    nanoseconds=5,
                    milliseconds=9,
                ),
                3_600_000_000_000
                + 2 * 60_000_000_000
                + 90 * 1_000_000_000
                + 9 * 1_000_000
                + 4 * 1_000
                + 5,
            ),
            # mixed signs
            (
                dict(hours=1, minutes=-2),
                3_600_000_000_000 - 2 * 60_000_000_000,
            ),
            (
                dict(hours=-1, milliseconds=2),
                -3_600_000_000_000 + 2 * 1_000_000,
            ),
            (dict(nanoseconds=1 << 66), 1 << 66),  # huge value outside i64
            # precision loss for floats
            (
                dict(microseconds=MAX_HOURS * 3_600_000_000 + 0.001),
                MAX_HOURS * 3_600_000_000_000,
            ),
            # no precision loss for integers
            (
                dict(hours=MAX_HOURS - 1),
                (MAX_HOURS - 1) * 3_600_000_000_000,
            ),
            (
                dict(minutes=MAX_HOURS * 60 - 1),
                MAX_HOURS * 3_600_000_000_000 - 60_000_000_000,
            ),
            (
                dict(seconds=MAX_HOURS * 3_600 - 1),
                MAX_HOURS * 3_600_000_000_000 - 1_000_000_000,
            ),
            (
                dict(milliseconds=MAX_HOURS * 3_600_000 - 1),
                MAX_HOURS * 3_600_000_000_000 - 1_000_000,
            ),
            (
                dict(microseconds=MAX_HOURS * 3_600_000_000 - 1),
                MAX_HOURS * 3_600_000_000_000 - 1_000,
            ),
            (
                dict(microseconds=-MAX_HOURS * 3_600_000_000 + 1),
                -MAX_HOURS * 3_600_000_000_000 + 1_000,
            ),
            (
                dict(nanoseconds=-MAX_HOURS * 3_600_000_000_000 + 1),
                -MAX_HOURS * 3_600_000_000_000 + 1,
            ),
            (
                dict(nanoseconds=MAX_HOURS * 3_600_000_000_000 - 1),
                MAX_HOURS * 3_600_000_000_000 - 1,
            ),
            # fractional values
            (dict(minutes=1.5), int(1.5 * 60_000_000_000)),
            (dict(seconds=1.5), int(1.5 * 1_000_000_000)),
        ],
    )
    def test_valid(self, kwargs, expected_nanos):
        d = TimeDelta(**kwargs)
        assert d.in_nanoseconds() == expected_nanos
        # the components are not accessible directly
        assert not hasattr(d, "hours")

    @pytest.mark.parametrize(
        "kwargs",
        [
            dict(hours=MAX_HOURS + 1),
            dict(hours=-MAX_HOURS - 1),
            dict(minutes=MAX_HOURS * 60 + 1),
            dict(minutes=-MAX_HOURS * 60 - 1),
            dict(seconds=MAX_HOURS * 3_600 + 1),
            dict(seconds=-MAX_HOURS * 3_600 - 1),
            dict(milliseconds=MAX_HOURS * 3_600_000 + 1),
            dict(milliseconds=-MAX_HOURS * 3_600_000 - 1),
            dict(microseconds=MAX_HOURS * 3_600_000_000 + 1),
            dict(microseconds=-MAX_HOURS * 3_600_000_000 - 1),
            dict(nanoseconds=MAX_HOURS * 3_600_000_000_000 + 1),
            dict(nanoseconds=-MAX_HOURS * 3_600_000_000_000 - 1),
            dict(hours=float("inf")),
            dict(minutes=float("inf")),
            dict(seconds=float("-inf")),
            dict(milliseconds=float("nan")),
            dict(milliseconds=1e273),
        ],
    )
    def test_invalid_out_of_range(self, kwargs):
        with pytest.raises(
            (ValueError, OverflowError), match="(range|inf|NaN)"
        ):
            TimeDelta(**kwargs)

    def test_invalid_kwargs(self):
        with pytest.raises(TypeError, match="foo"):
            TimeDelta(foo=1)  # type: ignore[call-arg]

        with pytest.raises(TypeError):
            TimeDelta(1)  # type: ignore[misc]

        with pytest.raises(TypeError):
            TimeDelta(**{1: 43})  # type: ignore[misc]


class TestFactories:

    @pytest.mark.parametrize(
        "f, arg, expected",
        [
            (hours, 3.5, TimeDelta(hours=3.5)),
            (minutes, 3.5, TimeDelta(minutes=3.5)),
            (seconds, 3.5, TimeDelta(seconds=3.5)),
            (microseconds, 3.5, TimeDelta(microseconds=3.5)),
            (milliseconds, 3.5, TimeDelta(milliseconds=3.5)),
            (nanoseconds, 3, TimeDelta(nanoseconds=3)),
        ],
    )
    def test_valid(self, f, arg, expected):
        assert f(arg) == expected

    @pytest.mark.parametrize(
        "factory, value",
        [
            (hours, 24 * 366 * 9999 + 1),
            (hours, -24 * 366 * 9999 - 1),
            (minutes, 60 * 24 * 366 * 9999 + 1),
            (minutes, -60 * 24 * 366 * 9999 - 1),
            (seconds, 3_600 * 24 * 366 * 9999 + 1),
            (seconds, -3_600 * 24 * 366 * 9999 - 1),
            (milliseconds, 3_600_000 * 24 * 366 * 9999 + 1),
            (milliseconds, -3_600_000 * 24 * 366 * 9999 - 1),
            (microseconds, 3_600_000_000 * 24 * 366 * 9999 + 1),
            (microseconds, -3_600_000_000 * 24 * 366 * 9999 - 1),
            (nanoseconds, 3_600_000_000_000 * 24 * 366 * 9999 + 1),
            (nanoseconds, -3_600_000_000_000 * 24 * 366 * 9999 - 1),
        ],
    )
    def test_bounds(self, factory, value):
        with pytest.raises((ValueError, OverflowError)):
            factory(value)


def test_constants():
    assert TimeDelta.ZERO == TimeDelta()
    assert TimeDelta.MAX == TimeDelta(
        nanoseconds=9999 * 366 * 24 * 60 * 60 * 1_000_000_000
    )
    assert TimeDelta.MIN == -TimeDelta.MAX


def test_boolean():
    assert not TimeDelta(hours=0, minutes=0, seconds=0, microseconds=0)
    assert not TimeDelta(hours=1, minutes=-60)
    assert TimeDelta(microseconds=1)


def test_aggregations():
    d = TimeDelta(hours=1, minutes=2, seconds=0.003, nanoseconds=4)
    assert d.in_microseconds() == approx(
        3_600_000_000 + 2 * 60_000_000 + 3 * 1_000 + 0.004
    )
    assert d.in_milliseconds() == approx(3_600_000 + 2 * 60_000 + 3 + 4e-6)
    assert d.in_seconds() == approx(3600 + 2 * 60 + 0.003 + 4e-9)
    assert d.in_minutes() == approx(60 + 2 + 0.003 / 60 + 4 / 60_000_000_000)
    assert d.in_hours() == approx(
        1 + 2 / 60 + 0.003 / 3_600 + 4 / 3_600_000_000_000_000
    )
    assert d.in_days_of_24h() == approx(
        1 / 24
        + 2 / (24 * 60)
        + 0.003 / (24 * 3_600)
        + 4 / (24 * 3_600_000_000_000_000)
    )


def test_equality():
    d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    same = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    same_total = TimeDelta(hours=0, minutes=62, seconds=3, microseconds=4)
    different = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=5)
    assert d == same
    assert d == same_total
    assert not d == different
    assert not d == NeverEqual()
    assert d == AlwaysEqual()
    assert not d != same
    assert not d != same_total
    assert d != different
    assert d != NeverEqual()
    assert not d != AlwaysEqual()

    assert hash(d) == hash(same)
    assert hash(d) == hash(same_total)
    assert hash(d) != hash(different)


def test_comparison():
    d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    same = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    same_total = TimeDelta(hours=0, minutes=62, seconds=3, microseconds=4)
    bigger = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=5)
    smaller = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=3)

    assert d <= same
    assert d <= same_total
    assert d <= bigger
    assert not d <= smaller
    assert d <= AlwaysLarger()
    assert not d <= AlwaysSmaller()

    assert not d < same
    assert not d < same_total
    assert d < bigger
    assert not d < smaller
    assert d < AlwaysLarger()
    assert not d < AlwaysSmaller()

    assert d >= same
    assert d >= same_total
    assert not d >= bigger
    assert d >= smaller
    assert not d >= AlwaysLarger()
    assert d >= AlwaysSmaller()

    assert not d > same
    assert not d > same_total
    assert not d > bigger
    assert d > smaller
    assert not d > AlwaysLarger()
    assert d > AlwaysSmaller()


@pytest.mark.parametrize(
    "d, expected",
    [
        (
            TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4),
            "PT1H2M3.000004S",
        ),
        (
            TimeDelta(hours=1, minutes=-2, seconds=3, microseconds=-4),
            "PT58M2.999996S",
        ),
        (
            TimeDelta(hours=1, minutes=2, seconds=3, microseconds=50_000),
            "PT1H2M3.05S",
        ),
        (TimeDelta(hours=1, minutes=120, seconds=3), "PT3H3S"),
        (TimeDelta(), "PT0S"),
        (TimeDelta(microseconds=1), "PT0.000001S"),
        (TimeDelta(microseconds=-1), "-PT0.000001S"),
        (TimeDelta(hours=4, nanoseconds=40), "PT4H0.00000004S"),
        (TimeDelta(seconds=2, microseconds=-3), "PT1.999997S"),
        (TimeDelta(hours=5), "PT5H"),
        (TimeDelta(hours=400), "PT400H"),
        (TimeDelta(minutes=-4), "-PT4M"),
    ],
)
def test_format_common_iso(d, expected):
    assert d.format_common_iso() == expected


def test_repr():
    assert (
        repr(TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4))
        == "TimeDelta(PT1h2m3.000004s)"
    )
    assert repr(TimeDelta()) == "TimeDelta(PT0s)"
    assert repr(TimeDelta(minutes=23, seconds=1)) == "TimeDelta(PT23m1s)"


VALID_TDELTAS = [
    (
        "PT1H2M3.000004S",
        TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4),
    ),
    (
        "PT58M2.999996S",
        TimeDelta(hours=1, minutes=-2, seconds=3, microseconds=-4),
    ),
    (
        "PT1H2M3.05S",
        TimeDelta(hours=1, minutes=2, seconds=3, microseconds=50_000),
    ),
    ("PT3H3S", TimeDelta(hours=1, minutes=120, seconds=3)),
    ("PT0S", TimeDelta()),
    ("PT0.000000001S", TimeDelta(nanoseconds=1)),
    ("PT450.000000001S", TimeDelta(seconds=450, nanoseconds=1)),
    ("PT0.000001S", TimeDelta(microseconds=1)),
    ("-PT0.000001S", TimeDelta(microseconds=-1)),
    ("PT1.999997S", TimeDelta(seconds=2, microseconds=-3)),
    ("PT5H", TimeDelta(hours=5)),
    ("PT400H", TimeDelta(hours=400)),
    ("PT400H0M0.0S", TimeDelta(hours=400)),
    ("-PT4M", TimeDelta(minutes=-4)),
    ("PT0S", TimeDelta()),
    ("PT3M", TimeDelta(minutes=3)),
    ("+PT3M", TimeDelta(minutes=3)),
    ("PT0M", TimeDelta()),
    ("PT0.000000000S", TimeDelta()),
    # extremely long but still valid
    (
        "PT0H0M000000000000000300000000000.000000000S",
        TimeDelta(seconds=300_000_000_000),
    ),
    ("PT316192377600S", TimeDelta.MAX),
    # non-uppercase
    (
        "pt58m2.999996s",
        TimeDelta(hours=1, minutes=-2, seconds=3, microseconds=-4),
    ),
    ("PT316192377600s", TimeDelta.MAX),
    ("PT400h", TimeDelta(hours=400)),
    # comma instead of dot
    ("PT1,999997S", TimeDelta(seconds=2, microseconds=-3)),
]

INVALID_TDELTAS = [
    "P1D",  # date units
    "P1YT4M",  # date units
    "T1H",  # wrong prefix
    "PT4M3H",  # wrong order
    "PT1.5H",  # fractional hours
    "PT1H2M3.000004S9H",  # stuff after nanoseconds
    "PT1H2M3.000004S ",  # stuff after nanoseconds
    "PT34.S",  # missing fractions
    "PTS",  # no digits
    "PT4HS",  # no digits
    "PT-3M",  # sign not at the beginning
    "PT5H.9S",  # wrong fraction
    "PT5H13.S",  # wrong fraction
    "PT𝟙H",  # non-ascii
    "PT0.0001",
    "PT.0001",
    "PT.S",
    "PT0.0000",
    "PT0.123456789",
    "PT0.123456789Sbla",
    "PT4M0.",
    "PT4M0.S",
    # spacing
    "PT 3M",
    "PT-3M",
    "PT3 M",
    "PT3M4 S"
    # too precise
    "PT1.0000000001S",
    # too small
    "",
    "P",
    "PTM",
    # way too many digits (there's a limit...)
    "PT000000000000000000000000000000000000000000000000000000000001S",
]


class TestParseCommonIso:

    @pytest.mark.parametrize("s, expected", VALID_TDELTAS)
    def test_valid(self, s, expected):
        assert TimeDelta.parse_common_iso(s) == expected

    @pytest.mark.parametrize("s", INVALID_TDELTAS)
    def test_invalid(self, s) -> None:
        with pytest.raises(
            ValueError,
            match=r"Invalid format.*" + re.escape(repr(s)),
        ):
            TimeDelta.parse_common_iso(s)

    @pytest.mark.parametrize(
        "s",
        [
            f"PT{10_000 * 366 * 24}H",  # too big value
            f"PT{10_000 * 366 * 24 * 3600}S",  # too big value
        ],
    )
    def test_too_large(self, s) -> None:
        with pytest.raises(ValueError, match="range"):
            TimeDelta.parse_common_iso(s)


def test_addition():
    d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    assert d + TimeDelta() == d
    assert d + TimeDelta(hours=1) == TimeDelta(
        hours=2, minutes=2, seconds=3, microseconds=4
    )
    assert d + TimeDelta(minutes=-1) == TimeDelta(
        hours=1, minutes=1, seconds=3, microseconds=4
    )

    with pytest.raises(TypeError, match="unsupported operand"):
        d + Ellipsis  # type: ignore[operator]

    with pytest.raises(TypeError, match="unsupported operand"):
        Ellipsis + d  # type: ignore[operator]


def test_subtraction():
    d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    assert d - TimeDelta() == d
    assert d - TimeDelta(hours=1) == TimeDelta(
        hours=0, minutes=2, seconds=3, microseconds=4
    )
    assert d - TimeDelta(minutes=-1) == TimeDelta(
        hours=1, minutes=3, seconds=3, microseconds=4
    )

    with pytest.raises(TypeError, match="unsupported operand"):
        d - Ellipsis  # type: ignore[operator]


def test_multiply():
    d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    assert d * 2 == TimeDelta(hours=2, minutes=4, seconds=6, microseconds=8)
    assert d * 0.5 == TimeDelta(
        hours=0, minutes=31, seconds=1, microseconds=500_002
    )
    assert d * 0.5 == 0.5 * d
    assert d * 2 == 2 * d

    # allow very big ints if there's no overflow
    assert TimeDelta(nanoseconds=1) * (1 << 66) == TimeDelta(
        nanoseconds=1 << 66
    )
    assert TimeDelta(nanoseconds=1) * float(1 << 66) == TimeDelta(
        nanoseconds=1 << 66
    )

    # overflow
    with pytest.raises(ValueError, match="range"):
        d * 1_000_000_000

    with pytest.raises(TypeError, match="unsupported operand"):
        d * Ellipsis  # type: ignore[operator]

    with pytest.raises(TypeError, match="unsupported operand"):
        Ellipsis * d  # type: ignore[operator]


class TestDivision:

    def test_by_number(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        assert d / 2 == TimeDelta(
            hours=0, minutes=31, seconds=1, microseconds=500_002
        )
        assert d / 0.5 == TimeDelta(
            hours=2, minutes=4, seconds=6, microseconds=8
        )
        assert TimeDelta.MAX / 1.0 == TimeDelta.MAX
        assert TimeDelta.MIN / 1.0 == TimeDelta.MIN

    def test_divide_by_timedelta(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        assert d / TimeDelta(hours=1) == approx(
            1 + 2 / 60 + 3 / 3_600 + 4 / 3_600_000_000
        )
        assert TimeDelta.ZERO / TimeDelta.MAX == 0.0
        assert TimeDelta.ZERO / TimeDelta.MIN == 0.0
        assert TimeDelta.MAX / TimeDelta.MAX == 1.0
        assert TimeDelta.MIN / TimeDelta.MIN == 1.0
        assert TimeDelta.MAX / TimeDelta.MIN == -1.0
        assert TimeDelta.MIN / TimeDelta.MAX == -1.0

    def test_divide_by_zero(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        with pytest.raises(ZeroDivisionError):
            d / TimeDelta()

        with pytest.raises(ZeroDivisionError):
            d / 0

        with pytest.raises(ZeroDivisionError):
            d / 0.0

    def test_invalid(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        with pytest.raises(TypeError):
            d / "invalid"  # type: ignore[operator]

        with pytest.raises(TypeError):
            "invalid" / d  # type: ignore[operator]

        with pytest.raises(TypeError):
            PlainDateTime(2020, 3, 1) / d  # type: ignore[operator]

        with pytest.raises(TypeError):
            DateDelta(days=5) / d  # type: ignore[operator]


class TestFloorDiv:

    def test_examples(self):
        d = TimeDelta(hours=3, minutes=40, seconds=3, microseconds=4)
        assert d // TimeDelta(hours=1) == 3
        assert d // TimeDelta(minutes=5) == 44
        assert d // TimeDelta(minutes=-5) == -45
        assert -d // TimeDelta(minutes=5) == -45
        assert -d // TimeDelta(minutes=-5) == 44

        # sub-second dividend
        assert d // TimeDelta(microseconds=-9) == -1467000001
        assert -d // TimeDelta(microseconds=9) == -1467000001
        assert -d // TimeDelta(microseconds=-9) == 1467000000
        assert d // TimeDelta(microseconds=9) == 1467000000

        # extreme cases
        assert TimeDelta.ZERO // TimeDelta.MAX == 0
        assert TimeDelta.ZERO // TimeDelta.MIN == 0
        assert TimeDelta.MAX // TimeDelta.MAX == 1
        assert TimeDelta.MIN // TimeDelta.MIN == 1
        assert TimeDelta.MAX // TimeDelta.MIN == -1
        assert TimeDelta.MIN // TimeDelta.MAX == -1
        # result larger than i64
        assert (
            TimeDelta.MAX // TimeDelta(nanoseconds=1)
            == 316192377600_000_000_000
        )

    def test_divide_by_zero(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        with pytest.raises(ZeroDivisionError):
            d // TimeDelta()

    def test_invalid(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        with pytest.raises(TypeError):
            d // "invalid"  # type: ignore[operator]

        with pytest.raises(TypeError):
            "invalid" // d  # type: ignore[operator]


class TestRemainder:

    def test_examples(self):
        d = TimeDelta(hours=3, minutes=40, seconds=3, microseconds=4)
        assert d % TimeDelta(hours=1) == TimeDelta(
            minutes=40, seconds=3, microseconds=4
        )
        assert d % TimeDelta(minutes=5) == TimeDelta(seconds=3, microseconds=4)
        assert d % TimeDelta(minutes=-5) == TimeDelta(
            minutes=-5, seconds=3, microseconds=4
        )
        assert -d % TimeDelta(minutes=5) == TimeDelta(
            minutes=5, seconds=-3, microseconds=-4
        )
        assert -d % TimeDelta(minutes=-5) == TimeDelta(
            seconds=-3, microseconds=-4
        )

        # sub-second dividend
        assert d % TimeDelta(microseconds=-9) == TimeDelta(microseconds=-5)
        assert -d % TimeDelta(microseconds=9) == TimeDelta(microseconds=5)
        assert -d % TimeDelta(microseconds=-9) == TimeDelta(microseconds=-4)
        assert d % TimeDelta(microseconds=9) == TimeDelta(microseconds=4)

        # extreme cases
        assert TimeDelta.ZERO % TimeDelta.MAX == TimeDelta.ZERO
        assert TimeDelta.ZERO % TimeDelta.MIN == TimeDelta.ZERO
        assert TimeDelta.MAX % TimeDelta.MAX == TimeDelta.ZERO
        assert TimeDelta.MIN % TimeDelta.MIN == TimeDelta.ZERO
        assert TimeDelta.MAX % TimeDelta.MIN == TimeDelta.ZERO
        assert TimeDelta.MIN % TimeDelta.MAX == TimeDelta.ZERO
        # result larger than i64
        assert (TimeDelta.MAX - TimeDelta(nanoseconds=1)) % TimeDelta.MAX == (
            TimeDelta.MAX - TimeDelta(nanoseconds=1)
        )

    def test_divide_by_zero(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        with pytest.raises(ZeroDivisionError):
            d % TimeDelta()

    def test_invalid(self):
        d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
        with pytest.raises(TypeError):
            d % "invalid"  # type: ignore[operator]

        with pytest.raises(TypeError):
            5.9 % d  # type: ignore[operator]


def test_negate():
    assert TimeDelta.ZERO == -TimeDelta.ZERO
    assert TimeDelta(
        hours=-1, minutes=2, seconds=-3, microseconds=4
    ) == -TimeDelta(hours=1, minutes=-2, seconds=3, microseconds=-4)
    assert -TimeDelta.MAX == TimeDelta.MIN


@pytest.mark.parametrize(
    "d",
    [
        TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4),
        TimeDelta.ZERO,
        TimeDelta(hours=-2, minutes=-15),
    ],
)
def test_pos(d):
    assert d is +d


class TestRound:
    @pytest.mark.parametrize(
        "t, increment, unit, floor, ceil, half_floor, half_ceil, half_even",
        [
            (
                TimeDelta.ZERO,
                1,
                "nanosecond",
                TimeDelta.ZERO,
                TimeDelta.ZERO,
                TimeDelta.ZERO,
                TimeDelta.ZERO,
                TimeDelta.ZERO,
            ),
            (
                TimeDelta(nanoseconds=5),
                10,
                "nanosecond",
                TimeDelta.ZERO,
                TimeDelta(nanoseconds=10),
                TimeDelta.ZERO,
                TimeDelta(nanoseconds=10),
                TimeDelta.ZERO,
            ),
            (
                TimeDelta(nanoseconds=-5),
                10,
                "nanosecond",
                TimeDelta(nanoseconds=-10),
                TimeDelta.ZERO,
                TimeDelta(nanoseconds=-10),
                TimeDelta.ZERO,
                TimeDelta.ZERO,
            ),
            (
                TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                1,
                "nanosecond",
                TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
            ),
            (
                -TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                1,
                "nanosecond",
                -TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                -TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                -TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                -TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                -TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
            ),
            # nanoseconds avoids a tie
            (
                TimeDelta(hours=1, minutes=2, seconds=3, nanoseconds=4),
                2,
                "second",
                TimeDelta(hours=1, minutes=2, seconds=2),
                TimeDelta(hours=1, minutes=2, seconds=4),
                TimeDelta(hours=1, minutes=2, seconds=4),
                TimeDelta(hours=1, minutes=2, seconds=4),
                TimeDelta(hours=1, minutes=2, seconds=4),
            ),
            # nanoseconds results in a tie
            (
                TimeDelta(hours=1, minutes=2, seconds=7, milliseconds=500),
                3,
                "second",
                TimeDelta(hours=1, minutes=2, seconds=6),
                TimeDelta(hours=1, minutes=2, seconds=9),
                TimeDelta(hours=1, minutes=2, seconds=6),
                TimeDelta(hours=1, minutes=2, seconds=9),
                TimeDelta(hours=1, minutes=2, seconds=6),
            ),
            (
                TimeDelta(hours=1, minutes=2, seconds=3),
                2,
                "second",
                TimeDelta(hours=1, minutes=2, seconds=2),
                TimeDelta(hours=1, minutes=2, seconds=4),
                TimeDelta(hours=1, minutes=2, seconds=2),
                TimeDelta(hours=1, minutes=2, seconds=4),
                TimeDelta(hours=1, minutes=2, seconds=4),
            ),
            (
                TimeDelta(hours=1, minutes=7.5),
                15,
                "minute",
                TimeDelta(hours=1, minutes=0),
                TimeDelta(hours=1, minutes=15),
                TimeDelta(hours=1, minutes=0),
                TimeDelta(hours=1, minutes=15),
                TimeDelta(hours=1, minutes=0),
            ),
            (
                -TimeDelta(hours=4, minutes=43),
                30,
                "minute",
                -TimeDelta(hours=5),
                -TimeDelta(hours=4.5),
                -TimeDelta(hours=4.5),
                -TimeDelta(hours=4.5),
                -TimeDelta(hours=4.5),
            ),
            (
                TimeDelta(hours=10, minutes=30),
                10,
                "hour",
                TimeDelta(hours=10),
                TimeDelta(hours=20),
                TimeDelta(hours=10),
                TimeDelta(hours=10),
                TimeDelta(hours=10),
            ),
        ],
    )
    def test_valid(
        self, t, increment, unit, floor, ceil, half_floor, half_ceil, half_even
    ):
        assert t.round(unit, increment=increment) == half_even
        assert t.round(unit, increment=increment, mode="ceil") == ceil
        assert t.round(unit, increment=increment, mode="floor") == floor
        assert (
            t.round(unit, increment=increment, mode="half_floor") == half_floor
        )
        assert (
            t.round(unit, increment=increment, mode="half_ceil") == half_ceil
        )
        assert (
            t.round(unit, increment=increment, mode="half_even") == half_even
        )

    @pytest.mark.parametrize(
        "unit, increment",
        [
            ("minute", 8),
            ("second", 14),
            ("millisecond", 15),
            ("millisecond", 2000),
            ("second", 90),
            ("hour", 5000),
            ("millisecond", -100),
        ],
    )
    def test_invalid_increment(self, unit, increment):
        t = TimeDelta.ZERO
        with pytest.raises(ValueError, match="[Ii]ncrement"):
            t.round(unit, increment=increment)

    def test_default_half_even_seconds(self):
        assert TimeDelta(seconds=2, milliseconds=500).round() == TimeDelta(
            seconds=2
        )
        assert TimeDelta(seconds=3, milliseconds=500).round() == TimeDelta(
            seconds=4
        )

    def test_default_increment(self):
        d = TimeDelta(seconds=2, nanoseconds=800)
        assert d.round("microsecond") == TimeDelta(seconds=2, microseconds=1)

    def test_invalid_unit(self):
        t = TimeDelta.ZERO
        with pytest.raises(ValueError, match="Invalid.*unit.*foo"):
            t.round("foo")  # type: ignore[arg-type]

    def test_invalid_mode(self):
        t = TimeDelta.ZERO
        with pytest.raises(ValueError, match="Invalid.*mode.*foo"):
            t.round(mode="foo")  # type: ignore[arg-type]

    def test_no_day_unit(self):
        t = TimeDelta.ZERO
        with pytest.raises(ValueError, match="day.*24 hours"):
            t.round("day")  # type: ignore[arg-type]

    def test_extremes(self):
        t = TimeDelta.MAX
        assert t.round(mode="floor") == TimeDelta.MAX

        with pytest.raises(ValueError, match="range"):
            t.round(unit="hour", increment=10)


@pytest.mark.parametrize(
    "d, expected",
    [
        (TimeDelta(), py_timedelta(0)),
        (
            TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4),
            py_timedelta(hours=1, minutes=2, seconds=3, microseconds=4),
        ),
        (TimeDelta(nanoseconds=-42_865), py_timedelta(microseconds=-43)),
        (TimeDelta(nanoseconds=1), py_timedelta()),
        (TimeDelta(nanoseconds=1_000), py_timedelta(microseconds=1)),
        (TimeDelta(nanoseconds=1_000_000), py_timedelta(milliseconds=1)),
        (TimeDelta(nanoseconds=987), py_timedelta()),
        (TimeDelta(nanoseconds=12987), py_timedelta(microseconds=12)),
        (TimeDelta(hours=48, nanoseconds=800), py_timedelta(days=2)),
        (
            TimeDelta(hours=48, nanoseconds=-800),
            py_timedelta(days=2, microseconds=-1),
        ),
    ],
)
def test_py_timedelta(d, expected):
    assert d.py_timedelta() == expected


def test_from_py_timedelta():
    assert TimeDelta.from_py_timedelta(py_timedelta(0)) == TimeDelta.ZERO
    assert TimeDelta.from_py_timedelta(
        py_timedelta(weeks=8, hours=1, minutes=2, seconds=3, microseconds=4)
    ) == TimeDelta(hours=1 + 7 * 24 * 8, minutes=2, seconds=3, microseconds=4)

    # subclass not allowed
    class SubclassTimedelta(py_timedelta):
        pass

    with pytest.raises(TypeError, match="timedelta.*exact"):
        TimeDelta.from_py_timedelta(SubclassTimedelta(1))

    with pytest.raises(ValueError, match="range"):
        TimeDelta.from_py_timedelta(py_timedelta.max)

    with pytest.raises(ValueError, match="range"):
        TimeDelta.from_py_timedelta(py_timedelta.min)


def test_as_hrs_mins_secs_nanos():
    d = TimeDelta(hours=1, minutes=2, seconds=-3, microseconds=4_060_000)
    hms = d.in_hrs_mins_secs_nanos()
    assert all(isinstance(x, int) for x in hms)
    assert hms == (1, 2, 1, 60_000_000)
    assert TimeDelta(hours=-2, minutes=-15).in_hrs_mins_secs_nanos() == (
        -2,
        -15,
        0,
        0,
    )
    assert TimeDelta(nanoseconds=-4).in_hrs_mins_secs_nanos() == (0, 0, 0, -4)
    assert TimeDelta.ZERO.in_hrs_mins_secs_nanos() == (0, 0, 0, 0)


def test_abs():
    assert abs(TimeDelta()) == TimeDelta()
    assert abs(
        TimeDelta(hours=-1, minutes=-2, seconds=-3, microseconds=-4)
    ) == TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    assert abs(TimeDelta(hours=1)) == TimeDelta(hours=1)


def test_copy():
    d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    assert copy(d) is d
    assert deepcopy(d) is d


def test_pickling():
    d = TimeDelta(hours=1, minutes=2, seconds=3, microseconds=4)
    dumped = pickle.dumps(d)
    assert len(dumped) < len(pickle.dumps(d.py_timedelta())) + 15
    assert pickle.loads(dumped) == d

    assert pickle.loads(pickle.dumps(TimeDelta.MAX)) == TimeDelta.MAX
    assert pickle.loads(pickle.dumps(TimeDelta.MIN)) == TimeDelta.MIN


def test_compatible_unpickle():
    dumped = (
        b"\x80\x04\x951\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\r_unpkl_t"
        b"delta\x94\x93\x94C\x0c\x8b\x0e\x00\x00\x00\x00\x00\x00\xa0\x0f"
        b"\x00\x00\x94\x85\x94R\x94."
    )
    assert pickle.loads(dumped) == TimeDelta(
        hours=1, minutes=2, seconds=3, microseconds=4
    )
