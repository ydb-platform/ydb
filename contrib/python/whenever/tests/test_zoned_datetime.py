import pickle
import re
from copy import copy, deepcopy
from datetime import (
    datetime as py_datetime,
    timedelta as py_timedelta,
    timezone as py_timezone,
)
from pathlib import Path
from typing import Any
from zoneinfo import (
    ZoneInfo,
    available_timezones as zoneinfo_available_timezones,
)

import pytest
from hypothesis import given
from hypothesis.strategies import text

from whenever import (
    Date,
    Instant,
    InvalidOffsetError,
    OffsetDateTime,
    PlainDateTime,
    RepeatedTime,
    SkippedTime,
    SystemDateTime,
    Time,
    TimeDelta,
    TimeZoneNotFoundError,
    ZonedDateTime,
    available_timezones,
    clear_tzcache,
    days,
    hours,
    milliseconds,
    minutes,
    reset_tzpath,
    weeks,
    years,
)

from .common import (
    AlwaysEqual,
    AlwaysLarger,
    AlwaysSmaller,
    NeverEqual,
    system_tz,
    system_tz_ams,
    system_tz_nyc,
)

try:
    import tzdata  # noqa
except ImportError:
    HAS_TZDATA = False
else:
    HAS_TZDATA = True

TEST_DIR = Path(__file__).parent


class TestInit:
    def test_unambiguous(self):
        zone = "America/New_York"
        d = ZonedDateTime(2020, 8, 15, 5, 12, 30, nanosecond=450, tz=zone)

        assert d.year == 2020
        assert d.month == 8
        assert d.day == 15
        assert d.hour == 5
        assert d.minute == 12
        assert d.second == 30
        assert d.nanosecond == 450
        assert d.tz == zone

    def test_repeated_time(self):
        kwargs: dict[str, Any] = dict(
            year=2023,
            month=10,
            day=29,
            hour=2,
            minute=15,
            second=30,
            tz="Europe/Amsterdam",
        )

        assert ZonedDateTime(**kwargs).exact_eq(
            ZonedDateTime(**kwargs, disambiguate="compatible")
        )

        with pytest.raises(
            RepeatedTime,
            match="2023-10-29 02:15:30 is repeated in timezone 'Europe/Amsterdam'",
        ):
            ZonedDateTime(**kwargs, disambiguate="raise")

        assert (
            ZonedDateTime(**kwargs, disambiguate="earlier").offset
            > ZonedDateTime(**kwargs, disambiguate="later").offset
        )
        assert ZonedDateTime(**kwargs, disambiguate="compatible").exact_eq(
            ZonedDateTime(**kwargs, disambiguate="earlier")
        )

    def test_invalid_zone(self):
        with pytest.raises((TypeError, AttributeError)):
            ZonedDateTime(
                2020,
                8,
                15,
                5,
                12,
                tz=hours(34),  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize(
        "key",
        [
            "America/Nowhere",  # non-existent
            "/America/New_York",  # slash at the beginning
            "America/New_York/",  # slash at the end
            "America/New\0York",  # null byte
            "America\\New_York",  # backslash
            "../America/New_York/",  # relative path
            "America/New_York/..",  # other dots
            "America//New_York",  # double slash
            "America/../America/New_York",  # not normalized
            "America/./America/New_York",  # not normalized
            "+VERSION",  # in tz path, but not a tzif file
            "leapseconds",  # in tz path, but not a tzif file
            "Europe",  # a directory
            "__init__.py",  # file in tzdata package
            "",
            ".",
            "/",
            " ",
            "Foo" * 100,  # too long
            # invalid file path characters
            "foo:bar",
            "bla*",
            "*",
            "**",
            ":",
            "&",
            # non-ascii
            "🇨🇦",
            "America/Bogotá",
            # invalid start characters
            "+B",
            "+",
            "-",
            "-foo",
        ],
    )
    def test_invalid_key(self, key: str):
        with pytest.raises(TimeZoneNotFoundError):
            ZonedDateTime(2020, 8, 15, 5, 12, tz=key)

    # This test is run last, because it modifies the tz cache
    # which can affect other tests (namely those using exact_eq)
    @pytest.mark.order(-1)
    def test_tz_cache_adjustments(self):
        nyc = "America/New_York"
        ams = "Europe/Amsterdam"
        # creating a ZDT puts it in the tz cache
        d = ZonedDateTime(2020, 8, 15, 5, 12, tz=nyc)
        ZonedDateTime(2020, 8, 15, 5, 12, tz=ams)

        assert available_timezones() == zoneinfo_available_timezones()

        from whenever import TZPATH

        prev_tzpath = TZPATH
        # We now set the TZ path to our test directory
        # (which contains some tzif files)
        reset_tzpath([TEST_DIR])
        from whenever import TZPATH

        assert TZPATH == (str(TEST_DIR),)
        try:
            # Available timezones should now be different
            assert available_timezones() != zoneinfo_available_timezones()
            # We still can find load the NYC timezone even though
            # it isn't in the new path. This is because it's cached!
            assert ZonedDateTime(1982, 8, 15, 5, 12, tz=nyc)
            assert ZonedDateTime(1982, 8, 15, 5, 12, tz=ams)
            # So let's clear the cache and check we can't find it anymore
            clear_tzcache(only_keys=[nyc])
            if not HAS_TZDATA:
                with pytest.raises(TimeZoneNotFoundError):
                    ZonedDateTime(1982, 8, 15, 5, 12, tz=nyc)

            # We can still use the old instance without problems
            d.add(hours=24)

            assert ZonedDateTime(1982, 8, 15, 5, 12, tz=ams)
            clear_tzcache()
            if not HAS_TZDATA:
                with pytest.raises(TimeZoneNotFoundError):
                    ZonedDateTime(1982, 8, 15, 5, 12, tz=ams)

            # We can still use the old instance without problems
            d.add(hours=24)

            # Ok, let's see if we can find our custom timezones
            d2 = ZonedDateTime(1982, 8, 15, 5, 12, tz="tzif/Amsterdam.tzif")
            assert ZonedDateTime(1982, 8, 15, 5, 12, tz="tzif/Honolulu.tzif")
        finally:
            # We need to reset the tzpath to the original one
            reset_tzpath()

        from whenever import TZPATH

        assert TZPATH == prev_tzpath

        # Available timezones should now be the same again
        assert available_timezones() == zoneinfo_available_timezones()

        # Our custom timezones are still in the cache
        assert ZonedDateTime(1982, 8, 15, 5, 12, tz="tzif/Amsterdam.tzif")
        # And clear the cache again
        clear_tzcache()
        # ...and now they aren't
        with pytest.raises(TimeZoneNotFoundError):
            ZonedDateTime(1982, 8, 15, 5, 12, tz="tzif/Amsterdam.tzif")

        # but we can still use an old instance
        d2.add(hours=24)

        # We can request proper timezones now again
        assert ZonedDateTime(2020, 8, 15, 5, 12, tz=nyc) == d
        # exact_eq() is affected (as documented)
        assert not ZonedDateTime(2020, 8, 15, 5, 12, tz=nyc).exact_eq(d)

        # check exception handling invalid arguments
        with pytest.raises(TypeError, match="iterable"):
            reset_tzpath("/usr/share/zoneinfo")  # must be a list!
        with pytest.raises(ValueError, match="absolute"):
            reset_tzpath(["../../share/zoneinfo"])

    def test_optionality(self):
        tz = "America/New_York"
        assert ZonedDateTime(2020, 8, 15, 12, tz=tz).exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                12,
                0,
                0,
                nanosecond=0,
                tz=tz,
                disambiguate="raise",
            )
        )

    def test_tz_required(self):
        with pytest.raises(TypeError):
            ZonedDateTime(2020, 8, 15, 12)  # type: ignore[call-arg]

    def test_out_of_range_due_to_offset(self):
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            ZonedDateTime(1, 1, 1, tz="Asia/Tokyo")

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            ZonedDateTime(9999, 12, 31, 23, tz="America/New_York")

    def test_invalid(self):
        with pytest.raises(ValueError):
            ZonedDateTime(
                2020,
                8,
                15,
                12,
                8,
                30,
                nanosecond=1_000_000_000,
                tz="Europe/Amsterdam",
            )

    def test_skipped(self):
        kwargs: dict[str, Any] = dict(
            year=2023,
            month=3,
            day=26,
            hour=2,
            minute=15,
            second=30,
            tz="Europe/Amsterdam",
        )

        assert ZonedDateTime(**kwargs).exact_eq(
            ZonedDateTime(**kwargs, disambiguate="compatible")
        )

        with pytest.raises(
            SkippedTime,
            match="2023-03-26 02:15:30 is skipped in timezone 'Europe/Amsterdam'",
        ):
            ZonedDateTime(**kwargs, disambiguate="raise")

        d1 = ZonedDateTime(**kwargs, disambiguate="compatible")
        assert d1.exact_eq(
            ZonedDateTime(2023, 3, 26, 3, 15, 30, tz="Europe/Amsterdam")
        )

        assert ZonedDateTime(**kwargs, disambiguate="later").exact_eq(
            ZonedDateTime(2023, 3, 26, 3, 15, 30, tz="Europe/Amsterdam")
        )
        assert ZonedDateTime(**kwargs, disambiguate="earlier").exact_eq(
            ZonedDateTime(2023, 3, 26, 1, 15, 30, tz="Europe/Amsterdam")
        )

        assert issubclass(SkippedTime, ValueError)


# NOTE: there's a separate test for changing the tzpath and
# its effect on available_timezones()
# We run this test relatively late to allow the cache to be used more
# organically throughout other tests instead of immediately loading everything
# here beforehand
@pytest.mark.order(-2)
def test_available_timezones():
    tzs = available_timezones()

    # So long as we don't mess with the configuration, these should be identical
    assert tzs == zoneinfo_available_timezones()

    d = ZonedDateTime(2025, 3, 26, 1, 15, 30, tz="UTC")

    # We should be able to load all of them
    for tz in tzs:
        d = d.to_tz(tz)


def test_offset():
    d = ZonedDateTime(
        2020, 8, 15, 5, 12, 30, nanosecond=450, tz="America/New_York"
    )
    assert d.offset == hours(-4)


def test_immutable():
    d = ZonedDateTime(2020, 8, 15, tz="Europe/Amsterdam")
    with pytest.raises(AttributeError):
        d.year = 2021  # type: ignore[misc]


def test_date():
    d = ZonedDateTime(2020, 8, 15, 14, tz="Europe/Amsterdam")
    assert d.date() == Date(2020, 8, 15)


def test_time():
    d = ZonedDateTime(2020, 8, 15, 14, 30, 45, tz="Europe/Amsterdam")
    assert d.time() == Time(14, 30, 45)


def test_to_plain():
    d = ZonedDateTime(2020, 8, 15, 13, tz="Europe/Amsterdam")
    assert d.to_plain() == PlainDateTime(2020, 8, 15, 13)
    assert d.replace(disambiguate="later").to_plain() == PlainDateTime(
        2020, 8, 15, 13
    )

    with pytest.deprecated_call():
        assert d.local() == d.to_plain()  # type: ignore[attr-defined]


class TestReplaceDate:
    def test_unambiguous(self):
        d = ZonedDateTime(2020, 8, 15, 14, nanosecond=2, tz="Europe/Amsterdam")
        assert d.replace_date(Date(2021, 1, 2)).exact_eq(
            ZonedDateTime(2021, 1, 2, 14, nanosecond=2, tz="Europe/Amsterdam")
        )

    def test_repeated_time(self):
        d = ZonedDateTime(2020, 1, 1, 2, 15, 30, tz="Europe/Amsterdam")
        date = Date(2023, 10, 29)

        with pytest.raises(RepeatedTime):
            assert d.replace_date(date, disambiguate="raise")

        assert d.replace_date(date).exact_eq(
            d.replace(year=2023, month=10, day=29)
        )
        assert d.replace_date(date, disambiguate="earlier").exact_eq(
            d.replace(year=2023, month=10, day=29, disambiguate="earlier")
        )
        assert d.replace_date(date, disambiguate="later").exact_eq(
            d.replace(year=2023, month=10, day=29, disambiguate="later")
        )
        assert d.replace_date(date, disambiguate="compatible").exact_eq(
            d.replace(year=2023, month=10, day=29, disambiguate="compatible")
        )

    def test_skipped_time(self):
        d = ZonedDateTime(2020, 1, 1, 2, 15, 30, tz="Europe/Amsterdam")
        date = Date(2023, 3, 26)

        with pytest.raises(SkippedTime):
            assert d.replace_date(date, disambiguate="raise")

        assert d.replace_date(date).exact_eq(
            d.replace(year=2023, month=3, day=26)
        )
        assert d.replace_date(date, disambiguate="earlier").exact_eq(
            d.replace(year=2023, month=3, day=26, disambiguate="earlier")
        )
        assert d.replace_date(date, disambiguate="later").exact_eq(
            d.replace(year=2023, month=3, day=26, disambiguate="later")
        )
        assert d.replace_date(date, disambiguate="compatible").exact_eq(
            d.replace(year=2023, month=3, day=26, disambiguate="compatible")
        )

    def test_invalid(self):
        d = ZonedDateTime(2020, 8, 15, 14, tz="Europe/Amsterdam")
        with pytest.raises((TypeError, AttributeError)):
            d.replace_date(object(), disambiguate="compatible")  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="disambiguate"):
            d.replace_date(Date(2020, 8, 15), disambiguate="foo")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="got 2|foo"):
            d.replace_date(Date(2020, 8, 15), disambiguate="raise", foo=4)  # type: ignore[call-arg]

        with pytest.raises(TypeError, match="foo"):
            d.replace_date(Date(2020, 8, 15), foo="raise")  # type: ignore[call-arg]

    def test_out_of_range_due_to_offset(self):
        d = ZonedDateTime(2020, 1, 1, tz="Asia/Tokyo")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.replace_date(Date(1, 1, 1), disambiguate="compatible")

        d2 = ZonedDateTime(2020, 1, 1, hour=23, tz="America/New_York")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d2.replace_date(Date(9999, 12, 31), disambiguate="compatible")


class TestReplaceTime:
    def test_unambiguous(self):
        d = ZonedDateTime(2020, 8, 15, 14, tz="Europe/Amsterdam")
        assert d.replace_time(Time(1, 2, 3, nanosecond=4_000)).exact_eq(
            ZonedDateTime(
                2020, 8, 15, 1, 2, 3, nanosecond=4_000, tz="Europe/Amsterdam"
            )
        )

    def test_repeated_time(self):
        d = ZonedDateTime(2023, 10, 29, 0, 15, 30, tz="Europe/Amsterdam")
        d_later = ZonedDateTime(2023, 10, 29, 4, 15, 30, tz="Europe/Amsterdam")
        time = Time(2, 15, 30)

        with pytest.raises(RepeatedTime):
            assert d.replace_time(time, disambiguate="raise")

        with pytest.raises(RepeatedTime):
            assert d_later.replace_time(time, disambiguate="raise")

        assert d.replace_time(time).exact_eq(
            d.replace(hour=2, minute=15, second=30)
        )
        assert d_later.replace_time(time).exact_eq(
            d_later.replace(hour=2, minute=15, second=30)
        )
        assert d.replace_time(time, disambiguate="earlier").exact_eq(
            d.replace(hour=2, minute=15, second=30, disambiguate="earlier")
        )
        assert d.replace_time(time, disambiguate="later").exact_eq(
            d.replace(hour=2, minute=15, second=30, disambiguate="later")
        )
        assert d.replace_time(time, disambiguate="compatible").exact_eq(
            d.replace(hour=2, minute=15, second=30, disambiguate="compatible")
        )
        # For small moves within a fold, keeps the same offset
        ZonedDateTime(
            2023, 10, 29, 2, 30, tz="Europe/Amsterdam", disambiguate="later"
        ).replace_time(time).exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                2,
                15,
                30,
                tz="Europe/Amsterdam",
                disambiguate="later",
            )
        )

    def test_skipped_time(self):
        d = ZonedDateTime(2023, 3, 26, 0, 15, tz="Europe/Amsterdam")
        time = Time(2, 15)
        with pytest.raises(SkippedTime):
            assert d.replace_time(time, disambiguate="raise")

        assert d.replace_time(time).exact_eq(
            d.replace(hour=2, minute=15, second=0)
        )
        assert d.replace_time(time, disambiguate="earlier").exact_eq(
            d.replace(hour=2, minute=15, disambiguate="earlier")
        )
        assert d.replace_time(time, disambiguate="later").exact_eq(
            d.replace(hour=2, minute=15, disambiguate="later")
        )
        assert d.replace_time(time, disambiguate="compatible").exact_eq(
            d.replace(hour=2, minute=15, disambiguate="compatible")
        )

    def test_invalid(self):
        d = ZonedDateTime(2020, 8, 15, 14, tz="Europe/Amsterdam")
        with pytest.raises((TypeError, AttributeError)):
            d.replace_time(object(), disambiguate="later")  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="disambiguate"):
            d.replace_time(Time(1, 2, 3), disambiguate="foo")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="got 2|foo"):
            d.replace_time(Time(1, 2, 3), disambiguate="raise", foo=4)  # type: ignore[call-arg]

        with pytest.raises(TypeError, match="foo"):
            d.replace_time(Time(1, 2, 3), foo="raise")  # type: ignore[call-arg]

    def test_out_of_range_due_to_offset(self):
        d = ZonedDateTime(1, 1, 1, hour=23, tz="Asia/Tokyo")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.replace_time(Time(1), disambiguate="compatible")

        d2 = ZonedDateTime(9999, 12, 31, hour=2, tz="America/New_York")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d2.replace_time(Time(23), disambiguate="compatible")


class TestFormatCommonIso:

    @pytest.mark.parametrize(
        "d, expected",
        [
            (
                ZonedDateTime(
                    2020,
                    8,
                    15,
                    23,
                    12,
                    9,
                    nanosecond=987_654_321,
                    tz="Europe/Amsterdam",
                ),
                "2020-08-15T23:12:09.987654321+02:00[Europe/Amsterdam]",
            ),
            (
                ZonedDateTime(
                    2023,
                    10,
                    29,
                    2,
                    15,
                    30,
                    tz="Europe/Amsterdam",
                    disambiguate="earlier",
                ),
                "2023-10-29T02:15:30+02:00[Europe/Amsterdam]",
            ),
            (
                ZonedDateTime(
                    2023,
                    10,
                    29,
                    2,
                    15,
                    30,
                    tz="Europe/Amsterdam",
                    disambiguate="later",
                ),
                "2023-10-29T02:15:30+01:00[Europe/Amsterdam]",
            ),
            (
                ZonedDateTime(
                    1900,
                    1,
                    1,
                    tz="Europe/Dublin",
                ),
                "1900-01-01T00:00:00-00:25:21[Europe/Dublin]",
            ),
        ],
    )
    def test_common_iso(self, d: ZonedDateTime, expected: str):
        assert str(d) == expected
        assert d.format_common_iso() == expected


class TestEquality:
    def test_same_exact(self):
        a = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        b = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_timezone(self):
        a = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")

        # same **wall clock** time, different timezone
        b = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="America/New_York")
        assert a != b
        assert hash(a) != hash(b)

        # same moment, different timezone
        c = ZonedDateTime(2020, 8, 15, 6, 8, 30, tz="America/New_York")
        assert a == c
        assert hash(a) == hash(c)

    def test_different_time(self):
        a = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        b = ZonedDateTime(2020, 8, 15, 12, 8, 31, tz="Europe/Amsterdam")
        assert a != b
        assert hash(a) != hash(b)

    def test_different_fold_no_ambiguity(self):
        a = ZonedDateTime(
            2020,
            8,
            15,
            12,
            8,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        b = a.replace(disambiguate="later")
        assert a == b
        assert hash(a) == hash(b)

    def test_different_fold_ambiguity(self):
        a = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        b = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="later",
        )
        assert a != b
        assert hash(a) != hash(b)

    def test_ambiguity_between_different_timezones(self):
        a = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="later",
        )
        b = a.to_tz("America/New_York")
        assert a.to_instant() == b.to_instant()  # sanity check
        assert hash(a) == hash(b)
        assert a == b

    @system_tz_nyc()
    def test_other_exact(self):
        d: ZonedDateTime | OffsetDateTime | SystemDateTime = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        assert d == d.to_instant()  # type: ignore[comparison-overlap]
        assert hash(d) == hash(d.to_instant())
        assert d != d.to_instant() + hours(2)  # type: ignore[comparison-overlap]

        assert d == d.to_system_tz()
        assert d != d.to_system_tz().replace(hour=8, disambiguate="raise")

        assert d == d.to_fixed_offset()
        assert hash(d) == hash(d.to_fixed_offset())
        assert d != d.to_fixed_offset().replace(hour=10, ignore_dst=True)

    def test_not_implemented(self):
        d = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        assert d == AlwaysEqual()
        assert d != NeverEqual()
        assert not d == NeverEqual()
        assert not d != AlwaysEqual()

        assert d != 42  # type: ignore[comparison-overlap]
        assert not d == 42  # type: ignore[comparison-overlap]
        assert 42 != d  # type: ignore[comparison-overlap]
        assert not 42 == d  # type: ignore[comparison-overlap]
        assert not hours(2) == d  # type: ignore[comparison-overlap]


@pytest.mark.parametrize(
    "d, expect",
    [
        (
            ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam"),
            False,
        ),
        (
            ZonedDateTime(
                2023,
                10,
                29,
                2,
                15,
                30,
                tz="Europe/Amsterdam",
                disambiguate="earlier",
            ),
            True,
        ),
        (
            # skipped times are shifted into non-ambiguous times
            ZonedDateTime(2023, 3, 26, 2, 15, 30, tz="Europe/Amsterdam"),
            False,
        ),
    ],
)
def test_is_ambiguous(d, expect):
    assert d.is_ambiguous() == expect

    # the same result with SystemDateTime
    with system_tz(d.tz):
        d_system = d.to_system_tz()
        assert d_system.is_ambiguous() == expect


class TestDayLength:
    @pytest.mark.parametrize(
        "d, expect",
        [
            # no special day
            (
                ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam"),
                hours(24),
            ),
            (ZonedDateTime(1832, 12, 15, 12, 1, 30, tz="UTC"), hours(24)),
            # Longer day
            (
                ZonedDateTime(2023, 10, 29, 12, 8, 30, tz="Europe/Amsterdam"),
                hours(25),
            ),
            (ZonedDateTime(2023, 10, 29, tz="Europe/Amsterdam"), hours(25)),
            (
                ZonedDateTime(2023, 10, 30, tz="Europe/Amsterdam").subtract(
                    nanoseconds=1
                ),
                hours(25),
            ),
            # Shorter day
            (
                ZonedDateTime(2023, 3, 26, 12, 8, 30, tz="Europe/Amsterdam"),
                hours(23),
            ),
            (ZonedDateTime(2023, 3, 26, tz="Europe/Amsterdam"), hours(23)),
            (
                ZonedDateTime(2023, 3, 27, tz="Europe/Amsterdam").subtract(
                    nanoseconds=1
                ),
                hours(23),
            ),
            # non-hour DST change
            (
                ZonedDateTime(2024, 10, 6, 1, tz="Australia/Lord_Howe"),
                hours(23.5),
            ),
            (
                ZonedDateTime(2024, 4, 7, 1, tz="Australia/Lord_Howe"),
                hours(24.5),
            ),
            # Non-regular transition
            (
                ZonedDateTime(1894, 6, 1, 1, tz="Europe/Zurich"),
                TimeDelta(hours=24, minutes=-30, seconds=-14),
            ),
            # DST starts at midnight
            (ZonedDateTime(2016, 2, 20, tz="America/Sao_Paulo"), hours(25)),
            (ZonedDateTime(2016, 2, 21, tz="America/Sao_Paulo"), hours(24)),
            (ZonedDateTime(2016, 10, 16, tz="America/Sao_Paulo"), hours(23)),
            (ZonedDateTime(2016, 10, 17, tz="America/Sao_Paulo"), hours(24)),
            # Samoa skipped a day
            (ZonedDateTime(2011, 12, 31, 21, tz="Pacific/Apia"), hours(24)),
            (ZonedDateTime(2011, 12, 29, 21, tz="Pacific/Apia"), hours(24)),
            # A day that starts twice
            (
                ZonedDateTime(
                    2016,
                    2,
                    20,
                    23,
                    45,
                    disambiguate="later",
                    tz="America/Sao_Paulo",
                ),
                hours(25),
            ),
            (
                ZonedDateTime(
                    2016,
                    2,
                    20,
                    23,
                    45,
                    disambiguate="earlier",
                    tz="America/Sao_Paulo",
                ),
                hours(25),
            ),
        ],
    )
    def test_typical(self, d: ZonedDateTime, expect):
        assert d.day_length() == expect

        # test the same behavior on SystemDateTime
        with system_tz(d.tz):
            d_system = d.to_system_tz()
            assert d_system.day_length() == expect

    def test_extreme_bounds(self):
        # Negative UTC offsets at lower bound are fine
        d_min_neg = ZonedDateTime(1, 1, 1, 2, tz="America/New_York")
        assert d_min_neg.day_length() == hours(24)
        with system_tz("America/New_York"):
            try:
                assert SystemDateTime(1, 1, 1, 2).day_length() == hours(24)
            except (ValueError, OverflowError):
                pass  # a controlled exception is also fine--just no crash

        # Positive UTC offsets at lower bound are NOT fine
        d_min_pos = ZonedDateTime(1, 1, 1, 12, tz="Asia/Tokyo")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d_min_pos.day_length()
        with system_tz("Asia/Tokyo"):
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                SystemDateTime(1, 1, 1, 12).day_length()

        # upper bound is NOT fine
        d_max_pos = ZonedDateTime(9999, 12, 31, 4, tz="Asia/Tokyo")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d_max_pos.day_length()
        with system_tz("Asia/Tokyo"):
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                SystemDateTime(9999, 12, 31, 4).day_length()
        d_max_neg = ZonedDateTime(9999, 12, 31, 12, tz="America/New_York")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d_max_neg.day_length()
        with system_tz("America/New_York"):
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                SystemDateTime(9999, 12, 31, 12).day_length()


class TestStartOfDay:

    @pytest.mark.parametrize(
        "d, expect",
        [
            # no special day
            (
                ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam"),
                ZonedDateTime(2020, 8, 15, tz="Europe/Amsterdam"),
            ),
            (
                ZonedDateTime(1832, 12, 15, 12, 1, 30, tz="UTC"),
                ZonedDateTime(1832, 12, 15, tz="UTC"),
            ),
            # DST at non-midnight
            (
                ZonedDateTime(2023, 10, 29, 12, 8, 30, tz="Europe/Amsterdam"),
                ZonedDateTime(2023, 10, 29, tz="Europe/Amsterdam"),
            ),
            (
                ZonedDateTime(2023, 3, 26, 12, 8, 30, tz="Europe/Amsterdam"),
                ZonedDateTime(2023, 3, 26, tz="Europe/Amsterdam"),
            ),
            (
                ZonedDateTime(2024, 4, 7, 1, tz="Australia/Lord_Howe"),
                ZonedDateTime(2024, 4, 7, tz="Australia/Lord_Howe"),
            ),
            # Non-regular transition
            (
                ZonedDateTime(1894, 6, 1, 1, tz="Europe/Zurich"),
                ZonedDateTime(1894, 6, 1, 0, 30, 14, tz="Europe/Zurich"),
            ),
            # DST starts at midnight
            (
                ZonedDateTime(2016, 2, 20, 8, tz="America/Sao_Paulo"),
                ZonedDateTime(2016, 2, 20, tz="America/Sao_Paulo"),
            ),
            (
                ZonedDateTime(2016, 2, 21, 2, tz="America/Sao_Paulo"),
                ZonedDateTime(2016, 2, 21, tz="America/Sao_Paulo"),
            ),
            (
                ZonedDateTime(2016, 10, 16, 15, tz="America/Sao_Paulo"),
                ZonedDateTime(2016, 10, 16, 1, tz="America/Sao_Paulo"),
            ),
            (
                ZonedDateTime(2016, 10, 17, 19, tz="America/Sao_Paulo"),
                ZonedDateTime(2016, 10, 17, tz="America/Sao_Paulo"),
            ),
            # Samoa skipped a day
            (
                ZonedDateTime(2011, 12, 31, 21, tz="Pacific/Apia"),
                ZonedDateTime(2011, 12, 31, tz="Pacific/Apia"),
            ),
            (
                ZonedDateTime(2011, 12, 29, 21, tz="Pacific/Apia"),
                ZonedDateTime(2011, 12, 29, tz="Pacific/Apia"),
            ),
            # Another edge case
            (
                ZonedDateTime(2010, 11, 7, 23, tz="America/St_Johns"),
                ZonedDateTime(
                    2010, 11, 7, tz="America/St_Johns", disambiguate="earlier"
                ),
            ),
            # a day that starts twice
            (
                ZonedDateTime(
                    2016,
                    2,
                    20,
                    23,
                    45,
                    disambiguate="later",
                    tz="America/Sao_Paulo",
                ),
                ZonedDateTime(
                    2016, 2, 20, tz="America/Sao_Paulo", disambiguate="raise"
                ),
            ),
        ],
    )
    def test_examples(self, d: ZonedDateTime, expect):
        assert d.start_of_day().exact_eq(expect)

        # test the same behavior on SystemDateTime
        with system_tz(d.tz):
            d_system = d.to_system_tz()
            expect_system = expect.to_system_tz()
            assert d_system.start_of_day().exact_eq(expect_system)

    def test_extreme_boundaries(self):
        # Negative UTC offsets at lower bound are fine
        assert (
            ZonedDateTime(1, 1, 1, 2, tz="America/New_York")
            .start_of_day()
            .exact_eq(ZonedDateTime(1, 1, 1, tz="America/New_York"))
        )
        with system_tz("America/New_York"):
            try:
                assert (
                    SystemDateTime(1, 1, 1, 2)
                    .start_of_day()
                    .exact_eq(SystemDateTime(1, 1, 1))
                )
            except (ValueError, OverflowError):
                pass  # a controlled exception is also fine--just no crash

        # Positive UTC offsets at lower bound are NOT fine
        d_max_pos = ZonedDateTime(1, 1, 1, 12, tz="Asia/Tokyo")
        with pytest.raises((ValueError, OverflowError), match="range"):
            d_max_pos.start_of_day()
        with system_tz("Asia/Tokyo"):
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                SystemDateTime(1, 1, 1, 12).start_of_day()

        # Upper bound is always fine
        assert (
            ZonedDateTime(9999, 12, 31, 23, tz="Asia/Tokyo")
            .start_of_day()
            .exact_eq(ZonedDateTime(9999, 12, 31, tz="Asia/Tokyo"))
        )
        with system_tz("Asia/Tokyo"):
            try:
                assert (
                    SystemDateTime(9999, 12, 31, 23)
                    .start_of_day()
                    .exact_eq(SystemDateTime(9999, 12, 31))
                )
            except (ValueError, OverflowError):
                pass  # a controlled exception is also fine--just no crash

        assert (
            ZonedDateTime(9999, 12, 31, 12, tz="America/New_York")
            .start_of_day()
            .exact_eq(ZonedDateTime(9999, 12, 31, tz="America/New_York"))
        )
        with system_tz("America/New_York"):
            try:
                assert (
                    SystemDateTime(9999, 12, 31, 12)
                    .start_of_day()
                    .exact_eq(SystemDateTime(9999, 12, 31))
                )
            except (ValueError, OverflowError):
                pass  # a controlled exception is also fine--just no crash


def test_instant():
    assert (
        ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        .to_instant()
        .exact_eq(Instant.from_utc(2020, 8, 15, 10, 8, 30))
    )
    d = ZonedDateTime(
        2023,
        10,
        29,
        2,
        15,
        30,
        tz="Europe/Amsterdam",
        disambiguate="earlier",
    )
    assert d.to_instant().exact_eq(Instant.from_utc(2023, 10, 29, 0, 15, 30))
    assert (
        ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="later",
        )
        .to_instant()
        .exact_eq(Instant.from_utc(2023, 10, 29, 1, 15, 30))
    )

    with pytest.deprecated_call():
        assert d.to_instant() == d.instant()  # type: ignore[attr-defined]


def test_to_tz():
    assert (
        ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        .to_tz("America/New_York")
        .exact_eq(ZonedDateTime(2020, 8, 15, 6, 8, 30, tz="America/New_York"))
    )
    ams = ZonedDateTime(
        2023, 10, 29, 2, 15, 30, tz="Europe/Amsterdam", disambiguate="earlier"
    )
    nyc = ZonedDateTime(2023, 10, 28, 20, 15, 30, tz="America/New_York")
    assert ams.to_tz("America/New_York").exact_eq(nyc)
    assert (
        ams.replace(disambiguate="later")
        .to_tz("America/New_York")
        .exact_eq(nyc.replace(hour=21, disambiguate="raise"))
    )
    assert nyc.to_tz("Europe/Amsterdam").exact_eq(ams)
    assert (
        nyc.replace(hour=21, disambiguate="raise")
        .to_tz("Europe/Amsterdam")
        .exact_eq(ams.replace(disambiguate="later"))
    )
    # disambiguation doesn't affect NYC time because there's no ambiguity
    assert (
        nyc.replace(disambiguate="later")
        .to_tz("Europe/Amsterdam")
        .exact_eq(ams)
    )

    # catch local time sliding out of range
    small_zdt = ZonedDateTime(1, 1, 1, tz="Etc/UTC")
    with pytest.raises((ValueError, OverflowError, OSError)):
        small_zdt.to_tz("America/New_York")

    big_zdt = ZonedDateTime(9999, 12, 31, 23, tz="Etc/UTC")
    with pytest.raises((ValueError, OverflowError, OSError)):
        big_zdt.to_tz("Asia/Tokyo")


def test_to_fixed_offset():
    d = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")

    assert d.to_fixed_offset().exact_eq(
        OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=hours(2))
    )
    assert (
        d.replace(month=1, disambiguate="raise")
        .to_fixed_offset()
        .exact_eq(OffsetDateTime(2020, 1, 15, 12, 8, 30, offset=hours(1)))
    )
    assert (
        d.replace(month=1, disambiguate="raise")
        .to_fixed_offset(hours(4))
        .exact_eq(OffsetDateTime(2020, 1, 15, 15, 8, 30, offset=hours(4)))
    )
    assert d.to_fixed_offset(hours(0)).exact_eq(
        OffsetDateTime(2020, 8, 15, 10, 8, 30, offset=hours(0))
    )
    assert d.to_fixed_offset(-4).exact_eq(
        OffsetDateTime(2020, 8, 15, 6, 8, 30, offset=hours(-4))
    )

    # catch local time sliding out of range
    small_zdt = ZonedDateTime(1, 1, 1, tz="Etc/UTC")
    with pytest.raises((ValueError, OverflowError), match="range|year"):
        small_zdt.to_fixed_offset(-3)

    big_zdt = ZonedDateTime(9999, 12, 31, 23, tz="Etc/UTC")
    with pytest.raises((ValueError, OverflowError), match="range|year"):
        big_zdt.to_fixed_offset(4)


@system_tz_ams()
def test_to_system_tz():
    d = ZonedDateTime(2023, 10, 28, 2, 15, tz="Europe/Amsterdam")
    assert d.to_system_tz().exact_eq(SystemDateTime(2023, 10, 28, 2, 15))
    assert (
        d.replace(day=29, disambiguate="later")
        .to_system_tz()
        .exact_eq(SystemDateTime(2023, 10, 29, 2, 15, disambiguate="later"))
    )

    # catch local time sliding out of range
    small_zdt = ZonedDateTime(1, 1, 1, tz="Etc/UTC")
    with system_tz_nyc():
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            small_zdt.to_system_tz()

    big_zdt = ZonedDateTime(9999, 12, 31, 23, tz="Etc/UTC")
    with pytest.raises((ValueError, OverflowError), match="range|year"):
        big_zdt.to_system_tz()


class TestParseCommonIso:
    @pytest.mark.parametrize(
        "s, expect",
        [
            (
                "2020-08-15T12:08:30+02:00[Europe/Amsterdam]",
                ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam"),
            ),
            (
                "2020-08-15T12:08:30Z[Iceland]",
                ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Iceland"),
            ),
            # fractions
            (
                "2020-08-15T12:08:30.02320+02:00[Europe/Amsterdam]",
                ZonedDateTime(
                    2020,
                    8,
                    15,
                    12,
                    8,
                    30,
                    nanosecond=23_200_000,
                    tz="Europe/Amsterdam",
                ),
            ),
            (
                "2020-08-15T12:08:30,02320+02:00[Europe/Amsterdam]",
                ZonedDateTime(
                    2020,
                    8,
                    15,
                    12,
                    8,
                    30,
                    nanosecond=23_200_000,
                    tz="Europe/Amsterdam",
                ),
            ),
            (
                "2020-08-15T12:08:30.000000001+02:00[Europe/Berlin]",
                ZonedDateTime(
                    2020, 8, 15, 12, 8, 30, nanosecond=1, tz="Europe/Berlin"
                ),
            ),
            # second-level offset
            (
                "1900-01-01T23:34:39.01-00:25:21[Europe/Dublin]",
                ZonedDateTime(
                    1900,
                    1,
                    1,
                    23,
                    34,
                    39,
                    nanosecond=10_000_000,
                    tz="Europe/Dublin",
                ),
            ),
            (
                "2020-08-15T12:08:30+02:00:00[Europe/Berlin]",
                ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Berlin"),
            ),
            # offset disambiguates
            (
                "2023-10-29T02:15:30+01:00[Europe/Amsterdam]",
                ZonedDateTime(
                    2023,
                    10,
                    29,
                    2,
                    15,
                    30,
                    tz="Europe/Amsterdam",
                    disambiguate="later",
                ),
            ),
            (
                "2023-10-29T02:15:30+02:00[Europe/Amsterdam]",
                ZonedDateTime(
                    2023,
                    10,
                    29,
                    2,
                    15,
                    30,
                    tz="Europe/Amsterdam",
                    disambiguate="earlier",
                ),
            ),
            # Offsets are optional
            (
                "2023-08-25T12:15:30[Europe/Amsterdam]",
                ZonedDateTime(2023, 8, 25, 12, 15, 30, tz="Europe/Amsterdam"),
            ),
            # no offset for skipped time
            (
                "2023-03-26T02:15:30[Europe/Amsterdam]",
                ZonedDateTime(
                    2023,
                    3,
                    26,
                    2,
                    15,
                    30,
                    tz="Europe/Amsterdam",
                    disambiguate="compatible",
                ),
            ),
            # Alternate formats
            (
                "20200815 12:08:30+02:00[Europe/Amsterdam]",
                ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam"),
            ),
            (
                "2020-02-15t120830z[Europe/London]",
                ZonedDateTime(2020, 2, 15, 12, 8, 30, tz="Europe/London"),
            ),
            (
                "2020-08-15T12:08:30+02[Europe/Amsterdam]",
                ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam"),
            ),
            # Z is also valid for non-0 offset timezones!
            (
                "2020-02-15t120830z[America/New_York]",
                ZonedDateTime(2020, 2, 15, 7, 8, 30, tz="America/New_York"),
            ),
        ],
    )
    def test_valid(self, s, expect):
        assert ZonedDateTime.parse_common_iso(s).exact_eq(expect)

    @pytest.mark.parametrize(
        "s",
        [
            "2020-08-15T12:08:30+02:00",  # no tz
            # bracket problems
            "2020-08-15T12:08:30+02:00[Europe/Amsterdam",
            "2020-08-15T12:08:30+02:00[Europe][Amsterdam]",
            "2020-08-15T12:08:30+02:00Europe/Amsterdam]",
            "2023-10-29T02:15:30+02:00(Europe/Amsterdam)",
            # separator problems
            "2020-08-15_12:08:30+02:00[Europe/Amsterdam]",
            "2020-08-15T12.08:30+02:00[Europe/Amsterdam]",
            "2020_08-15T12:08:30+02:00[Europe/Amsterdam]",
            # padding problems
            "2020-08-15T12:8:30+02:00[Europe/Amsterdam]",
            # invalid values
            "2020-08-32T12:08:30+02:00[Europe/Amsterdam]",
            "2020-08-12T12:68:30+02:00[Europe/Amsterdam]",
            "2020-08-12T12:68:30+99:00[Europe/Amsterdam]",
            "2020-08-12T12:68:30+14:89[Europe/Amsterdam]",
            "2020-08-12T12:68:30+01:00[Europe/Amsterdam]",
            "2020-08-12T12:68:30+14:29:60[Europe/Amsterdam]",
            "2023-10-29T02:15:30>02:00[Europe/Amsterdam]",
            # trailing/leading space
            " 2023-10-29T02:15:30+02:00[Europe/Amsterdam]",
            "2023-10-29T02:15:30+02:00[Europe/Amsterdam] ",
            # invalid offsets
            "1900-01-01T23:34:39.01-00:24:81[Europe/Dublin]",
            "2020-01-01T00:00:00+04:90[Asia/Calcutta]",
            "2023-10-29",  # only date
            "02:15:30",  # only time
            "2023-10-29T02:15:30",  # no offset
            "",  # empty
            "garbage",  # garbage
            "2023-10-29T02:15:30.0000000001+02:00[Europe/Amsterdam]",  # overly precise fraction
            "2023-10-29T02:15:30+02:00:00.00[Europe/Amsterdam]",  # subsecond offset
            "2023-10-29T02:15:30+0𝟙:00[Europe/Amsterdam]",
            "2020-08-15T12:08:30.000000001+29:00[Europe/Berlin]",  # out of range offset
            # decimal problems
            "2020-08-15T12:08:30.+02:00[Europe/Paris]",
            "2020-08-15T12:08:30. +02:00[Europe/Paris]",
            "2020-08-15T12:08:30,+02:00[Europe/Paris]",
            "2020-08-15T12:08:30,Z[Europe/Paris]",
        ],
    )
    def test_invalid(self, s):
        with pytest.raises(ValueError, match="format.*" + re.escape(s)):
            ZonedDateTime.parse_common_iso(s)

    def test_invalid_tz(self):
        with pytest.raises(TimeZoneNotFoundError):
            ZonedDateTime.parse_common_iso(
                "2020-08-15T12:08:30+02:00[Europe/Nowhere]"
            )

        with pytest.raises(TimeZoneNotFoundError):
            ZonedDateTime.parse_common_iso("2020-08-15T12:08:30Z[X]")

        with pytest.raises((TimeZoneNotFoundError, ValueError)):
            ZonedDateTime.parse_common_iso(
                f"2023-10-29T02:15:30+02:00[{'X'*9999}]"
            )

        with pytest.raises((TimeZoneNotFoundError, ValueError)):
            ZonedDateTime.parse_common_iso(
                f"2023-10-29T02:15:30+02:00[{chr(1600)}]",
            )

        assert issubclass(TimeZoneNotFoundError, ValueError)

    @pytest.mark.parametrize(
        "s",
        [
            "0001-01-01T00:15:30+09:00[Etc/GMT-9]",
            "9999-12-31T20:15:30-09:00[Etc/GMT+9]",
            "9999-12-31T20:15:30Z[Asia/Tokyo]",
            "0001-01-01T00:15:30Z[America/New_York]",
        ],
    )
    def test_out_of_range(self, s):
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            ZonedDateTime.parse_common_iso(s)

    def test_offset_timezone_mismatch(self):
        with pytest.raises(InvalidOffsetError):
            # at the exact DST transition
            ZonedDateTime.parse_common_iso(
                "2023-10-29T02:15:30+03:00[Europe/Amsterdam]"
            )
        with pytest.raises(InvalidOffsetError):
            # some other time in the year
            ZonedDateTime.parse_common_iso(
                "2020-08-15T12:08:30+01:00:01[Europe/Amsterdam]"
            )

        with pytest.raises(InvalidOffsetError):
            # some other time in the year
            ZonedDateTime.parse_common_iso(
                "2020-08-15T12:08:30+00:00[Europe/Amsterdam]"
            )

        assert issubclass(InvalidOffsetError, ValueError)

    def test_skipped_time(self):
        with pytest.raises(InvalidOffsetError):
            ZonedDateTime.parse_common_iso(
                "2023-03-26T02:15:30+01:00[Europe/Amsterdam]"
            )

    @given(text())
    def test_fuzzing(self, s: str):
        with pytest.raises(
            ValueError,
            match=r"Invalid format.*" + re.escape(repr(s)),
        ):
            ZonedDateTime.parse_common_iso(s)


class TestTimestamp:

    def test_default_seconds(self):
        assert ZonedDateTime(1970, 1, 1, tz="Iceland").timestamp() == 0
        assert (
            ZonedDateTime(
                2020, 8, 15, 8, 8, 30, nanosecond=45_123, tz="America/New_York"
            ).timestamp()
            == 1_597_493_310
        )

        ambiguous = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        assert (
            ambiguous.timestamp()
            != ambiguous.replace(disambiguate="later").timestamp()
        )

    def test_millis(self):
        assert ZonedDateTime(1970, 1, 1, tz="Iceland").timestamp_millis() == 0
        assert (
            ZonedDateTime(
                2020,
                8,
                15,
                8,
                8,
                30,
                nanosecond=45_923_789,
                tz="America/New_York",
            ).timestamp_millis()
            == 1_597_493_310_045
        )

        ambiguous = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        assert (
            ambiguous.timestamp_millis()
            != ambiguous.replace(disambiguate="later").timestamp_millis()
        )

    def test_nanos(self):
        assert ZonedDateTime(1970, 1, 1, tz="Iceland").timestamp_nanos() == 0
        assert (
            ZonedDateTime(
                2020,
                8,
                15,
                8,
                8,
                30,
                nanosecond=45_123_789,
                tz="America/New_York",
            ).timestamp_nanos()
            == 1_597_493_310_045_123_789
        )

        ambiguous = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        assert (
            ambiguous.timestamp_nanos()
            != ambiguous.replace(disambiguate="later").timestamp_nanos()
        )


class TestFromTimestamp:

    @pytest.mark.parametrize(
        "method, factor",
        [
            (ZonedDateTime.from_timestamp, 1),
            (ZonedDateTime.from_timestamp_millis, 1_000),
            (ZonedDateTime.from_timestamp_nanos, 1_000_000_000),
        ],
    )
    def test_all(self, method, factor):
        assert method(0, tz="Iceland").exact_eq(
            ZonedDateTime(1970, 1, 1, tz="Iceland")
        )
        assert method(1_597_493_310 * factor, tz="America/Nuuk").exact_eq(
            ZonedDateTime(2020, 8, 15, 10, 8, 30, tz="America/Nuuk")
        )
        with pytest.raises((OSError, OverflowError, ValueError)):
            method(1_000_000_000_000_000_000 * factor, tz="America/Nuuk")

        with pytest.raises((OSError, OverflowError, ValueError)):
            method(-1_000_000_000_000_000_000 * factor, tz="America/Nuuk")

        with pytest.raises((TypeError, AttributeError)):
            method(0, tz=3)

        with pytest.raises(TypeError):
            method("0", tz="America/New_York")

        with pytest.raises(TimeZoneNotFoundError):
            method(0, tz="America/Nowhere")

        with pytest.raises(TypeError, match="got 3|foo"):
            method(0, tz="America/New_York", foo="bar")

        with pytest.raises(TypeError, match="positional|ts"):
            method(ts=0, tz="America/New_York")

        with pytest.raises(TypeError):
            method(0, foo="bar")

        with pytest.raises(TypeError):
            method(0)

        with pytest.raises(TypeError):
            method(0, "bar")

        assert ZonedDateTime.from_timestamp_millis(
            -4, tz="America/Nuuk"
        ).to_instant() == Instant.from_timestamp(0) - milliseconds(4)

        assert ZonedDateTime.from_timestamp_nanos(
            -4, tz="America/Nuuk"
        ).to_instant() == Instant.from_timestamp(0).subtract(nanoseconds=4)

    def test_nanos(self):
        assert ZonedDateTime.from_timestamp_nanos(
            1_597_493_310_123_456_789, tz="America/Nuuk"
        ).exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                10,
                8,
                30,
                nanosecond=123_456_789,
                tz="America/Nuuk",
            )
        )

    def test_millis(self):
        assert ZonedDateTime.from_timestamp_millis(
            1_597_493_310_123, tz="America/Nuuk"
        ).exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                10,
                8,
                30,
                nanosecond=123_000_000,
                tz="America/Nuuk",
            )
        )

    def test_float(self):
        assert ZonedDateTime.from_timestamp(
            1.0,
            tz="America/New_York",
        ).exact_eq(
            ZonedDateTime.from_timestamp(
                1,
                tz="America/New_York",
            )
        )

        assert ZonedDateTime.from_timestamp(
            1.000_000_001,
            tz="America/New_York",
        ).exact_eq(
            ZonedDateTime.from_timestamp(
                1,
                tz="America/New_York",
            ).add(
                nanoseconds=1,
            )
        )

        assert ZonedDateTime.from_timestamp(
            -9.000_000_100,
            tz="America/New_York",
        ).exact_eq(
            ZonedDateTime.from_timestamp(
                -9,
                tz="America/New_York",
            ).subtract(
                nanoseconds=100,
            )
        )

        with pytest.raises((ValueError, OverflowError)):
            ZonedDateTime.from_timestamp(9e200, tz="America/New_York")

        with pytest.raises((ValueError, OverflowError, OSError)):
            ZonedDateTime.from_timestamp(
                float(Instant.MAX.timestamp()) + 0.99999999,
                tz="America/New_York",
            )

        with pytest.raises((ValueError, OverflowError)):
            ZonedDateTime.from_timestamp(float("inf"), tz="America/New_York")

        with pytest.raises((ValueError, OverflowError)):
            ZonedDateTime.from_timestamp(float("nan"), tz="America/New_York")


def test_repr():
    d = ZonedDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=9_876_543, tz="Australia/Darwin"
    )
    assert (
        repr(d) == "ZonedDateTime(2020-08-15 23:12:09.009876543"
        "+09:30[Australia/Darwin])"
    )
    assert (
        repr(ZonedDateTime(2020, 8, 15, 23, 12, tz="Iceland"))
        == "ZonedDateTime(2020-08-15 23:12:00+00:00[Iceland])"
    )
    assert (
        repr(ZonedDateTime(2020, 8, 15, 23, 12, tz="UTC"))
        == "ZonedDateTime(2020-08-15 23:12:00+00:00[UTC])"
    )


class TestComparison:
    def test_different_timezones(self):
        d = ZonedDateTime(2020, 8, 15, 15, 12, 9, tz="Asia/Kolkata")
        later = ZonedDateTime(2020, 8, 15, 14, tz="Europe/Amsterdam")

        assert d < later
        assert d <= later
        assert later > d
        assert later >= d
        assert not d > later
        assert not d >= later
        assert not later < d
        assert not later <= d

    def test_same_timezone_ambiguity(self):
        d = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        later = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="later",
        )
        assert d < later
        assert d <= later
        assert later > d
        assert later >= d
        assert not d > later
        assert not d >= later
        assert not later < d
        assert not later <= d

    def test_different_timezone_same_time(self):
        d = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        other = d.to_tz("America/New_York")
        assert not d < other
        assert d <= other
        assert not other > d
        assert other >= d
        assert not d > other
        assert d >= other
        assert not other < d
        assert other <= d

    def test_instant(self):
        d = ZonedDateTime(
            2023, 10, 29, 2, 30, tz="Europe/Amsterdam", disambiguate="later"
        )

        inst_eq = d.to_instant()
        inst_lt = inst_eq - minutes(1)
        inst_gt = inst_eq + minutes(1)

        assert d >= inst_eq
        assert d <= inst_eq
        assert not d > inst_eq
        assert not d < inst_eq

        assert d > inst_lt
        assert d >= inst_lt
        assert not d < inst_lt
        assert not d <= inst_lt

        assert d < inst_gt
        assert d <= inst_gt
        assert not d > inst_gt
        assert not d >= inst_gt

    def test_offset(self):
        d = ZonedDateTime(
            2023, 10, 29, 2, 30, tz="Europe/Amsterdam", disambiguate="later"
        )

        offset_eq = d.to_fixed_offset()
        offset_lt = offset_eq.replace(minute=29, ignore_dst=True)
        offset_gt = offset_eq.replace(minute=31, ignore_dst=True)

        assert d >= offset_eq
        assert d <= offset_eq
        assert not d > offset_eq
        assert not d < offset_eq

        assert d > offset_lt
        assert d >= offset_lt
        assert not d < offset_lt
        assert not d <= offset_lt

        assert d < offset_gt
        assert d <= offset_gt
        assert not d > offset_gt
        assert not d >= offset_gt

    def test_system_tz(self):
        d = ZonedDateTime(
            2023, 10, 29, 2, 30, tz="Europe/Amsterdam", disambiguate="earlier"
        )

        sys_eq = d.to_system_tz()
        sys_lt = sys_eq.replace(minute=29, disambiguate="earlier")
        sys_gt = sys_eq.replace(minute=31, disambiguate="earlier")

        assert d >= sys_eq
        assert d <= sys_eq
        assert not d > sys_eq
        assert not d < sys_eq

        assert d > sys_lt
        assert d >= sys_lt
        assert not d < sys_lt
        assert not d <= sys_lt

        assert d < sys_gt
        assert d <= sys_gt
        assert not d > sys_gt
        assert not d >= sys_gt

    def test_notimplemented(self):
        d = ZonedDateTime(2020, 8, 15, tz="Europe/Amsterdam")
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
        with pytest.raises(TypeError):
            d <= 42  # type: ignore[operator]
        with pytest.raises(TypeError):
            d > 42  # type: ignore[operator]
        with pytest.raises(TypeError):
            d >= 42  # type: ignore[operator]
        with pytest.raises(TypeError):
            42 < d  # type: ignore[operator]
        with pytest.raises(TypeError):
            42 <= d  # type: ignore[operator]
        with pytest.raises(TypeError):
            42 > d  # type: ignore[operator]
        with pytest.raises(TypeError):
            42 >= d  # type: ignore[operator]


def test_py_datetime():
    d = ZonedDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=987_654_999, tz="Europe/Amsterdam"
    )
    assert d.py_datetime() == py_datetime(
        2020, 8, 15, 23, 12, 9, 987_654, tzinfo=ZoneInfo("Europe/Amsterdam")
    )

    # ambiguous time
    d2 = ZonedDateTime(
        2023,
        10,
        29,
        2,
        15,
        tz="Europe/Amsterdam",
        disambiguate="earlier",
    )
    assert d2.py_datetime().fold == 0
    assert d2.replace(disambiguate="later").py_datetime().fold == 1

    # ensure the ZoneInfo isn't file-based, and can thus be pickled
    pickle.dumps(d2)


class TestFromPyDatetime:

    def test_simple(self):
        d = py_datetime(
            2020, 8, 15, 23, 12, 9, 987_654, tzinfo=ZoneInfo("Europe/Paris")
        )
        assert ZonedDateTime.from_py_datetime(d).exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                23,
                12,
                9,
                nanosecond=987_654_000,
                tz="Europe/Paris",
            )
        )

    def test_subclass(self):
        class MyDatetime(py_datetime):
            pass

        assert ZonedDateTime.from_py_datetime(
            MyDatetime(
                2020,
                8,
                15,
                23,
                12,
                9,
                987_654,
                tzinfo=ZoneInfo("Europe/Paris"),
            )
        ).exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                23,
                12,
                9,
                nanosecond=987_654_000,
                tz="Europe/Paris",
            )
        )

    def test_wrong_tzinfo(self):
        d = py_datetime(
            2020, 8, 15, 23, 12, 9, 987_654, tzinfo=py_timezone.utc
        )
        with pytest.raises(ValueError, match="datetime.timezone"):
            ZonedDateTime.from_py_datetime(d)

    def test_zoneinfo_subclass(self):

        # ZoneInfo subclass also not allowed
        class MyZoneInfo(ZoneInfo):
            pass

        with pytest.raises(ValueError, match="ZoneInfo.*MyZoneInfo"):
            ZonedDateTime.from_py_datetime(
                py_datetime(
                    2020,
                    8,
                    15,
                    23,
                    12,
                    9,
                    987_654,
                    tzinfo=MyZoneInfo("Europe/Paris"),
                )
            )

    def test_naive(self):

        with pytest.raises(ValueError, match="None"):
            ZonedDateTime.from_py_datetime(py_datetime(2020, 3, 4))

    def test_skipped_time(self):
        assert ZonedDateTime.from_py_datetime(
            py_datetime(
                2023, 3, 26, 2, 15, 30, tzinfo=ZoneInfo("Europe/Amsterdam")
            )
        ).exact_eq(
            ZonedDateTime(2023, 3, 26, 3, 15, 30, tz="Europe/Amsterdam")
        )
        assert ZonedDateTime.from_py_datetime(
            py_datetime(
                2023,
                3,
                26,
                2,
                15,
                30,
                fold=1,
                tzinfo=ZoneInfo("Europe/Amsterdam"),
            )
        ).exact_eq(
            ZonedDateTime(2023, 3, 26, 1, 15, 30, tz="Europe/Amsterdam")
        )

    def test_repeated_time(self):
        assert ZonedDateTime.from_py_datetime(
            py_datetime(
                2023, 10, 29, 2, 15, 30, tzinfo=ZoneInfo("Europe/Amsterdam")
            )
        ).exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                2,
                15,
                30,
                tz="Europe/Amsterdam",
                disambiguate="earlier",
            )
        )
        assert ZonedDateTime.from_py_datetime(
            py_datetime(
                2023,
                10,
                29,
                2,
                15,
                30,
                fold=1,
                tzinfo=ZoneInfo("Europe/Amsterdam"),
            )
        ).exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                2,
                15,
                30,
                tz="Europe/Amsterdam",
                disambiguate="later",
            )
        )

    def test_out_of_range(self):
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            ZonedDateTime.from_py_datetime(
                py_datetime(1, 1, 1, tzinfo=ZoneInfo("Asia/Kolkata"))
            )

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            ZonedDateTime.from_py_datetime(
                py_datetime(
                    9999, 12, 31, 22, tzinfo=ZoneInfo("America/New_York")
                )
            )

    def test_zoneinfo_key_is_none(self):
        with TEST_DIR.joinpath("tzif/Amsterdam.tzif").open("rb") as f:
            tz = ZoneInfo.from_file(f)

        with pytest.raises(ValueError, match="key"):
            ZonedDateTime.from_py_datetime(
                py_datetime(2020, 8, 15, 12, 8, 30, tzinfo=tz)
            )


def test_now():
    now = ZonedDateTime.now("Iceland")
    assert now.tz == "Iceland"
    py_now = py_datetime.now(ZoneInfo("Iceland"))
    assert py_now - now.py_datetime() < py_timedelta(seconds=1)


class TestExactEquality:
    def test_same_exact(self):
        a = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        b = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        assert a.exact_eq(b)

    def test_different_zones(self):
        a = ZonedDateTime(
            2020, 8, 15, 12, 43, nanosecond=1, tz="Europe/Amsterdam"
        )
        b = a.to_tz("America/New_York")
        assert a == b
        assert not a.exact_eq(b)

        # Different zone but same offset
        c = a.to_tz("Europe/Paris")
        assert a == c
        assert not a.exact_eq(c)

    def test_same_timezone_ambiguity(self):
        a = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            nanosecond=1,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        b = a.replace(disambiguate="later")
        assert a != b
        assert not a.exact_eq(b)

    def test_same_ambiguous(self):
        a = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            nanosecond=1,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        b = a.replace(disambiguate="earlier")
        assert a.exact_eq(b)

    def test_same_unambiguous(self):
        a = ZonedDateTime(
            2020, 8, 15, 12, 43, nanosecond=1, tz="Europe/Amsterdam"
        )
        b = a.replace(disambiguate="later")
        assert a.exact_eq(b)
        assert a.exact_eq(b.replace(disambiguate="later"))

    def test_invalid(self):
        a = ZonedDateTime(2020, 8, 15, 12, 8, 30, tz="Europe/Amsterdam")
        with pytest.raises(TypeError):
            a.exact_eq(42)  # type: ignore[arg-type]

        with pytest.raises(TypeError):
            a.exact_eq(a.to_instant())  # type: ignore[arg-type]


class TestReplace:
    def test_basics(self):
        d = ZonedDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654, tz="Europe/Amsterdam"
        )
        assert d.replace(year=2021).exact_eq(
            ZonedDateTime(
                2021,
                8,
                15,
                23,
                12,
                9,
                nanosecond=987_654,
                tz="Europe/Amsterdam",
            )
        )
        assert d.replace(month=9, disambiguate="raise").exact_eq(
            ZonedDateTime(
                2020,
                9,
                15,
                23,
                12,
                9,
                nanosecond=987_654,
                tz="Europe/Amsterdam",
            )
        )
        assert d.replace(day=16, disambiguate="raise").exact_eq(
            ZonedDateTime(
                2020,
                8,
                16,
                23,
                12,
                9,
                nanosecond=987_654,
                tz="Europe/Amsterdam",
            )
        )
        assert d.replace(hour=0, disambiguate="raise").exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                0,
                12,
                9,
                nanosecond=987_654,
                tz="Europe/Amsterdam",
            )
        )
        assert d.replace(minute=0, disambiguate="raise").exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                23,
                0,
                9,
                nanosecond=987_654,
                tz="Europe/Amsterdam",
            )
        )
        assert d.replace(second=0, disambiguate="raise").exact_eq(
            ZonedDateTime(
                2020,
                8,
                15,
                23,
                12,
                0,
                nanosecond=987_654,
                tz="Europe/Amsterdam",
            )
        )
        assert d.replace(nanosecond=0, disambiguate="raise").exact_eq(
            ZonedDateTime(
                2020, 8, 15, 23, 12, 9, nanosecond=0, tz="Europe/Amsterdam"
            )
        )
        assert d.replace(tz="Iceland", disambiguate="raise").exact_eq(
            ZonedDateTime(
                2020, 8, 15, 23, 12, 9, nanosecond=987_654, tz="Iceland"
            )
        )

    def test_invalid(self):
        d = ZonedDateTime(2020, 8, 15, tz="Europe/Amsterdam")

        with pytest.raises(TypeError, match="tzinfo"):
            d.replace(tzinfo=py_timezone.utc, disambiguate="compatible")  # type: ignore[call-arg]

        with pytest.raises(TypeError, match="fold"):
            d.replace(fold=1, disambiguate="compatible")  # type: ignore[call-arg]

        with pytest.raises(TypeError, match="foo"):
            d.replace(foo="bar", disambiguate="compatible")  # type: ignore[call-arg]

        with pytest.raises(TimeZoneNotFoundError, match="Nowhere"):
            d.replace(tz="Nowhere", disambiguate="compatible")

        with pytest.raises(ValueError, match="date|day"):
            d.replace(year=2023, month=2, day=29, disambiguate="compatible")

        with pytest.raises(ValueError, match="nano|time"):
            d.replace(nanosecond=1_000_000_000, disambiguate="compatible")

    def test_repeated_time(self):
        d = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        d_later = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="later",
        )
        with pytest.raises(
            RepeatedTime,
            match="2023-10-29 02:15:30 is repeated in timezone 'Europe/Amsterdam'",
        ):
            d.replace(disambiguate="raise")

        assert d.replace(disambiguate="later").exact_eq(d_later)
        assert d.replace(disambiguate="earlier").exact_eq(d)
        assert d.replace(disambiguate="compatible").exact_eq(d)

        # earlier offset is reused if possible
        assert d.replace().exact_eq(d)
        assert d_later.replace().exact_eq(d_later)
        assert d.replace(minute=30).exact_eq(
            d.replace(minute=30, disambiguate="earlier")
        )
        assert d_later.replace(minute=30).exact_eq(
            d_later.replace(minute=30, disambiguate="later")
        )

        # Disambiguation may differ depending on whether we change tz
        assert d_later.replace(minute=30, tz="Europe/Amsterdam").exact_eq(
            d_later.replace(minute=30)
        )
        assert not d_later.replace(minute=30, tz="Europe/Paris").exact_eq(
            d_later.replace(minute=30)
        )

        # don't reuse offset per se when changing timezone
        assert d.replace(hour=3, tz="Europe/Athens").exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                3,
                15,
                30,
                tz="Europe/Athens",
                disambiguate="earlier",
            )
        )
        assert d_later.replace(hour=1, tz="Europe/London").exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                1,
                15,
                30,
                tz="Europe/London",
                disambiguate="earlier",
            )
        )
        assert d.replace(hour=1, tz="Europe/London").exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                1,
                15,
                30,
                tz="Europe/London",
            )
        )
        assert d_later.replace(hour=3, tz="Europe/Athens").exact_eq(
            ZonedDateTime(
                2023,
                10,
                29,
                3,
                15,
                30,
                tz="Europe/Athens",
            )
        )

    def test_skipped_time(self):
        d = ZonedDateTime(2023, 3, 26, 1, 15, 30, tz="Europe/Amsterdam")
        d_later = ZonedDateTime(2023, 3, 26, 3, 15, 30, tz="Europe/Amsterdam")
        with pytest.raises(
            SkippedTime,
            match="2023-03-26 02:15:30 is skipped in timezone 'Europe/Amsterdam'",
        ):
            d.replace(hour=2, disambiguate="raise")

        # Disambiguation may differ depending on whether we change tz
        assert d.replace(
            hour=2, disambiguate="earlier", tz="Europe/Amsterdam"
        ).exact_eq(d)
        assert not d.replace(hour=2, tz="Europe/Paris").exact_eq(d)

        assert d.replace(hour=2, disambiguate="earlier").exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                2,
                15,
                30,
                tz="Europe/Amsterdam",
                disambiguate="earlier",
            )
        )

        assert d.replace(hour=2, disambiguate="later").exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                2,
                15,
                30,
                tz="Europe/Amsterdam",
                disambiguate="later",
            )
        )

        assert d.replace(hour=2, disambiguate="compatible").exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                2,
                15,
                30,
                tz="Europe/Amsterdam",
                disambiguate="compatible",
            )
        )
        # Don't per se reuse the offset when changing timezone
        assert d.replace(tz="Europe/London").exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                1,
                15,
                30,
                tz="Europe/London",
                disambiguate="later",
            )
        )
        assert d_later.replace(tz="Europe/Athens").exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                4,
                15,
                30,
                tz="Europe/Athens",
            )
        )
        # can't reuse offset
        assert d.replace(hour=3, tz="Europe/Athens").exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                4,
                15,
                30,
                tz="Europe/Athens",
            )
        )
        assert d_later.replace(hour=1, tz="Europe/London").exact_eq(
            ZonedDateTime(
                2023,
                3,
                26,
                2,
                15,
                30,
                tz="Europe/London",
            )
        )

    def test_out_of_range(self):
        d = ZonedDateTime(1, 1, 1, tz="America/New_York")

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.replace(tz="Europe/Amsterdam", disambiguate="compatible")

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.replace(
                year=9999, month=12, day=31, hour=23, disambiguate="compatible"
            )


class TestShiftTimeUnits:
    def test_zero(self):
        d = ZonedDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654, tz="Europe/Amsterdam"
        )
        assert (d + hours(0)).exact_eq(d)

        # the same with the method
        assert d.add().exact_eq(d)

        # the same with subtraction
        assert (d - hours(0)).exact_eq(d)
        assert d.subtract().exact_eq(d)

    def test_ambiguous_plus_zero(self):
        d = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            disambiguate="earlier",
            tz="Europe/Amsterdam",
        )
        assert (d + hours(0)).exact_eq(d)
        assert (d.replace(disambiguate="later") + hours(0)).exact_eq(
            d.replace(disambiguate="later")
        )

        # the equivalent with the method
        assert d.add(hours=0).exact_eq(d)
        assert (
            d.replace(disambiguate="later")
            .add(hours=0)
            .exact_eq(d.replace(disambiguate="later"))
        )

        # equivalent with subtraction
        assert (d - hours(0)).exact_eq(d)
        assert d.subtract(hours=0).exact_eq(d)

    @system_tz_ams()
    def test_accounts_for_dst(self):
        d = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            disambiguate="earlier",
            tz="Europe/Amsterdam",
        )
        assert (d + hours(24)).exact_eq(
            ZonedDateTime(2023, 10, 30, 1, 15, 30, tz="Europe/Amsterdam")
        )
        assert (d.replace(disambiguate="later") + hours(24)).exact_eq(
            ZonedDateTime(2023, 10, 30, 2, 15, 30, tz="Europe/Amsterdam")
        )

        # the equivalent with the method (kwargs)
        assert d.add(hours=24).exact_eq(d + hours(24))
        assert (
            d.replace(disambiguate="later")
            .add(hours=24)
            .exact_eq(d.replace(disambiguate="later") + hours(24))
        )

        # equivalent with method (arg)
        assert d.add(hours(24)).exact_eq(d + hours(24))
        assert (
            d.replace(disambiguate="later")
            .add(hours(24))
            .exact_eq(d.replace(disambiguate="later") + hours(24))
        )

        # equivalent with subtraction
        assert (d - hours(-24)).exact_eq(
            ZonedDateTime(2023, 10, 30, 1, 15, 30, tz="Europe/Amsterdam")
        )
        assert d.subtract(hours=-24).exact_eq(d + hours(24))
        assert (
            d.replace(disambiguate="later")
            .subtract(hours=-24)
            .exact_eq(d.replace(disambiguate="later") + hours(24))
        )

    @system_tz_ams()
    def test_out_of_range(self):
        d = ZonedDateTime(2020, 8, 15, tz="Africa/Abidjan")

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d + hours(24 * 366 * 8_000)

        # the equivalent with the method
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(hours=24 * 366 * 8_000)

    @system_tz_ams()
    def test_not_implemented(self):
        d = ZonedDateTime(2020, 8, 15, tz="Asia/Tokyo")
        with pytest.raises(TypeError, match="unsupported operand type"):
            d + 42  # type: ignore[operator]

        with pytest.raises(TypeError, match="unsupported operand type"):
            d - 42  # type: ignore[operator]

        with pytest.raises(TypeError, match="unsupported operand type"):
            42 + d  # type: ignore[operator]

        with pytest.raises(TypeError, match="unsupported operand type"):
            42 - d  # type: ignore[operator]

        with pytest.raises(TypeError, match="unsupported operand type"):
            years(1) + d  # type: ignore[operator]

        with pytest.raises(TypeError, match="unsupported operand type"):
            years(1) - d  # type: ignore[operator]

        with pytest.raises(TypeError, match="unsupported operand type"):
            d + d  # type: ignore[operator]

        with pytest.raises((TypeError, AttributeError)):
            d.add(4)  # type: ignore[call-overload]

        # mix args/kwargs
        with pytest.raises(TypeError):
            d.add(hours(34), seconds=3)  # type: ignore[call-overload]


class TestShiftDateUnits:

    def test_zero(self):
        d = ZonedDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654_321, tz="Asia/Tokyo"
        )
        assert d.add(days=0, disambiguate="raise").exact_eq(d)
        assert d.add(days=0).exact_eq(d)
        assert d.add(weeks=0).exact_eq(d)
        assert d.add(months=0).exact_eq(d)
        assert d.add(years=0, weeks=0).exact_eq(d)
        assert d.add().exact_eq(d)

        # same with operators
        assert (d + days(0)).exact_eq(d)
        assert (d + weeks(0)).exact_eq(d)
        assert (d + years(0)).exact_eq(d)

        # same with subtraction
        assert d.subtract(days=0, disambiguate="raise").exact_eq(d)
        assert d.subtract(days=0).exact_eq(d)

        assert (d - days(0)).exact_eq(d)
        assert (d - weeks(0)).exact_eq(d)
        assert (d - years(0)).exact_eq(d)

    def test_simple_date(self):
        d = ZonedDateTime(
            2020,
            8,
            15,
            23,
            12,
            9,
            nanosecond=987_654_321,
            tz="Australia/Sydney",
        )
        assert d.add(days=1).exact_eq(d.replace(day=16))
        assert d.add(years=1, weeks=2, days=-2).exact_eq(
            d.replace(year=2021, day=27)
        )

        # same with subtraction
        assert d.subtract(days=1).exact_eq(d.replace(day=14))
        assert d.subtract(years=1, weeks=2, days=-2).exact_eq(
            d.replace(year=2019, day=3)
        )

        assert d.add(years=1, weeks=2, days=-2).exact_eq(
            d.replace(year=2021, day=27)
        )
        # same with arg
        assert d.add(years(1) + weeks(2) + days(-2)).exact_eq(
            d.add(years=1, weeks=2, days=-2)
        )
        assert d.add(years(1) + weeks(2) + hours(2)).exact_eq(
            d.add(years=1, weeks=2, hours=2)
        )
        # same with operators
        assert (d + (years(1) + weeks(2) + days(-2))).exact_eq(
            d.add(years=1, weeks=2, days=-2)
        )
        assert (d + (years(1) + weeks(2) + hours(2))).exact_eq(
            d.add(years=1, weeks=2, hours=2)
        )
        assert (d - (years(1) + weeks(2) + days(-2))).exact_eq(
            d.subtract(years=1, weeks=2, days=-2)
        )
        assert (d - (years(1) + weeks(2) + hours(2))).exact_eq(
            d.subtract(years=1, weeks=2, hours=2)
        )

    def test_ambiguity(self):
        d = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            disambiguate="later",
            tz="Europe/Berlin",
        )
        assert d.add(days=0).exact_eq(d)
        assert d.add(days=7, weeks=-1).exact_eq(d)
        assert d.add(days=1).exact_eq(d.replace(day=30))
        assert d.add(days=6).exact_eq(d.replace(month=11, day=4))
        assert d.replace(disambiguate="earlier").add(hours=1).exact_eq(d)

        # transition to another fold
        assert d.add(years=1, days=-2, disambiguate="compatible").exact_eq(
            d.replace(year=2024, day=27, disambiguate="earlier")
        )
        # check operators too
        assert (d + years(1) - days(2)).exact_eq(d.add(years=1, days=-2))

        # transition to a gap
        assert d.add(months=5, days=2, disambiguate="compatible").exact_eq(
            d.replace(year=2024, month=3, day=31, disambiguate="later")
        )

        # transition over a gap
        assert d.add(
            months=5, days=2, hours=2, disambiguate="compatible"
        ).exact_eq(
            d.replace(year=2024, month=3, day=31, hour=5, disambiguate="raise")
        )
        assert d.add(
            months=5, days=2, hours=-1, disambiguate="compatible"
        ).exact_eq(
            d.replace(year=2024, month=3, day=31, disambiguate="earlier")
        )

        # same with subtraction
        assert d.subtract(days=0, disambiguate="raise").exact_eq(d)
        assert d.subtract(days=7, weeks=-1, disambiguate="raise").exact_eq(d)
        assert d.subtract(days=1, disambiguate="raise").exact_eq(
            d.replace(day=28, disambiguate="raise")
        )

    def test_out_of_bounds_min(self):
        d = ZonedDateTime(2000, 1, 1, tz="Europe/Amsterdam")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(years=-1999, disambiguate="compatible")

    def test_out_of_bounds_max(self):
        d = ZonedDateTime(2000, 12, 31, hour=23, tz="America/New_York")
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(years=7999, disambiguate="compatible")


class TestDifference:

    def test_simple(self):
        d = ZonedDateTime(
            2023, 10, 29, 5, tz="Europe/Amsterdam", disambiguate="earlier"
        )
        other = ZonedDateTime(
            2023, 10, 28, 3, nanosecond=4_000_000, tz="Europe/Amsterdam"
        )
        assert d - other == (hours(27) - milliseconds(4))
        assert other - d == (hours(-27) + milliseconds(4))

        # same with the method
        assert d.difference(other) == d - other
        assert other.difference(d) == other - d

    def test_amibiguous(self):
        d = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        other = ZonedDateTime(2023, 10, 28, 3, 15, tz="Europe/Amsterdam")
        assert d - other == hours(23)
        assert d.replace(disambiguate="later") - other == hours(24)
        assert other - d == hours(-23)
        assert other - d.replace(disambiguate="later") == hours(-24)

        # same with the method
        assert d.difference(other) == d - other

    def test_instant(self):
        d = ZonedDateTime(
            2023, 10, 29, 2, tz="Europe/Amsterdam", disambiguate="earlier"
        )
        other = Instant.from_utc(2023, 10, 28, 20)
        assert d - other == hours(4)
        assert d.replace(disambiguate="later") - other == hours(5)

        # same with the method
        assert d.difference(other) == d - other

    def test_offset(self):
        d = ZonedDateTime(
            2023, 10, 29, 2, tz="Europe/Amsterdam", disambiguate="earlier"
        )
        other = OffsetDateTime(2023, 10, 28, 20, offset=hours(1))
        assert d - other == hours(5)
        assert d.replace(disambiguate="later") - other == hours(6)

        # same with the method
        assert d.difference(other) == d - other

    @system_tz_nyc()
    def test_system_tz(self):
        d = ZonedDateTime(
            2023, 10, 29, 2, tz="Europe/Amsterdam", disambiguate="earlier"
        )
        other = SystemDateTime(2023, 10, 28, 19)
        assert d - other == hours(1)
        assert d.replace(disambiguate="later") - other == hours(2)

        # same with the method
        assert d.difference(other) == d - other


class TestRound:

    @pytest.mark.parametrize(
        "d, increment, unit, floor, ceil, half_floor, half_ceil, half_even",
        [
            (
                ZonedDateTime(
                    2023, 7, 14, 1, nanosecond=459_999_999, tz="Europe/Paris"
                ),
                1,
                "nanosecond",
                ZonedDateTime(
                    2023, 7, 14, 1, nanosecond=459_999_999, tz="Europe/Paris"
                ),
                ZonedDateTime(
                    2023, 7, 14, 1, nanosecond=459_999_999, tz="Europe/Paris"
                ),
                ZonedDateTime(
                    2023, 7, 14, 1, nanosecond=459_999_999, tz="Europe/Paris"
                ),
                ZonedDateTime(
                    2023, 7, 14, 1, nanosecond=459_999_999, tz="Europe/Paris"
                ),
                ZonedDateTime(
                    2023, 7, 14, 1, nanosecond=459_999_999, tz="Europe/Paris"
                ),
            ),
            (
                ZonedDateTime(
                    2023,
                    7,
                    14,
                    1,
                    2,
                    21,
                    nanosecond=459_999_999,
                    tz="Europe/Paris",
                ),
                4,
                "second",
                ZonedDateTime(2023, 7, 14, 1, 2, 20, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 1, 2, 24, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 1, 2, 20, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 1, 2, 20, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 1, 2, 20, tz="Europe/Paris"),
            ),
            (
                ZonedDateTime(
                    2023,
                    7,
                    14,
                    23,
                    52,
                    29,
                    nanosecond=999_999_999,
                    tz="Europe/Paris",
                ),
                10,
                "minute",
                ZonedDateTime(2023, 7, 14, 23, 50, 0, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 15, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 23, 50, 0, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 23, 50, 0, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 23, 50, 0, tz="Europe/Paris"),
            ),
            (
                ZonedDateTime(
                    2023,
                    7,
                    14,
                    11,
                    59,
                    29,
                    nanosecond=999_999_999,
                    tz="Europe/Paris",
                ),
                12,
                "hour",
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 12, 0, 0, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 12, 0, 0, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 12, 0, 0, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, 12, 0, 0, tz="Europe/Paris"),
            ),
            # normal, 24-hour day at midnight
            (
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                1,
                "day",
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
            ),
            # normal, 24-hour day
            (
                ZonedDateTime(2023, 7, 14, 12, tz="Europe/Paris"),
                1,
                "day",
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 15, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 15, tz="Europe/Paris"),
                ZonedDateTime(2023, 7, 14, tz="Europe/Paris"),
            ),
            # shorter day
            (
                ZonedDateTime(2023, 3, 26, 11, 30, tz="Europe/Paris"),
                1,
                "day",
                ZonedDateTime(2023, 3, 26, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 27, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 26, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 27, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 26, tz="Europe/Paris"),
            ),
            # shorter day (23 hours)
            (
                ZonedDateTime(2023, 3, 26, 11, 30, tz="Europe/Paris"),
                1,
                "day",
                ZonedDateTime(2023, 3, 26, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 27, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 26, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 27, tz="Europe/Paris"),
                ZonedDateTime(2023, 3, 26, tz="Europe/Paris"),
            ),
            # longer day (24.5 hours)
            (
                ZonedDateTime(2024, 4, 7, 12, 15, tz="Australia/Lord_Howe"),
                1,
                "day",
                ZonedDateTime(2024, 4, 7, tz="Australia/Lord_Howe"),
                ZonedDateTime(2024, 4, 8, tz="Australia/Lord_Howe"),
                ZonedDateTime(2024, 4, 7, tz="Australia/Lord_Howe"),
                ZonedDateTime(2024, 4, 8, tz="Australia/Lord_Howe"),
                ZonedDateTime(2024, 4, 7, tz="Australia/Lord_Howe"),
            ),
            # keeps the offset if possible
            (
                ZonedDateTime(
                    2023,
                    10,
                    29,
                    2,
                    15,
                    tz="Europe/Paris",
                    disambiguate="later",
                ),
                30,
                "minute",
                ZonedDateTime(
                    2023, 10, 29, 2, 0, tz="Europe/Paris", disambiguate="later"
                ),
                ZonedDateTime(
                    2023,
                    10,
                    29,
                    2,
                    30,
                    tz="Europe/Paris",
                    disambiguate="later",
                ),
                ZonedDateTime(
                    2023, 10, 29, 2, 0, tz="Europe/Paris", disambiguate="later"
                ),
                ZonedDateTime(
                    2023,
                    10,
                    29,
                    2,
                    30,
                    tz="Europe/Paris",
                    disambiguate="later",
                ),
                ZonedDateTime(
                    2023, 10, 29, 2, 0, tz="Europe/Paris", disambiguate="later"
                ),
            ),
        ],
    )
    def test_round(
        self,
        d: ZonedDateTime,
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
        d = ZonedDateTime(
            2023, 7, 14, 1, 2, 3, nanosecond=500_000_000, tz="Europe/Paris"
        )
        assert d.round() == ZonedDateTime(
            2023, 7, 14, 1, 2, 4, tz="Europe/Paris"
        )
        assert d.replace(second=8).round() == ZonedDateTime(
            2023, 7, 14, 1, 2, 8, tz="Europe/Paris"
        )

    def test_default_increment(self):
        d = ZonedDateTime(
            2023, 7, 14, 1, 2, 3, nanosecond=800_000, tz="Europe/Paris"
        )
        assert d.round("millisecond").exact_eq(
            ZonedDateTime(
                2023, 7, 14, 1, 2, 3, nanosecond=1_000_000, tz="Europe/Paris"
            )
        )

    def test_invalid_mode(self):
        d = ZonedDateTime(
            2023, 7, 14, 1, 2, 3, nanosecond=4_000, tz="Europe/Paris"
        )
        with pytest.raises(ValueError, match="mode.*foo"):
            d.round("second", mode="foo")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "unit, increment",
        [
            ("minute", 8),
            ("second", 14),
            ("millisecond", 15),
            ("day", 2),
            ("hour", 48),
            ("microsecond", 1500),
            ("second", -1),
            ("second", 0),
        ],
    )
    def test_invalid_increment(self, unit, increment):
        d = ZonedDateTime(
            2023, 7, 14, 1, 2, 3, nanosecond=4_000, tz="Europe/Paris"
        )
        with pytest.raises(ValueError, match="[Ii]ncrement"):
            d.round(unit, increment=increment)

    def test_invalid_unit(self):
        d = ZonedDateTime(
            2023, 7, 14, 1, 2, 3, nanosecond=4_000, tz="Europe/Paris"
        )
        with pytest.raises(ValueError, match="Invalid.*unit.*foo"):
            d.round("foo")  # type: ignore[arg-type]

    def test_out_of_range(self):
        d = ZonedDateTime(9999, 12, 31, 23, tz="Etc/UTC")

        with pytest.raises((ValueError, OverflowError), match="range"):
            d.round("hour", increment=4)

        with pytest.raises((ValueError, OverflowError), match="range"):
            d.round("day")


class TestPickle:
    def test_simple(self):
        d = ZonedDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654, tz="Europe/Amsterdam"
        )
        dumped = pickle.dumps(d)
        assert len(dumped) <= len(pickle.dumps(d.py_datetime()))
        assert pickle.loads(pickle.dumps(d)).exact_eq(d)

    def test_ambiguous(self):
        d1 = ZonedDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            tz="Europe/Amsterdam",
            disambiguate="earlier",
        )
        d2 = d1.replace(disambiguate="later")
        assert pickle.loads(pickle.dumps(d1)).exact_eq(d1)
        assert pickle.loads(pickle.dumps(d2)).exact_eq(d2)


def test_old_pickle_data_remains_unpicklable():
    # Don't update this value after 1.x release: the whole idea is that
    # it's a pickle at a specific version of the library,
    # and it should remain unpicklable even in later versions.
    dumped = (
        b"\x80\x04\x95F\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\x0c_unp"
        b"kl_zoned\x94\x93\x94C\x0f\xe4\x07\x08\x0f\x17\x0c\t\x06\x12\x0f\x00"
        b" \x1c\x00\x00\x94\x8c\x10Europe/Amsterdam\x94\x86\x94R\x94."
    )
    assert pickle.loads(dumped).exact_eq(
        ZonedDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654, tz="Europe/Amsterdam"
        )
    )


def test_copy():
    d = ZonedDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=987_654, tz="Europe/Amsterdam"
    )
    assert copy(d) is d
    assert deepcopy(d) is d


def test_cannot_subclass():
    with pytest.raises(TypeError):

        class Subclass(ZonedDateTime):  # type: ignore[misc]
            pass
