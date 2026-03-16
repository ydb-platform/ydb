import pickle
import re
from copy import copy, deepcopy
from datetime import datetime as py_datetime, timedelta, timezone, tzinfo
from typing import Any

import pytest
from hypothesis import given
from hypothesis.strategies import text

from whenever import (
    Date,
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
    microseconds,
    minutes,
    seconds,
    weeks,
    years,
)

from .common import (
    AlwaysEqual,
    AlwaysLarger,
    AlwaysSmaller,
    NeverEqual,
    ZoneInfo,
    system_tz,
    system_tz_ams,
    system_tz_nyc,
)
from .test_offset_datetime import INVALID_ISO_STRINGS


class TestInit:
    @system_tz_ams()
    def test_basic(self):
        d = SystemDateTime(2020, 8, 15, 5, 12, 30, nanosecond=450)

        assert d.year == 2020
        assert d.month == 8
        assert d.day == 15
        assert d.hour == 5
        assert d.minute == 12
        assert d.second == 30
        assert d.nanosecond == 450
        assert d.offset == hours(2)

    def test_optionality(self):
        assert SystemDateTime(2020, 8, 15, 12).exact_eq(
            SystemDateTime(
                2020, 8, 15, 12, 0, 0, nanosecond=0, disambiguate="raise"
            )
        )

    @system_tz_ams()
    def test_repeated(self):
        kwargs: dict[str, Any] = {
            "year": 2023,
            "month": 10,
            "day": 29,
            "hour": 2,
            "minute": 15,
        }
        assert SystemDateTime(**kwargs, disambiguate="compatible").exact_eq(
            SystemDateTime(**kwargs)
        )

        d = SystemDateTime(**kwargs, disambiguate="earlier")
        assert d < SystemDateTime(**kwargs, disambiguate="later")

        with pytest.raises(
            RepeatedTime,
            match="2023-10-29 02:15:00 is repeated in the system timezone",
        ):
            SystemDateTime(2023, 10, 29, 2, 15, disambiguate="raise")

    @system_tz_ams()
    def test_skipped(self):
        kwargs: dict[str, Any] = {
            "year": 2023,
            "month": 3,
            "day": 26,
            "hour": 2,
            "minute": 30,
        }

        assert SystemDateTime(**kwargs, disambiguate="compatible").exact_eq(
            SystemDateTime(**kwargs)
        )

        with pytest.raises(
            SkippedTime,
        ):
            SystemDateTime(**kwargs, disambiguate="raise")

        assert SystemDateTime(**kwargs, disambiguate="earlier").exact_eq(
            SystemDateTime(**{**kwargs, "hour": 1})
        )
        assert SystemDateTime(**kwargs, disambiguate="later").exact_eq(
            SystemDateTime(**{**kwargs, "hour": 3})
        )
        assert SystemDateTime(**kwargs, disambiguate="compatible").exact_eq(
            SystemDateTime(**{**kwargs, "hour": 3})
        )

    def test_invalid(self):
        with pytest.raises(ValueError, match="range|time"):
            SystemDateTime(2020, 1, 15, nanosecond=1_000_000_000)

        with pytest.raises(ValueError, match="disambiguate"):
            SystemDateTime(2020, 1, 15, disambiguate="foo")  # type: ignore[arg-type]

    @system_tz_ams()
    def test_bounds_min(self):
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            SystemDateTime(1, 1, 1)

    @system_tz_nyc()
    def test_bounds_max(self):
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            SystemDateTime(9999, 12, 31, 23)


class TestInstant:
    @system_tz_ams()
    def test_common_time(self):
        d = SystemDateTime(2020, 8, 15, 11)
        assert d.to_instant().exact_eq(Instant.from_utc(2020, 8, 15, 9))

    @system_tz_ams()
    def test_amibiguous_time(self):
        d = SystemDateTime(2023, 10, 29, 2, 15, disambiguate="earlier")
        assert d.to_instant().exact_eq(Instant.from_utc(2023, 10, 29, 0, 15))
        assert (
            d.replace(disambiguate="later")
            .to_instant()
            .exact_eq(Instant.from_utc(2023, 10, 29, 1, 15))
        )


def test_to_plain():
    d = SystemDateTime(2020, 8, 15, 12, 8, 30)
    assert d.to_plain() == PlainDateTime(2020, 8, 15, 12, 8, 30)

    with pytest.deprecated_call():
        assert d.local() == d.to_plain()  # type: ignore[attr-defined]


@system_tz_ams()
def test_to_tz():
    assert (
        SystemDateTime(2020, 8, 15, 12, 8, 30)
        .to_tz("America/New_York")
        .exact_eq(ZonedDateTime(2020, 8, 15, 6, 8, 30, tz="America/New_York"))
    )
    ams = SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="earlier")
    nyc = ZonedDateTime(2023, 10, 28, 20, 15, 30, tz="America/New_York")
    assert ams.to_tz("America/New_York").exact_eq(nyc)
    assert (
        ams.replace(disambiguate="later")
        .to_tz("America/New_York")
        .exact_eq(nyc.replace(hour=21, disambiguate="raise"))
    )
    assert nyc.to_system_tz().exact_eq(ams)
    assert (
        nyc.replace(hour=21, disambiguate="raise")
        .to_system_tz()
        .exact_eq(ams.replace(disambiguate="later"))
    )
    # disambiguation doesn't affect NYC time because there's no ambiguity
    assert nyc.replace(disambiguate="later").to_system_tz().exact_eq(ams)

    try:
        d_min = Instant.MIN.to_system_tz()
    except OverflowError as e:
        # For some platforms, this cannot be represented as a time_t.
        # we skip the test in this case.
        assert "time_t" in str(e)
        pytest.skip("time_t overflow")
    with pytest.raises((ValueError, OverflowError), match="range|year"):
        d_min.to_tz("America/New_York")

    with system_tz_nyc():
        d_max = Instant.MAX.to_system_tz()
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d_max.to_tz("Europe/Amsterdam")


class TestToFixedOffset:
    @system_tz_ams()
    def test_simple(self):
        assert (
            SystemDateTime(2020, 8, 15, 12, 8, 30)
            .to_fixed_offset()
            .exact_eq(OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=2))
        )

    @system_tz_ams()
    def test_repeated(self):
        assert (
            SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="earlier")
            .to_fixed_offset()
            .exact_eq(OffsetDateTime(2023, 10, 29, 2, 15, 30, offset=hours(2)))
        )
        assert (
            SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="later")
            .to_fixed_offset()
            .exact_eq(OffsetDateTime(2023, 10, 29, 2, 15, 30, offset=hours(1)))
        )

    @system_tz_ams()
    def test_custom_offset(self):
        d = SystemDateTime(2020, 8, 15, 12, 30)
        assert d.to_fixed_offset(hours(3)).exact_eq(
            OffsetDateTime(2020, 8, 15, 13, 30, offset=hours(3))
        )
        assert d.to_fixed_offset(hours(0)).exact_eq(
            OffsetDateTime(2020, 8, 15, 10, 30, offset=hours(0))
        )
        assert d.to_fixed_offset(-1).exact_eq(
            OffsetDateTime(2020, 8, 15, 9, 30, offset=hours(-1))
        )

    @system_tz_ams()
    def test_bounds(self):
        try:
            small_dt = Instant.MIN.to_system_tz()
        except OverflowError as e:
            # For some platforms, this cannot be represented as a time_t.
            # we skip the test in this case.
            assert "time_t" in str(e)
            pytest.skip("time_t overflow")

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            small_dt.to_fixed_offset(-23)

        with system_tz_nyc():
            big_dt = Instant.MAX.to_system_tz()
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                big_dt.to_fixed_offset(23)


class TestToSystemTz:

    @system_tz_ams()
    def test_no_timezone_change(self):
        d = SystemDateTime(2020, 8, 15, 12, 8, 30)
        assert d.to_system_tz().exact_eq(d)

    @system_tz_ams()
    def test_timezone_change(self):
        d = SystemDateTime(2020, 8, 15, 12, 8, 30)
        with system_tz_nyc():
            assert d.to_system_tz().exact_eq(
                SystemDateTime(2020, 8, 15, 6, 8, 30)
            )

    @system_tz_nyc()
    def test_bounds(self):
        try:
            d = Instant.MAX.to_system_tz()
        except OverflowError as e:
            # For some platforms, this cannot be represented as a time_t.
            # we skip the test in this case.
            assert "time_t" in str(e)
            pytest.skip("time_t overflow")

        with system_tz_ams():
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                d.to_system_tz()

            d2 = Instant.MIN.to_system_tz()

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d2.to_system_tz()


@system_tz_ams()
def test_immutable():
    d = SystemDateTime(2020, 8, 15)
    with pytest.raises(AttributeError):
        d.year = 2021  # type: ignore[misc]


class TestFormatCommonIso:
    @system_tz_ams()
    def test_simple(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_300)
        expected = "2020-08-15T23:12:09.9876543+02:00"
        assert str(d) == expected
        assert d.format_common_iso() == expected

    @system_tz_ams()
    def test_repeated(self):
        d = SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="earlier")
        expected = "2023-10-29T02:15:30+02:00"
        assert str(d) == expected
        assert d.format_common_iso() == expected
        d2 = d.replace(disambiguate="later")
        assert str(d2) == expected.replace("+02:00", "+01:00")
        assert d2.format_common_iso() == expected.replace("+02:00", "+01:00")


class TestEquality:
    def test_same_exact(self):
        d = SystemDateTime(2020, 8, 15, 12, 8, 30, nanosecond=450)
        same = SystemDateTime(2020, 8, 15, 12, 8, 30, nanosecond=450)
        assert d == same
        assert not d != same
        assert hash(d) == hash(same)

    @system_tz_ams()
    def test_same_moment(self):
        d = SystemDateTime(2020, 8, 15, 12, 8, 30, nanosecond=450)
        with system_tz_nyc():
            same = SystemDateTime(2020, 8, 15, 6, 8, 30, nanosecond=450)
        assert d == same
        assert not d != same
        assert hash(d) == hash(same)

    @system_tz_ams()
    def test_amibiguous(self):
        d = SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="earlier")
        other = SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="later")
        assert d != other
        assert not d == other
        assert hash(d) != hash(other)

    @system_tz_ams()
    def test_instant(self):
        d: SystemDateTime | Instant = SystemDateTime(
            2023, 10, 29, 2, 15, disambiguate="earlier"
        )
        same = Instant.from_utc(2023, 10, 29, 0, 15)
        different = Instant.from_utc(2023, 10, 29, 1, 15)
        assert d == same
        assert not d != same
        assert d != different
        assert not d == different

        assert hash(d) == hash(same)
        assert hash(d) != hash(different)

    @system_tz_ams()
    def test_offset(self):
        d: SystemDateTime | OffsetDateTime = SystemDateTime(
            2023, 10, 29, 2, 15, disambiguate="earlier"
        )
        same = d.to_fixed_offset(hours(5))
        different = d.to_fixed_offset(hours(3)).replace(
            minute=14, ignore_dst=True
        )
        assert d == same
        assert not d != same
        assert d != different
        assert not d == different

        assert hash(d) == hash(same)
        assert hash(d) != hash(different)

    @system_tz_ams()
    def test_zoned(self):
        d: SystemDateTime | ZonedDateTime = SystemDateTime(
            2023, 10, 29, 2, 15, disambiguate="earlier"
        )
        same = d.to_tz("Europe/Paris")
        assert same.is_ambiguous()  # important we test this case
        different = d.to_tz("Europe/Amsterdam").replace(
            minute=14, disambiguate="earlier"
        )
        assert d == same
        assert not d != same
        assert d != different
        assert not d == different

        assert hash(d) == hash(same)
        assert hash(d) != hash(different)

    def test_notimplemented(self):
        d = SystemDateTime(2020, 8, 15)
        assert d == AlwaysEqual()
        assert not d != AlwaysEqual()
        assert d != NeverEqual()
        assert not d == NeverEqual()

        assert not d == 3  # type: ignore[comparison-overlap]
        assert d != 3  # type: ignore[comparison-overlap]
        assert not 3 == d  # type: ignore[comparison-overlap]
        assert 3 != d  # type: ignore[comparison-overlap]

        assert not d == None  # noqa: E711
        assert d != None  # noqa: E711
        assert not None == d  # noqa: E711
        assert None != d  # noqa: E711


class TestComparison:
    @system_tz_nyc()
    def test_different_timezones(self):
        d = SystemDateTime(2020, 8, 15, 12, 30)
        later = d + hours(1)
        earlier = d - hours(1)
        assert d < later
        assert d <= later
        assert later > d
        assert later >= d
        assert not d > later
        assert not d >= later
        assert not later < d
        assert not later <= d

        assert d > earlier
        assert d >= earlier
        assert earlier < d
        assert earlier <= d
        assert not d < earlier
        assert not d <= earlier
        assert not earlier > d
        assert not earlier >= d

    @system_tz_ams()
    def test_same_timezone_fold(self):
        d = SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="earlier")
        later = d.replace(disambiguate="later")
        assert d < later
        assert d <= later
        assert later > d
        assert later >= d
        assert not d > later
        assert not d >= later
        assert not later < d
        assert not later <= d

    @system_tz_ams()
    def test_offset(self):
        d = SystemDateTime(2020, 8, 15, 12, 30)
        same = d.to_fixed_offset(hours(5))
        later = same.replace(minute=31, ignore_dst=True)
        earlier = same.replace(minute=29, ignore_dst=True)
        assert d >= same
        assert d <= same
        assert not d > same
        assert not d < same

        assert d < later
        assert d <= later
        assert not d > later
        assert not d >= later

        assert d > earlier
        assert d >= earlier
        assert not d < earlier
        assert not d <= earlier

    @system_tz_ams()
    def test_zoned(self):
        d = SystemDateTime(2020, 8, 15, 12, 30)
        same = d.to_tz("America/New_York")
        later = same + minutes(1)
        earlier = same - minutes(1)
        assert d >= same
        assert d <= same
        assert not d > same
        assert not d < same

        assert d < later
        assert d <= later
        assert not d > later
        assert not d >= later

        assert d > earlier
        assert d >= earlier
        assert not d < earlier
        assert not d <= earlier

    @system_tz_ams()
    def test_utc(self):
        d = SystemDateTime(2020, 8, 15, 12, 30)
        same = d.to_instant()
        later = same + minutes(1)
        earlier = same - minutes(1)
        assert d >= same
        assert d <= same
        assert not d > same
        assert not d < same

        assert d < later
        assert d <= later
        assert not d > later
        assert not d >= later

        assert d > earlier
        assert d >= earlier
        assert not d < earlier
        assert not d <= earlier

    def test_notimplemented(self):
        d = SystemDateTime(2020, 8, 15)
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


@system_tz_ams()
def test_exact_equality():
    a = SystemDateTime(2020, 8, 15, 12, 8, 30, nanosecond=450)
    same = a.replace(disambiguate="raise")
    with system_tz_nyc():
        same_moment = SystemDateTime(2020, 8, 15, 6, 8, 30, nanosecond=450)
    assert same.to_instant() == same_moment.to_instant()
    different = a.replace(hour=13, disambiguate="raise")

    assert a.exact_eq(same)
    assert same.exact_eq(a)
    assert not a.exact_eq(same_moment)
    assert not same_moment.exact_eq(a)
    assert not a.exact_eq(different)
    assert not different.exact_eq(a)

    with pytest.raises(TypeError):
        a.exact_eq(42)  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        a.exact_eq(a.to_instant())  # type: ignore[arg-type]


class TestParseCommonIso:
    @pytest.mark.parametrize(
        "s, expect, offset",
        [
            (
                "2020-08-15T12:08:30+05:00",
                PlainDateTime(2020, 8, 15, 12, 8, 30),
                hours(5),
            ),
            (
                "2020-08-15T12:08:30+20:00",
                PlainDateTime(2020, 8, 15, 12, 8, 30),
                hours(20),
            ),
            (
                "2020-08-15T12:08:30.0034+05:00",
                PlainDateTime(2020, 8, 15, 12, 8, 30, nanosecond=3_400_000),
                hours(5),
            ),
            (
                "2020-08-15T12:08:30.000000010+05:00",
                PlainDateTime(2020, 8, 15, 12, 8, 30, nanosecond=10),
                hours(5),
            ),
            (
                "2020-08-15T12:08:30.0034-05:00:01",
                PlainDateTime(
                    2020,
                    8,
                    15,
                    12,
                    8,
                    30,
                    nanosecond=3_400_000,
                ),
                -hours(5) - seconds(1),
            ),
            (
                "2020-08-15T12:08:30+00:00",
                PlainDateTime(2020, 8, 15, 12, 8, 30),
                hours(0),
            ),
            (
                "2020-08-15T12:08:30-00:00",
                PlainDateTime(2020, 8, 15, 12, 8, 30),
                hours(0),
            ),
            (
                "2020-08-15T12:08:30Z",
                PlainDateTime(2020, 8, 15, 12, 8, 30),
                hours(0),
            ),
        ],
    )
    def test_valid(self, s, expect, offset):
        dt = SystemDateTime.parse_common_iso(s)
        assert dt.to_plain() == expect
        assert dt.offset == offset

    @pytest.mark.parametrize("s", INVALID_ISO_STRINGS)
    def test_invalid(self, s):
        with pytest.raises(ValueError, match="format.*" + re.escape(repr(s))):
            SystemDateTime.parse_common_iso(s)

    @pytest.mark.parametrize(
        "s",
        [
            "0001-01-01T02:08:30+05:00",
            "9999-12-31T22:08:30-05:00",
        ],
    )
    def test_bounds(self, s):
        with pytest.raises(ValueError):
            SystemDateTime.parse_common_iso(s)

    @given(text())
    def test_fuzzing(self, s: str):
        with pytest.raises(
            ValueError,
            match=r"format.*" + re.escape(repr(s)),
        ):
            SystemDateTime.parse_common_iso(s)


class TestTimestamp:

    @system_tz_nyc()
    def test_default_seconds(self):
        assert SystemDateTime(1969, 12, 31, 19).timestamp() == 0
        assert (
            SystemDateTime(
                2020, 8, 15, 8, 8, 30, nanosecond=999_999_999
            ).timestamp()
            == 1_597_493_310
        )

        ambiguous = SystemDateTime(
            2023, 11, 5, 1, 15, 30, disambiguate="earlier"
        )
        assert (
            ambiguous.timestamp()
            != ambiguous.replace(disambiguate="later").timestamp()
        )

    @system_tz_nyc()
    def test_millis(self):
        assert SystemDateTime(1969, 12, 31, 19).timestamp_millis() == 0
        assert (
            SystemDateTime(
                2020, 8, 15, 8, 8, 30, nanosecond=45_999_999
            ).timestamp_millis()
            == 1_597_493_310_045
        )

        ambiguous = SystemDateTime(
            2023, 11, 5, 1, 15, 30, disambiguate="earlier"
        )
        assert (
            ambiguous.timestamp()
            != ambiguous.replace(disambiguate="later").timestamp_millis()
        )

    @system_tz_nyc()
    def test_nanos(self):
        assert SystemDateTime(1969, 12, 31, 19).timestamp_nanos() == 0
        assert (
            SystemDateTime(
                2020, 8, 15, 8, 8, 30, nanosecond=450
            ).timestamp_nanos()
            == 1_597_493_310_000_000_450
        )

        ambiguous = SystemDateTime(
            2023, 11, 5, 1, 15, 30, disambiguate="earlier"
        )
        assert (
            ambiguous.timestamp()
            != ambiguous.replace(disambiguate="later").timestamp_nanos()
        )


class TestFromTimestamp:

    @pytest.mark.parametrize(
        "method, factor",
        [
            (SystemDateTime.from_timestamp, 1),
            (SystemDateTime.from_timestamp_millis, 1_000),
            (SystemDateTime.from_timestamp_nanos, 1_000_000_000),
        ],
    )
    @system_tz_ams()
    def test_all(self, method, factor):
        assert method(0).exact_eq(SystemDateTime(1970, 1, 1, 1))
        assert method(1_597_493_310 * factor).exact_eq(
            SystemDateTime(2020, 8, 15, 14, 8, 30)
        )
        with pytest.raises((OSError, OverflowError, ValueError)):
            method(1_000_000_000_000_000_000 * factor)

        with pytest.raises((OSError, OverflowError, ValueError)):
            method(-1_000_000_000_000_000_000 * factor)

        with pytest.raises(TypeError):
            method()

        with pytest.raises(TypeError):
            method("0")

        assert SystemDateTime.from_timestamp_millis(
            -4,
        ).to_instant() == Instant.from_timestamp(0).subtract(milliseconds=4)

        assert SystemDateTime.from_timestamp_nanos(
            -4,
        ).to_instant() == Instant.from_timestamp(0).subtract(nanoseconds=4)

    @system_tz_ams()
    def test_nanos(self):
        assert SystemDateTime.from_timestamp_nanos(
            1_597_493_310_123_456_789
        ).exact_eq(
            SystemDateTime(2020, 8, 15, 14, 8, 30, nanosecond=123_456_789)
        )

    @system_tz_ams()
    def test_millis(self):
        assert SystemDateTime.from_timestamp_millis(
            1_597_493_310_123
        ).exact_eq(
            SystemDateTime(2020, 8, 15, 14, 8, 30, nanosecond=123_000_000)
        )

    @system_tz_ams()
    def test_float(self):
        assert SystemDateTime.from_timestamp(
            1.0,
        ).exact_eq(
            SystemDateTime.from_timestamp(
                1,
            )
        )

        assert SystemDateTime.from_timestamp(
            1.000_000_001,
        ).exact_eq(
            SystemDateTime.from_timestamp(
                1,
            ).add(
                nanoseconds=1,
            )
        )

        assert SystemDateTime.from_timestamp(
            -9.000_000_100,
        ).exact_eq(
            SystemDateTime.from_timestamp(
                -9,
            ).subtract(
                nanoseconds=100,
            )
        )

        with pytest.raises((ValueError, OverflowError)):
            SystemDateTime.from_timestamp(9e200)

        with pytest.raises((ValueError, OverflowError)):
            SystemDateTime.from_timestamp(
                float(Instant.MAX.timestamp()) + 0.99999999,
            )

        with pytest.raises((ValueError, OverflowError)):
            SystemDateTime.from_timestamp(float("inf"))

        with pytest.raises((ValueError, OverflowError)):
            SystemDateTime.from_timestamp(float("nan"))


@system_tz_nyc()
def test_repr():
    d = SystemDateTime(2023, 3, 26, 2, 15)
    assert repr(d) == "SystemDateTime(2023-03-26 02:15:00-04:00)"


@system_tz_nyc()
def test_py_datetime():
    d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_999)
    py = d.py_datetime()
    assert py == py_datetime(2020, 8, 15, 23, 12, 9, 987_654).astimezone(None)


class TestFromPyDateTime:
    @system_tz_ams()
    def test_basic(self):
        d = py_datetime(
            2020, 8, 15, 23, tzinfo=timezone(hours(2).py_timedelta())
        )
        assert SystemDateTime.from_py_datetime(d).exact_eq(
            SystemDateTime(2020, 8, 15, 23)
        )

    @system_tz_ams()
    def test_disambiguated(self):
        d = py_datetime(
            2023, 10, 29, 2, 15, 30, tzinfo=timezone(hours(1).py_timedelta())
        )
        assert SystemDateTime.from_py_datetime(d).exact_eq(
            SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="later")
        )

    @system_tz_ams()
    def test_zoneinfo(self):
        assert SystemDateTime.from_py_datetime(
            py_datetime(2020, 8, 15, 23, tzinfo=ZoneInfo("Europe/Paris"))
        ).exact_eq(SystemDateTime(2020, 8, 15, 23))

    def test_bounds(self):
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            SystemDateTime.from_py_datetime(
                py_datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=2)))
            )

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            SystemDateTime.from_py_datetime(
                py_datetime(
                    9999, 12, 31, hour=23, tzinfo=timezone(timedelta(hours=-2))
                )
            )

    def test_subsecond_offset(self):
        with pytest.raises(ValueError, match="Sub-second"):
            SystemDateTime.from_py_datetime(
                py_datetime(
                    2020,
                    8,
                    15,
                    23,
                    12,
                    9,
                    987_654,
                    tzinfo=timezone(timedelta(hours=2, microseconds=30)),
                )
            )

    def test_utcoffset_none(self):

        class MyTz(tzinfo):
            def utcoffset(self, _):
                return None

        with pytest.raises(ValueError, match="naive"):
            SystemDateTime.from_py_datetime(
                py_datetime(2020, 8, 15, tzinfo=MyTz())  # type: ignore[abstract]
            )

    @system_tz_ams()
    def test_naive(self):
        with pytest.raises(ValueError, match="naive"):
            SystemDateTime.from_py_datetime(py_datetime(2020, 8, 15, 12))

    @system_tz_ams()
    def test_subclass(self):
        class MyDatetime(py_datetime):
            pass

        d = MyDatetime(
            2020, 8, 15, 23, 12, 9, 987_654, tzinfo=ZoneInfo("Europe/Paris")
        )
        assert SystemDateTime.from_py_datetime(d).exact_eq(
            SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_000)
        )


@system_tz_nyc()
def test_now():
    now = SystemDateTime.now()
    assert now.offset in (hours(-4), hours(-5))
    py_now = py_datetime.now(ZoneInfo("America/New_York"))
    assert py_now - now.py_datetime() < timedelta(seconds=1)


class TestReplace:
    @system_tz_ams()
    def test_basics(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_321)
        assert d.replace(year=2021).exact_eq(
            SystemDateTime(2021, 8, 15, 23, 12, 9, nanosecond=987_654_321)
        )
        assert d.replace(month=9).exact_eq(
            SystemDateTime(2020, 9, 15, 23, 12, 9, nanosecond=987_654_321)
        )
        assert d.replace(day=16).exact_eq(
            SystemDateTime(2020, 8, 16, 23, 12, 9, nanosecond=987_654_321)
        )
        assert d.replace(hour=1).exact_eq(
            SystemDateTime(2020, 8, 15, 1, 12, 9, nanosecond=987_654_321)
        )
        assert d.replace(minute=1).exact_eq(
            SystemDateTime(2020, 8, 15, 23, 1, 9, nanosecond=987_654_321)
        )
        assert d.replace(second=1).exact_eq(
            SystemDateTime(2020, 8, 15, 23, 12, 1, nanosecond=987_654_321)
        )
        assert d.replace(nanosecond=1).exact_eq(
            SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=1)
        )

    def test_invalid(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9)
        with pytest.raises(TypeError, match="tzinfo"):
            d.replace(tzinfo=timezone.utc, disambiguate="compatible")  # type: ignore[call-arg]
        with pytest.raises(TypeError, match="fold"):
            d.replace(fold=1, disambiguate="compatible")  # type: ignore[call-arg]
        with pytest.raises(TypeError, match="foo"):
            d.replace(foo=1, disambiguate="compatible")  # type: ignore[call-arg]

    @system_tz_ams()
    def test_repeated(self):
        d = SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="earlier")
        d_later = SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="later")
        with pytest.raises(
            RepeatedTime,
            match="2023-10-29 02:15:30 is repeated in the system timezone",
        ):
            d.replace(disambiguate="raise")

        assert d.replace(disambiguate="later").exact_eq(
            SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="later")
        )
        assert d.replace(disambiguate="earlier").exact_eq(d)
        assert d.replace().exact_eq(d)
        assert d_later.replace().exact_eq(d_later)

        # very rare case where offset cannot be reused
        with system_tz_nyc():
            assert d.replace(month=11, day=5).exact_eq(
                SystemDateTime(
                    2023, 11, 5, 2, 15, 30, disambiguate="compatible"
                )
            )
            assert d_later.replace(month=11, day=5).exact_eq(
                SystemDateTime(
                    2023, 11, 5, 2, 15, 30, disambiguate="compatible"
                )
            )

    @system_tz_ams()
    def test_skipped(self):
        d = SystemDateTime(2023, 3, 26, 1, 15, 30)
        d_later = SystemDateTime(2023, 3, 26, 3, 15, 30)
        with pytest.raises(
            SkippedTime,
            match="2023-03-26 02:15:30 is skipped in the system timezone",
        ):
            d.replace(hour=2, disambiguate="raise")

        assert d.replace(hour=2).exact_eq(
            SystemDateTime(2023, 3, 26, 2, 15, 30, disambiguate="earlier")
        )
        assert d_later.replace(hour=2).exact_eq(
            SystemDateTime(2023, 3, 26, 2, 15, 30, disambiguate="later")
        )

        # very rare case where offset cannot be reused
        with system_tz_nyc():
            assert d.replace(day=12, hour=2).exact_eq(
                SystemDateTime(2023, 3, 12, 3, 15, 30)
            )
            assert d_later.replace(day=12, hour=2).exact_eq(
                SystemDateTime(2023, 3, 12, 3, 15, 30)
            )

    @system_tz_ams()
    def test_bounds_min(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9)
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.replace(year=1, month=1, day=1, disambiguate="compatible")

    @system_tz_nyc()
    def test_bounds_max(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9)
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.replace(
                year=9999, month=12, day=31, hour=23, disambiguate="compatible"
            )


class TestShiftTimeUnits:
    @system_tz_ams()
    def test_zero(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_321)
        assert (d + hours(0)).exact_eq(d)

        # the equivalent with the method
        assert d.add(hours=0).exact_eq(d)

        # equivalent with subtraction
        assert (d - hours(0)).exact_eq(d)
        assert d.subtract(hours=0).exact_eq(d)

    @system_tz_ams()
    def test_ambiguous_plus_zero(self):
        d = SystemDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            disambiguate="earlier",
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
        d = SystemDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            disambiguate="earlier",
        )
        assert (d + hours(24)).exact_eq(
            SystemDateTime(2023, 10, 30, 1, 15, 30)
        )
        assert (d.replace(disambiguate="later") + hours(24)).exact_eq(
            SystemDateTime(2023, 10, 30, 2, 15, 30)
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
            SystemDateTime(2023, 10, 30, 1, 15, 30)
        )
        assert d.subtract(hours=-24).exact_eq(d + hours(24))
        assert (
            d.replace(disambiguate="later")
            .subtract(hours=-24)
            .exact_eq(d.replace(disambiguate="later") + hours(24))
        )

    @system_tz_ams()
    def test_out_of_range(self):
        d = SystemDateTime(2020, 8, 15)

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d + hours(24 * 366 * 8_000)

        # the equivalent with the method
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(hours=24 * 366 * 8_000)

    @system_tz_ams()
    def test_not_implemented(self):
        d = SystemDateTime(2020, 8, 15)
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

        with pytest.raises(TypeError):
            d.add(hours(4), seconds=3)  # type: ignore[call-overload]


class TestShiftDateUnits:

    @system_tz_ams()
    def test_zero(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_321)
        assert d.add(days=0, disambiguate="raise").exact_eq(d)
        assert d.add(days=0).exact_eq(d)
        assert d.add(weeks=0).exact_eq(d)
        assert d.add(months=0).exact_eq(d)
        assert d.add(years=0, weeks=0).exact_eq(d)
        assert d.add().exact_eq(d)

        # same with operators
        assert d + days(0) == d
        assert d + weeks(0) == d
        assert d + years(0) == d

        # same with subtraction
        assert d.subtract(days=0).exact_eq(d)

        assert d - days(0) == d
        assert d - weeks(0) == d
        assert d - years(0) == d

    @system_tz_ams()
    def test_simple_date(self):
        d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_321)
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
        assert d + (years(1) + weeks(2) + days(-2)) == d.add(
            years=1, weeks=2, days=-2
        )
        assert d + (years(1) + weeks(2) + hours(2)) == d.add(
            years=1, weeks=2, hours=2
        )
        assert d - (years(1) + weeks(2) + days(-2)) == d.subtract(
            years=1, weeks=2, days=-2
        )
        assert d - (years(1) + weeks(2) + hours(2)) == d.subtract(
            years=1, weeks=2, hours=2
        )

    @system_tz_ams()
    def test_ambiguity(self):
        d = SystemDateTime(
            2023,
            10,
            29,
            2,
            15,
            30,
            disambiguate="later",
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
        assert d + years(1) - days(2) == d.add(years=1, days=-2)

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
        assert d.subtract(days=0).exact_eq(d)
        assert d.subtract(days=7, weeks=-1).exact_eq(d)
        assert d.subtract(days=1).exact_eq(d.replace(day=28))

    @system_tz_ams()
    def test_out_of_bounds_min(self):
        d = SystemDateTime(2000, 1, 1)
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(years=-1999, disambiguate="compatible")

    @system_tz_nyc()
    def test_out_of_bounds_max(self):
        d = SystemDateTime(2000, 12, 31, hour=23)
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(years=7999, disambiguate="compatible")


class TestDifference:

    @system_tz_ams()
    def test_subtract_datetime(self):
        d = SystemDateTime(2023, 10, 29, 5)
        other = SystemDateTime(2023, 10, 28, 3, nanosecond=2_000)
        assert d - other == (hours(27) - microseconds(2))
        assert other - d == -(hours(27) - microseconds(2))

        # same with method
        assert d.difference(other) == d - other
        assert other.difference(d) == other - d

    @system_tz_ams()
    def test_subtract_amibiguous_datetime(self):
        d = SystemDateTime(2023, 10, 29, 2, 15, disambiguate="earlier")
        other = SystemDateTime(2023, 10, 28, 3, 15)
        assert d - other == hours(23)
        assert d.replace(disambiguate="later") - other == hours(24)
        assert other - d == hours(-23)
        assert other - d.replace(disambiguate="later") == hours(-24)

        # same with method
        assert d.difference(other) == d - other
        assert other.difference(d) == other - d

    @system_tz_ams()
    def test_instant(self):
        d = SystemDateTime(2023, 10, 29, 2, disambiguate="earlier")
        other = Instant.from_utc(2023, 10, 28, 20)
        assert d - other == hours(4)
        assert d.replace(disambiguate="later") - Instant.from_utc(
            2023, 10, 28, 20
        ) == hours(5)

        # same with method
        assert d.difference(other) == d - other

    @system_tz_ams()
    def test_offset(self):
        d = SystemDateTime(2023, 10, 29, 2, disambiguate="earlier")
        other = OffsetDateTime(2023, 10, 28, 22, offset=hours(1))
        assert d - other == hours(3)
        assert d.replace(disambiguate="later") - other == hours(4)

        # same with method
        assert d.difference(other) == d - other

    @system_tz_ams()
    def test_zoned(self):
        d = SystemDateTime(2023, 10, 29, 2, disambiguate="earlier")
        other = ZonedDateTime(2023, 10, 28, 17, tz="America/New_York")
        assert d - other == hours(3)
        assert d.replace(disambiguate="later") - other == hours(4)

        # same with method
        assert d.difference(other) == d - other


class TestReplaceDate:
    @system_tz_ams()
    def test_unambiguous(self):
        d = SystemDateTime(2020, 8, 15, 14)
        assert d.replace_date(Date(2021, 1, 2)) == SystemDateTime(
            2021, 1, 2, 14
        )
        assert d.replace_date(
            Date(2021, 1, 2), disambiguate="raise"
        ) == SystemDateTime(2021, 1, 2, 14)

    @system_tz_ams()
    def test_repeated(self):
        d = SystemDateTime(2020, 1, 1, 2, 15, 30)
        date = Date(2023, 10, 29)
        with pytest.raises(RepeatedTime):
            assert d.replace_date(date, disambiguate="raise")

        assert d.replace_date(date, disambiguate="earlier") == d.replace(
            year=2023, month=10, day=29, disambiguate="earlier"
        )
        assert d.replace_date(date, disambiguate="later") == d.replace(
            year=2023, month=10, day=29, disambiguate="later"
        )
        assert d.replace_date(date, disambiguate="compatible") == d.replace(
            year=2023, month=10, day=29, disambiguate="compatible"
        )
        assert d.replace_date(date).exact_eq(
            d.replace_date(date, disambiguate="later")
        )

    @system_tz_ams()
    def test_skipped(self):
        d = SystemDateTime(2020, 1, 1, 2, 15, 30)
        date = Date(2023, 3, 26)
        with pytest.raises(SkippedTime):
            assert d.replace_date(date, disambiguate="raise")

        assert d.replace_date(date, disambiguate="earlier") == d.replace(
            year=2023, month=3, day=26, disambiguate="earlier"
        )
        assert d.replace_date(date, disambiguate="later") == d.replace(
            year=2023, month=3, day=26, disambiguate="later"
        )
        assert d.replace_date(date, disambiguate="compatible") == d.replace(
            year=2023, month=3, day=26, disambiguate="compatible"
        )
        assert d.replace_date(date).exact_eq(
            d.replace_date(date, disambiguate="earlier")
        )

    def test_invalid(self):
        d = SystemDateTime(2020, 8, 15, 14)
        with pytest.raises((TypeError, AttributeError)):
            d.replace_date(object(), disambiguate="raise")  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="disambiguate"):
            d.replace_date(Date(2020, 8, 15), disambiguate="foo")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="got 2|foo"):
            d.replace_date(Date(2020, 8, 15), disambiguate="raise", foo=4)  # type: ignore[call-arg]

        with pytest.raises(TypeError, match="foo"):
            d.replace_date(Date(2020, 8, 15), foo="raise")  # type: ignore[call-arg]

    def test_out_of_range_due_to_offset(self):
        with system_tz_ams():
            d = SystemDateTime(2020, 1, 1)
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                d.replace_date(Date(1, 1, 1), disambiguate="compatible")

        with system_tz_nyc():
            d2 = SystemDateTime(2020, 1, 1, hour=23)
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                d2.replace_date(Date(9999, 12, 31), disambiguate="compatible")


class TestReplaceTime:
    @system_tz_ams()
    def test_unambiguous(self):
        d = SystemDateTime(2020, 8, 15, 14)
        assert d.replace_time(
            Time(1, 2, 3, nanosecond=4_000), disambiguate="raise"
        ).exact_eq(SystemDateTime(2020, 8, 15, 1, 2, 3, nanosecond=4_000))
        assert d.replace_time(Time(1, 2, 3, nanosecond=4_000)).exact_eq(
            SystemDateTime(2020, 8, 15, 1, 2, 3, nanosecond=4_000)
        )

    @system_tz_ams()
    def test_repeated_time(self):
        d = SystemDateTime(2023, 10, 29, 0, 15, 30)
        time = Time(2, 15, 30)
        with pytest.raises(RepeatedTime):
            assert d.replace_time(time, disambiguate="raise")

        assert d.replace_time(time, disambiguate="earlier").exact_eq(
            d.replace(hour=2, minute=15, second=30, disambiguate="earlier")
        )
        assert d.replace_time(time, disambiguate="later").exact_eq(
            d.replace(hour=2, minute=15, second=30, disambiguate="later")
        )
        assert d.replace_time(time, disambiguate="compatible").exact_eq(
            d.replace(hour=2, minute=15, second=30, disambiguate="compatible")
        )
        # default behavior
        assert d.replace_time(time).exact_eq(
            d.replace(hour=2, minute=15, second=30, disambiguate="earlier")
        )
        # For small moves within a fold, keeps the same offset
        SystemDateTime(2023, 10, 29, 2, 30, disambiguate="later").replace_time(
            time
        ).exact_eq(
            SystemDateTime(2023, 10, 29, 2, 15, 30, disambiguate="later")
        )

    @system_tz_ams()
    def test_skipped_time(self):
        d = SystemDateTime(2023, 3, 26, 8, 15)
        time = Time(2, 15)
        with pytest.raises(SkippedTime):
            assert d.replace_time(time, disambiguate="raise")

        assert d.replace_time(time, disambiguate="earlier").exact_eq(
            d.replace(hour=2, minute=15, disambiguate="earlier")
        )
        assert d.replace_time(time, disambiguate="later").exact_eq(
            d.replace(hour=2, minute=15, disambiguate="later")
        )
        assert d.replace_time(time, disambiguate="compatible").exact_eq(
            d.replace(hour=2, minute=15, disambiguate="compatible")
        )
        # default behavior
        assert d.replace_time(time).exact_eq(
            d.replace(hour=2, minute=15, disambiguate="later")
        )

    def test_invalid(self):
        d = SystemDateTime(2020, 8, 15, 14)
        with pytest.raises((TypeError, AttributeError)):
            d.replace_time(object(), disambiguate="raise")  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="disambiguate"):
            d.replace_time(Time(1, 2, 3), disambiguate="foo")  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="got 2|foo"):
            d.replace_time(Time(1, 2, 3), disambiguate="raise", foo=4)  # type: ignore[call-arg]

        with pytest.raises(TypeError, match="foo"):
            d.replace_time(Time(1, 2, 3), foo="raise")  # type: ignore[call-arg]

    def test_out_of_range_due_to_offset(self):
        with system_tz_ams():
            try:
                d = Instant.MIN.to_system_tz()
            except OverflowError as e:
                # For some platforms, this cannot be represented as a time_t.
                # we skip the test in this case.
                assert "time_t" in str(e)
                pytest.skip("time_t overflow")

            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                d.replace_time(Time(0), disambiguate="compatible")

        with system_tz_nyc():
            d2 = Instant.MAX.to_system_tz()
            with pytest.raises(
                (ValueError, OverflowError), match="range|year"
            ):
                d2.replace_time(Time(23), disambiguate="compatible")


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
        with system_tz(d.tz):
            d_sys = d.to_system_tz()
            assert (
                d_sys.round(unit, increment=increment)
                == half_even.to_system_tz()
            )
            assert (
                d_sys.round(unit, increment=increment, mode="floor")
                == floor.to_system_tz()
            )
            assert (
                d_sys.round(unit, increment=increment, mode="ceil")
                == ceil.to_system_tz()
            )
            assert (
                d_sys.round(unit, increment=increment, mode="half_floor")
                == half_floor.to_system_tz()
            )
            assert (
                d_sys.round(unit, increment=increment, mode="half_ceil")
                == half_ceil.to_system_tz()
            )
            assert (
                d_sys.round(unit, increment=increment, mode="half_even")
                == half_even.to_system_tz()
            )

    @system_tz_ams()
    def test_default(self):
        d = SystemDateTime(2023, 7, 14, 1, 2, 3, nanosecond=500_000_000)
        assert d.round() == SystemDateTime(2023, 7, 14, 1, 2, 4)
        assert d.replace(second=8).round() == SystemDateTime(
            2023, 7, 14, 1, 2, 8
        )

    @system_tz_ams()
    def test_invalid_mode(self):
        d = SystemDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000)
        with pytest.raises(ValueError, match="mode.*foo"):
            d.round("second", mode="foo")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "unit, increment",
        [
            ("minute", 8),
            ("second", 14),
            ("millisecond", 15),
            ("day", 2),
            ("hour", 30),
            ("microsecond", 1500),
        ],
    )
    @system_tz_ams()
    def test_invalid_increment(self, unit, increment):
        d = SystemDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000)
        with pytest.raises(ValueError, match="[Ii]ncrement"):
            d.round(unit, increment=increment)

    @system_tz_ams()
    def test_invalid_unit(self):
        d = SystemDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000)
        with pytest.raises(ValueError, match="Invalid.*unit.*foo"):
            d.round("foo")  # type: ignore[arg-type]


@system_tz_ams()
def test_pickle():
    d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_321)
    dumped = pickle.dumps(d)
    assert len(dumped) <= len(pickle.dumps(d.py_datetime()))
    assert pickle.loads(pickle.dumps(d)).exact_eq(d)


@system_tz_ams()
def test_old_pickle_data_remains_unpicklable():
    # Don't update this value -- the whole idea is that it's a pickle at
    # a specific version of the library.
    dumped = (
        b"\x80\x04\x954\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\r_unpkl_s"
        b"ystem\x94\x93\x94C\x0f\xe4\x07\x08\x0f\x17\x0c\t\xb1h\xde: \x1c\x00"
        b"\x00\x94\x85\x94R\x94."
    )
    assert pickle.loads(dumped).exact_eq(
        SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654_321)
    )


def test_copy():
    d = SystemDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654)
    assert copy(d) is d
    assert deepcopy(d) is d


def test_cannot_subclass():
    with pytest.raises(TypeError):

        class Subclass(SystemDateTime):  # type: ignore[misc]
            pass
