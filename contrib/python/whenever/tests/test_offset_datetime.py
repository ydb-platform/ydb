import pickle
import re
from datetime import datetime as py_datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis.strategies import floats, integers, text

from whenever import (
    Date,
    ImplicitlyIgnoringDST,
    Instant,
    OffsetDateTime,
    PlainDateTime,
    SystemDateTime,
    Time,
    TimeZoneNotFoundError,
    ZonedDateTime,
    days,
    hours,
    milliseconds,
    minutes,
    months,
    nanoseconds,
    seconds,
)

from .common import (
    AlwaysEqual,
    AlwaysLarger,
    AlwaysSmaller,
    NeverEqual,
    system_tz_ams,
    system_tz_nyc,
)


class TestInit:
    def test_init_and_attributes(self):
        d = OffsetDateTime(
            2020, 8, 15, 5, 12, 30, nanosecond=450, offset=hours(5)
        )
        assert d.year == 2020
        assert d.month == 8
        assert d.day == 15
        assert d.hour == 5
        assert d.minute == 12
        assert d.second == 30
        assert d.nanosecond == 450
        assert d.offset == hours(5)

    def test_int_offset(self):
        d = OffsetDateTime(2020, 8, 15, 5, 12, 30, nanosecond=450, offset=-5)
        assert d.offset == hours(-5)

    def test_offset_missing(self):
        with pytest.raises(TypeError, match="required.*offset"):
            OffsetDateTime(2020, 8, 15, 5, 12, 30, nanosecond=450)  # type: ignore[call-arg]

    def test_invalid_offset_int(self):
        with pytest.raises(ValueError, match="offset.*24.*hours"):
            OffsetDateTime(2020, 8, 15, 5, 12, offset=34)

    def test_invalid_offset_delta(self):
        # too large
        with pytest.raises(ValueError, match="offset.*24.*hours"):
            OffsetDateTime(2020, 8, 15, 5, 12, offset=hours(34))

        # too precise
        with pytest.raises(ValueError, match="(o|O)ffset.*whole.*seconds"):
            OffsetDateTime(
                2020, 8, 15, 5, 12, offset=hours(34) + milliseconds(1)
            )

    def test_init_optionality(self):
        assert (
            OffsetDateTime(2020, 8, 15, 12, offset=5)
            == OffsetDateTime(2020, 8, 15, 12, 0, offset=5)
            == OffsetDateTime(2020, 8, 15, 12, 0, 0, offset=5)
        )

    def test_kwargs(self):
        d = OffsetDateTime(
            year=2020,
            month=8,
            day=15,
            hour=5,
            minute=12,
            second=30,
            offset=5,
        )
        assert d == OffsetDateTime(
            2020, 8, 15, 5, 12, 30, nanosecond=0, offset=5
        )

    def test_invalid(self):
        with pytest.raises(ValueError, match="date|day"):
            OffsetDateTime(2020, 2, 30, 5, 12, offset=5)

        with pytest.raises(ValueError, match="time|minute"):
            OffsetDateTime(2020, 2, 28, 5, 64, offset=5)

        with pytest.raises(ValueError, match="nano|time"):
            OffsetDateTime(
                2020, 2, 28, 5, 12, nanosecond=1_000_000_000, offset=5
            )

    def test_bounds(self):
        with pytest.raises(ValueError, match="range"):
            OffsetDateTime(1, 1, 1, 0, offset=1)


def test_immutable():
    d = OffsetDateTime(2020, 8, 15, offset=minutes(5))
    with pytest.raises(AttributeError):
        d.year = 2021  # type: ignore[misc]


class TestFormatCommonIso:

    @pytest.mark.parametrize(
        "d, expected",
        [
            (
                OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=5),
                "2020-08-15T23:12:09+05:00",
            ),
            (
                OffsetDateTime(
                    2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=5
                ),
                "2020-08-15T23:12:09.000987654+05:00",
            ),
            (
                OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=minutes(73)),
                "2020-08-15T23:12:09+01:13",
            ),
            (
                OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-minutes(73)),
                "2020-08-15T23:12:09-01:13",
            ),
            (
                OffsetDateTime(
                    2020,
                    8,
                    15,
                    23,
                    12,
                    9,
                    nanosecond=1,
                    offset=minutes(73) + seconds(32),
                ),
                "2020-08-15T23:12:09.000000001+01:13:32",
            ),
        ],
    )
    def test_default(self, d: OffsetDateTime, expected: str):
        assert str(d) == expected
        assert d.format_common_iso() == expected


INVALID_ISO_STRINGS = [
    # padding issues
    "2020-08-15T2:08:30+05:00:01",
    "2020-8-15T12:8:30+05:00",
    # fraction issues
    "2020-08-15T12:08:30.0000000001+05:00",
    "2020-08-15T12:08:30.+05:00",
    "2020-08-15T12:08:30+05:00:21.0",
    "2020-08-15T12:08:30+05:00:21.",
    # separators
    "2020-08-15T12:08.30+05:00",
    "2020-08-15T12:08.30+05.00",
    "2020-08-15T12:08.30+05 00",
    # invalid offset
    "2020-08-15T12:08:30+24:00",
    "2020-08-15T12:08:30-24:00",
    "2020-08-15T12:08:30+09:80",
    "2020-08-15T12:08:30-09:30:60",
    "2020-08-15T12:08:30-99:00",
    # other
    "2020-08-15T12:08:30+05:00stuff",  # trailing stuff
    "2020-08-15T12:𝟘8:30+00:00",  # non-ASCII
    "2020-08-15T12:08:30.0034+05:𝟙0",  # non-ASCII
    "2020-08-15T12:08:30[Iceland]",  # TZ ID but no offset
    # not enough content
    "",
    "T",
    "2020",
    "2020-08-15",
    "2020-08-15T",
    "garbage",
    # out-of-bounds
    "9999-12-31T22:08:30-05:00",
    "0001-01-01 02:08:30+05:00",
    # invalid tz format
    "2020-08-15T12:08:30+05:00[",
    "2020-08-15T12:08:30+05:00[[]",
    "2020-08-15T12:08:30+05:00[sdf[]",
    "2020-08-15T12:08:30+05:00[]",
    "2020-08-15T12:08:30+05:00]",
    "2020-08-15T12:08:30+05:00[abc]foo",
    # unsupported ISO features (weekdays, ordinal dates)
    "2020-W08-1T23:12:09-01",
    "2020-123T23:12:09-01",
]

VALID_ISO_STRINGS = [
    (
        "2020-08-15T12:08:30+05:00",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=5),
    ),
    (
        "2020-08-15T12:08:30+20:00",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=20),
    ),
    (
        "2020-08-15T12:08:30.0034+05:00",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, nanosecond=3_400_000, offset=5),
    ),
    (
        "2020-08-15T12:08:30.000000010+05:00",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, nanosecond=10, offset=5),
    ),
    (
        "2020-08-15T12:08:30.0034-05:00:01",
        OffsetDateTime(
            2020,
            8,
            15,
            12,
            8,
            30,
            nanosecond=3_400_000,
            offset=-hours(5) - seconds(1),
        ),
    ),
    (
        "2020-08-15T12:08:30+00:00",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=0),
    ),
    (
        "2020-08-15T12:08:30-00:00",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=0),
    ),
    (
        "2020-08-15T12:08:30Z",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=0),
    ),
    (
        "2020-08-15T12:08:30z",
        OffsetDateTime(2020, 8, 15, 12, 8, 30, offset=0),
    ),
    # Shorter and alternative time formats
    (
        "1924-12-02T12+00",
        OffsetDateTime(1924, 12, 2, 12, offset=0),
    ),
    (
        "1924-12-02T12+01:00",
        OffsetDateTime(1924, 12, 2, 12, offset=1),
    ),
    (
        "19241203t12:00:12+003059",
        OffsetDateTime(
            1924,
            12,
            3,
            12,
            0,
            12,
            offset=minutes(30) + seconds(59),
        ),
    ),
    (
        "1924-08-15T120012-0030",
        OffsetDateTime(1924, 8, 15, 12, 0, 12, offset=minutes(-30)),
    ),
    (
        "2020-08-15T120012-0030[Foo]",
        OffsetDateTime(2020, 8, 15, 12, 0, 12, offset=minutes(-30)),
    ),
    # Decimals
    (
        "2020-08-15T120012,3-00:30[Foo]",
        OffsetDateTime(
            2020, 8, 15, 12, 0, 12, nanosecond=300_000_000, offset=minutes(-30)
        ),
    ),
    (
        "2020-08-15T120012.3112-00:30[Foo]",
        OffsetDateTime(
            2020, 8, 15, 12, 0, 12, nanosecond=311_200_000, offset=minutes(-30)
        ),
    ),
]


class TestParseCommonIso:
    @pytest.mark.parametrize("s, expect", VALID_ISO_STRINGS)
    def test_valid(self, s, expect):
        assert OffsetDateTime.parse_common_iso(s).exact_eq(expect)

    @pytest.mark.parametrize("s", INVALID_ISO_STRINGS)
    def test_invalid(self, s):
        with pytest.raises(ValueError, match="format.*" + re.escape(repr(s))):
            OffsetDateTime.parse_common_iso(s)

    @pytest.mark.parametrize(
        "s",
        [
            "0001-01-01T02:08:30+05:00",
            "9999-12-31T22:08:30-05:00",
        ],
    )
    def test_bounds(self, s):
        with pytest.raises(ValueError):
            OffsetDateTime.parse_common_iso(s)

    @given(text())
    def test_fuzzing(self, s: str):
        with pytest.raises(
            ValueError,
            match=r"format.*" + re.escape(repr(s)),
        ):
            OffsetDateTime.parse_common_iso(s)


def test_exact_equality():
    d = OffsetDateTime(2020, 8, 15, 12, offset=5)
    same = d.replace(ignore_dst=True)
    utc_same = d.replace(hour=13, offset=hours(6), ignore_dst=True)
    different = d.replace(offset=hours(6), ignore_dst=True)
    assert d.exact_eq(same)
    assert not d.exact_eq(utc_same)
    assert not d.exact_eq(different)
    assert not d.exact_eq(d.replace(nanosecond=1, ignore_dst=True))

    with pytest.raises(TypeError):
        d.exact_eq(d.to_instant())  # type: ignore[arg-type]


class TestEquality:
    def test_same_exact(self):
        d = OffsetDateTime(2020, 8, 15, 12, offset=5)
        same = d.replace(ignore_dst=True)
        assert d == same
        assert not d != same
        assert hash(d) == hash(same)

    def test_different(self):
        d = OffsetDateTime(2020, 8, 15, 12, offset=5)
        different = d.replace(offset=hours(6), ignore_dst=True)
        assert d != different
        assert not d == different
        assert hash(d) != hash(different)

    def test_same_time(self):
        d = OffsetDateTime(2020, 8, 15, 12, offset=5)
        same_time = d.replace(hour=11, offset=hours(4), ignore_dst=True)
        assert d == same_time
        assert not d != same_time
        assert hash(d) == hash(same_time)

    def test_zoned(self):
        d: OffsetDateTime | ZonedDateTime = OffsetDateTime(
            2023, 10, 29, 5, 15, offset=4
        )
        zoned_same = ZonedDateTime(
            2023, 10, 29, 2, 15, tz="Europe/Paris", disambiguate="later"
        )
        zoned_different = ZonedDateTime(
            2023, 10, 29, 2, 15, tz="Europe/Paris", disambiguate="earlier"
        )
        assert d == zoned_same
        assert not d != zoned_same
        assert not d == zoned_different
        assert d != zoned_different

        assert hash(d) == hash(zoned_same)
        assert hash(d) != hash(zoned_different)

    @system_tz_ams()
    def test_system_tz(self):
        d: OffsetDateTime | SystemDateTime = OffsetDateTime(
            2023, 10, 29, 0, 15, offset=-1
        )
        sys_same = SystemDateTime(2023, 10, 29, 2, 15, disambiguate="later")
        sys_different = SystemDateTime(
            2023, 10, 29, 2, 15, disambiguate="earlier"
        )
        assert d == sys_same
        assert not d != sys_same
        assert not d == sys_different
        assert d != sys_different

        assert hash(d) == hash(sys_same)
        assert hash(d) != hash(sys_different)

    def test_utc(self):
        d: Instant | OffsetDateTime = OffsetDateTime(2020, 8, 15, 12, offset=5)
        utc_same = Instant.from_utc(2020, 8, 15, 7)
        utc_different = Instant.from_utc(2020, 8, 15, 7, 1)
        assert d == utc_same
        assert not d != utc_same
        assert not d == utc_different
        assert d != utc_different

        assert hash(d) == hash(utc_same)
        assert hash(d) != hash(utc_different)

    def test_not_implemented(self):
        d = OffsetDateTime(2020, 8, 15, 12, offset=5)
        assert d == AlwaysEqual()
        assert d != NeverEqual()
        assert not d == NeverEqual()
        assert not d != AlwaysEqual()
        assert d != 42  # type: ignore[comparison-overlap]
        assert not d == 42  # type: ignore[comparison-overlap]
        assert 42 != d  # type: ignore[comparison-overlap]
        assert not 42 == d  # type: ignore[comparison-overlap]


class TestTimestamp:

    def test_default_seconds(self):
        assert OffsetDateTime(1970, 1, 1, 3, offset=3).timestamp() == 0
        assert (
            OffsetDateTime(
                2020, 8, 15, 8, 8, 30, nanosecond=45, offset=-4
            ).timestamp()
            == 1_597_493_310
        )

    def test_millis(self):
        assert OffsetDateTime(1970, 1, 1, 3, offset=3).timestamp_millis() == 0
        assert (
            OffsetDateTime(
                2020, 8, 15, 8, 8, 30, nanosecond=45_999_123, offset=-4
            ).timestamp_millis()
            == 1_597_493_310_045
        )

    def test_nanos(self):
        assert OffsetDateTime(1970, 1, 1, 3, offset=3).timestamp_nanos() == 0
        assert (
            OffsetDateTime(
                2020, 8, 15, 8, 8, 30, nanosecond=45, offset=-4
            ).timestamp_nanos()
            == 1_597_493_310_000_000_045
        )


class TestFromTimestamp:

    @pytest.mark.parametrize(
        "method, factor",
        [
            (OffsetDateTime.from_timestamp, 1),
            (OffsetDateTime.from_timestamp_millis, 1_000),
            (OffsetDateTime.from_timestamp_nanos, 1_000_000_000),
        ],
    )
    def test_all(self, method, factor):
        assert method(0, offset=3, ignore_dst=True).exact_eq(
            OffsetDateTime(1970, 1, 1, 3, offset=3)
        )
        assert method(
            1_597_493_310 * factor, offset=hours(-2), ignore_dst=True
        ).exact_eq(OffsetDateTime(2020, 8, 15, 10, 8, 30, offset=-2))
        with pytest.raises((OSError, OverflowError, ValueError)):
            method(
                1_000_000_000_000_000_000 * factor, offset=3, ignore_dst=True
            )

        with pytest.raises((OSError, OverflowError, ValueError)):
            method(
                -1_000_000_000_000_000_000 * factor, offset=3, ignore_dst=True
            )

        with pytest.raises(TypeError):
            method(0, offset="3", ignore_dst=True)

        with pytest.raises(TypeError):
            method("0", offset=3, ignore_dst=True)

        with pytest.raises(ValueError):
            method(0, offset=hours(31), ignore_dst=True)

        with pytest.raises(TypeError, match="got 3|foo"):
            method(0, offset=3, foo="bar", ignore_dst=True)

        with pytest.raises(TypeError):
            method(0, foo="bar", ignore_dst=True)

        with pytest.raises(TypeError):
            method(0, ignore_dst=True)

        with pytest.raises(TypeError):
            method(0, 3, ignore_dst=True)

        with pytest.raises(ImplicitlyIgnoringDST):
            method(0, offset=3)

        assert OffsetDateTime.from_timestamp_millis(
            -4, offset=1, ignore_dst=True
        ).to_instant() == Instant.from_timestamp(0) - milliseconds(4)

        assert OffsetDateTime.from_timestamp_nanos(
            -4, offset=-3, ignore_dst=True
        ).to_instant() == Instant.from_timestamp(0) - nanoseconds(4)

    def test_float(self):
        assert OffsetDateTime.from_timestamp(
            1.0, offset=1, ignore_dst=True
        ).exact_eq(OffsetDateTime.from_timestamp(1, offset=1, ignore_dst=True))

        assert OffsetDateTime.from_timestamp(
            1.000_000_001, offset=1, ignore_dst=True
        ).exact_eq(
            OffsetDateTime.from_timestamp(1, offset=1, ignore_dst=True).add(
                nanoseconds=1, ignore_dst=True
            )
        )

        assert OffsetDateTime.from_timestamp(
            -9.000_000_100, offset=-2, ignore_dst=True
        ).exact_eq(
            OffsetDateTime.from_timestamp(
                -9, offset=-2, ignore_dst=True
            ).subtract(nanoseconds=100, ignore_dst=True)
        )

        with pytest.raises((ValueError, OverflowError)):
            OffsetDateTime.from_timestamp(9e200, ignore_dst=True, offset=0)

        with pytest.raises((ValueError, OverflowError, OSError)):
            OffsetDateTime.from_timestamp(
                float(Instant.MAX.timestamp()) + 0.99999999,
                ignore_dst=True,
                offset=0,
            )

        with pytest.raises((ValueError, OverflowError)):
            OffsetDateTime.from_timestamp(
                float("inf"), ignore_dst=True, offset=0
            )

        with pytest.raises((ValueError, OverflowError)):
            OffsetDateTime.from_timestamp(
                float("nan"), ignore_dst=True, offset=0
            )

    def test_nanos(self):
        assert OffsetDateTime.from_timestamp_nanos(
            1_597_493_310_123_456_789, offset=-2, ignore_dst=True
        ).exact_eq(
            OffsetDateTime(
                2020, 8, 15, 10, 8, 30, nanosecond=123_456_789, offset=-2
            )
        )

    def test_millis(self):
        assert OffsetDateTime.from_timestamp_millis(
            1_597_493_310_123, offset=-2, ignore_dst=True
        ).exact_eq(
            OffsetDateTime(
                2020, 8, 15, 10, 8, 30, nanosecond=123_000_000, offset=-2
            )
        )


def test_repr():
    d = OffsetDateTime(
        2020,
        8,
        15,
        23,
        12,
        9,
        nanosecond=1_987_654,
        offset=hours(5) + minutes(22),
    )
    assert repr(d) == "OffsetDateTime(2020-08-15 23:12:09.001987654+05:22)"
    assert (
        repr(OffsetDateTime(2020, 8, 15, 23, 12, offset=0))
        == "OffsetDateTime(2020-08-15 23:12:00+00:00)"
    )


class TestComparison:
    def test_offset(self):
        d = OffsetDateTime(2020, 8, 15, 12, 30, offset=5)
        later = d.replace(nanosecond=13, ignore_dst=True)
        assert d < later
        assert d <= later
        assert later > d
        assert later >= d

    def test_instant(self):
        d = OffsetDateTime(2020, 8, 15, 12, 30, offset=5)
        inst_eq = d.to_instant()
        inst_gt = inst_eq + minutes(1)
        inst_lt = inst_eq - minutes(1)

        assert d >= inst_eq
        assert d <= inst_eq
        assert not d > inst_eq
        assert not d < inst_eq

        assert d < inst_gt
        assert d <= inst_gt
        assert not d > inst_gt
        assert not d >= inst_gt

        assert d > inst_lt
        assert d >= inst_lt
        assert not d < inst_lt
        assert not d <= inst_lt

    def test_zoned(self):
        d = OffsetDateTime(2023, 10, 29, 5, 30, offset=5)
        zoned_eq = d.to_tz("Europe/Paris")
        zoned_gt = zoned_eq.replace(minute=31, disambiguate="earlier")
        zoned_lt = zoned_eq.replace(minute=29, disambiguate="earlier")

        assert d >= zoned_eq
        assert d <= zoned_eq
        assert not d > zoned_eq
        assert not d < zoned_eq

        assert d < zoned_gt
        assert d <= zoned_gt
        assert not d > zoned_gt
        assert not d >= zoned_gt

        assert d > zoned_lt
        assert d >= zoned_lt
        assert not d < zoned_lt
        assert not d <= zoned_lt

    @system_tz_nyc()
    def test_system_tz(self):
        d = OffsetDateTime(2020, 8, 15, 12, 30, offset=5)
        sys_eq = d.to_system_tz()
        sys_gt = sys_eq + minutes(1)
        sys_lt = sys_eq - minutes(1)

        assert d >= sys_eq
        assert d <= sys_eq
        assert not d > sys_eq
        assert not d < sys_eq

        assert d < sys_gt
        assert d <= sys_gt
        assert not d > sys_gt
        assert not d >= sys_gt

        assert d > sys_lt
        assert d >= sys_lt
        assert not d < sys_lt
        assert not d <= sys_lt

    def test_not_implemented(self):
        d = OffsetDateTime(2020, 8, 15, 12, 30, offset=5)

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


@pytest.mark.parametrize(
    "d, expect",
    [
        (
            OffsetDateTime(
                2020, 8, 15, 23, 12, 9, nanosecond=987_654_999, offset=5
            ),
            py_datetime(
                2020,
                8,
                15,
                23,
                12,
                9,
                987_654,
                tzinfo=timezone(timedelta(hours=5)),
            ),
        ),
        (
            OffsetDateTime(
                2020,
                8,
                15,
                23,
                12,
                9,
                nanosecond=987_654_999,
                offset=minutes(-73),
            ),
            py_datetime(
                2020,
                8,
                15,
                23,
                12,
                9,
                987_654,
                tzinfo=timezone(timedelta(minutes=-73)),
            ),
        ),
    ],
)
def test_py_datetime(d: OffsetDateTime, expect: py_datetime):
    assert d.py_datetime() == expect


class TestFromPyDatetime:

    def test_offset(self):

        d = py_datetime(
            2020,
            8,
            15,
            23,
            12,
            9,
            987_654,
            tzinfo=timezone(timedelta(hours=2)),
        )
        assert OffsetDateTime.from_py_datetime(d).exact_eq(
            OffsetDateTime(
                2020, 8, 15, 23, 12, 9, nanosecond=987_654_000, offset=2
            )
        )

    def test_zoneinfo(self):

        d = py_datetime(
            2020,
            8,
            15,
            23,
            12,
            9,
            987_654,
            tzinfo=ZoneInfo("Europe/Amsterdam"),
        )
        assert OffsetDateTime.from_py_datetime(d).exact_eq(
            OffsetDateTime(
                2020, 8, 15, 23, 12, 9, nanosecond=987_654_000, offset=2
            )
        )

    def test_naive(self):
        with pytest.raises(ValueError, match="naive"):
            OffsetDateTime.from_py_datetime(py_datetime(12, 3, 4, 12))

    def test_out_of_range(self):
        d = py_datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=5)))
        with pytest.raises(ValueError, match="range"):
            OffsetDateTime.from_py_datetime(d)

    def test_utcoffset_none(self):

        class MyTz(tzinfo):
            def utcoffset(self, _):
                return None

        with pytest.raises(ValueError, match="naive"):
            OffsetDateTime.from_py_datetime(
                py_datetime(2020, 8, 15, tzinfo=MyTz())  # type: ignore[abstract]
            )

    def test_subsecond_offset(self):
        with pytest.raises(ValueError, match="Sub-second"):
            OffsetDateTime.from_py_datetime(
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

    def test_subclass(self):
        class MyDatetime(py_datetime):
            pass

        d = MyDatetime(2020, 8, 15, 23, 12, 9, 987_654, tzinfo=timezone.utc)
        assert OffsetDateTime.from_py_datetime(d).exact_eq(
            OffsetDateTime(
                2020, 8, 15, 23, 12, 9, nanosecond=987_654_000, offset=0
            )
        )


def test_replace_date():
    d = OffsetDateTime(2020, 8, 15, 3, 12, 9, offset=5)
    assert d.replace_date(Date(1996, 2, 19), ignore_dst=True).exact_eq(
        OffsetDateTime(1996, 2, 19, 3, 12, 9, offset=5)
    )

    with pytest.raises(ValueError, match="range"):
        d.replace_date(Date(1, 1, 1), ignore_dst=True)

    with pytest.raises((TypeError, AttributeError), match="date"):
        d.replace_date(42, ignore_dst=True)  # type: ignore[arg-type]

    # ignore_dst required
    with pytest.raises(ImplicitlyIgnoringDST):
        d.replace_date(Date(1996, 2, 19))  # type: ignore[call-arg]


def test_replace_time():
    d = OffsetDateTime(2020, 8, 15, 3, 12, 9, offset=5)
    assert d.replace_time(Time(1, 2, 3), ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 15, 1, 2, 3, offset=5)
    )

    d2 = OffsetDateTime(1, 1, 1, 3, 12, 9, offset=3)
    with pytest.raises(ValueError, match="range"):
        d2.replace_time(Time(1), ignore_dst=True)

    with pytest.raises((TypeError, AttributeError)):
        d.replace_time(42, ignore_dst=True)  # type: ignore[arg-type]

    # ignore_dst required
    with pytest.raises(ImplicitlyIgnoringDST):
        d.replace_time(Time(1, 2, 3))  # type: ignore[call-arg]


def test_components():
    d = OffsetDateTime(2020, 8, 15, 3, 12, 9, offset=5)
    assert d.date() == Date(2020, 8, 15)
    assert d.time() == Time(3, 12, 9)
    assert d.offset == hours(5)


class TestNow:

    def test_timedelta(self):
        now = OffsetDateTime.now(hours(5), ignore_dst=True)
        assert now.offset == hours(5)
        py_now = py_datetime.now(timezone.utc)
        assert py_now - now.py_datetime() < timedelta(seconds=1)

        # ignore_dst required
        with pytest.raises(ImplicitlyIgnoringDST):
            OffsetDateTime.now(3)  # type: ignore[call-arg]

    def test_int(self):
        now = OffsetDateTime.now(-5, ignore_dst=True)
        assert now.offset == hours(-5)
        py_now = py_datetime.now(timezone.utc)
        assert py_now - now.py_datetime() < timedelta(seconds=1)


def test_replace():
    d = OffsetDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=5)
    assert d.replace(year=2021, ignore_dst=True).exact_eq(
        OffsetDateTime(2021, 8, 15, 23, 12, 9, nanosecond=987_654, offset=5)
    )
    assert d.replace(month=9, ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 9, 15, 23, 12, 9, nanosecond=987_654, offset=5)
    )
    assert d.replace(day=16, ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 16, 23, 12, 9, nanosecond=987_654, offset=5)
    )
    assert d.replace(hour=1, ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 15, 1, 12, 9, nanosecond=987_654, offset=5)
    )
    assert d.replace(minute=59, ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 15, 23, 59, 9, nanosecond=987_654, offset=5)
    )
    assert d.replace(second=2, ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 15, 23, 12, 2, nanosecond=987_654, offset=5)
    )
    assert d.replace(nanosecond=3, ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 15, 23, 12, 9, nanosecond=3, offset=5)
    )
    assert d.replace(offset=hours(6), ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=6)
    )
    assert d.replace(offset=-6, ignore_dst=True).exact_eq(
        OffsetDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=-6)
    )

    with pytest.raises(TypeError, match="tzinfo"):
        d.replace(tzinfo=timezone.utc, ignore_dst=True)  # type: ignore[call-arg]

    with pytest.raises(ValueError, match="range"):
        d.replace(year=1, month=1, day=1, hour=4, offset=5, ignore_dst=True)

    with pytest.raises(TypeError, match="nano"):
        d.replace(nanosecond="0", ignore_dst=True)  # type: ignore[arg-type]

    # ignore_dst required
    with pytest.raises(ImplicitlyIgnoringDST):
        d.replace(year=2021)  # type: ignore[call-arg]


def test_add_operator_not_allowed():
    d = OffsetDateTime(2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=5)
    with pytest.raises(TypeError, match="unsupported operand type"):
        d + hours(4)  # type: ignore[operator]

    with pytest.raises(TypeError, match="unsupported operand type"):
        d + 32  # type: ignore[operator]

    with pytest.raises(TypeError, match="unsupported operand type"):
        32 + d  # type: ignore[operator]

    with pytest.raises(TypeError, match="unsupported operand type"):
        hours(4) + d  # type: ignore[operator]


class TestShiftMethods:

    def test_valid(self):
        d = OffsetDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=-5
        )
        shifted = OffsetDateTime(
            2020, 5, 27, 23, 12, 14, nanosecond=987_651, offset=-5
        )
        assert d.add(ignore_dst=True).exact_eq(d)

        assert d.add(
            months=-3,
            days=10,
            hours=48,
            seconds=5,
            nanoseconds=-3,
            ignore_dst=True,
        ).exact_eq(shifted)

        # same result with deltas
        assert (
            d.add(hours(48) + seconds(5) + nanoseconds(-3), ignore_dst=True)
            .add(months(-3), ignore_dst=True)
            .add(days(10), ignore_dst=True)
            .exact_eq(shifted)
        )

        # same result with subtract()
        assert d.subtract(
            months=3,
            days=-10,
            hours=-48,
            seconds=-5,
            nanoseconds=3,
            ignore_dst=True,
        ).exact_eq(shifted)

        # same result with deltas
        assert (
            d.subtract(
                hours(-48) + seconds(-5) + nanoseconds(3), ignore_dst=True
            )
            .subtract(months(3), ignore_dst=True)
            .subtract(days(-10), ignore_dst=True)
            .exact_eq(shifted)
        )

    def test_invalid(self):
        d = OffsetDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=4
        )
        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(hours=24 * 365 * 8000, ignore_dst=True)

        with pytest.raises((ValueError, OverflowError), match="range|year"):
            d.add(hours=-24 * 365 * 3000, ignore_dst=True)

        with pytest.raises((TypeError, AttributeError)):
            d.add(4, ignore_dst=True)  # type: ignore[call-overload]

        # ignore_dst is required
        with pytest.raises(ImplicitlyIgnoringDST):
            d.add(hours=48, seconds=5)  # type: ignore[call-overload]

        # no mixing args/kwargs
        with pytest.raises(TypeError):
            d.add(seconds(4), hours=48, seconds=5, ignore_dst=True)  # type: ignore[call-overload]

        # tempt a i128 overflow
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
        d = OffsetDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654_321, offset=2
        )
        try:
            d.add(**kwargs, ignore_dst=True)
        except (ValueError, OverflowError):
            pass


class TestDifference:

    def test_offset(self):
        d = OffsetDateTime(2020, 8, 15, 23, 12, 9, nanosecond=3, offset=5)
        other = OffsetDateTime(2020, 8, 14, 23, 12, 4, nanosecond=4, offset=-3)
        assert d - other == hours(16) + seconds(5) - nanoseconds(1)

        # same result with method
        assert d.difference(other) == d - other

    def test_instant(self):
        d = OffsetDateTime(2020, 8, 15, 20, offset=5)
        other = Instant.from_utc(2020, 8, 15, 20)
        assert d - other == -hours(5)

        # same result with method
        assert d.difference(other) == d - other

    def test_zoned(self):
        d = OffsetDateTime(2023, 10, 29, 6, offset=2)
        other = ZonedDateTime(
            2023,
            10,
            29,
            3,
            tz="Europe/Paris",
        )
        assert d - other == hours(2)
        assert d - ZonedDateTime(
            2023, 10, 29, 2, tz="Europe/Paris", disambiguate="later"
        ) == hours(3)
        assert d - ZonedDateTime(
            2023, 10, 29, 2, tz="Europe/Paris", disambiguate="earlier"
        ) == hours(4)
        assert d - ZonedDateTime(2023, 10, 29, 1, tz="Europe/Paris") == hours(
            5
        )

        # same result with method
        assert d.difference(other) == d - other

    @system_tz_ams()
    def test_system_tz(self):
        d = OffsetDateTime(2023, 10, 29, 6, offset=2)
        other = SystemDateTime(2023, 10, 29, 3, disambiguate="later")
        assert d - other == hours(2)
        assert d - SystemDateTime(
            2023, 10, 29, 2, disambiguate="later"
        ) == hours(3)
        assert d - SystemDateTime(
            2023, 10, 29, 2, disambiguate="earlier"
        ) == hours(4)
        assert d - SystemDateTime(2023, 10, 29, 1) == hours(5)

        # same result with method
        assert d.difference(other) == d - other

    def test_invalid(self):
        d = OffsetDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654, offset=5
        )
        with pytest.raises(TypeError, match="unsupported operand type"):
            d - 42  # type: ignore[operator]

        # subtracting a delta hints at the ignore_dst way
        with pytest.raises(ImplicitlyIgnoringDST):
            d - hours(2)  # type: ignore[operator]


def test_pickle():
    d = OffsetDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=987_654_321, offset=3
    )
    dumped = pickle.dumps(d)
    assert len(dumped) <= len(pickle.dumps(d.py_datetime()))
    assert pickle.loads(pickle.dumps(d)).exact_eq(d)


def test_old_pickle_data_remains_unpicklable():
    # Don't update this value -- the whole idea is that it's a pickle at
    # a specific version of the library.
    dumped = (
        b"\x80\x04\x954\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\r_unpkl_o"
        b"ffset\x94\x93\x94C\x0f\xe4\x07\x08\x0f\x17\x0c\t\xb1h\xde:0*\x00"
        b"\x00\x94\x85\x94R\x94."
    )
    assert pickle.loads(dumped).exact_eq(
        OffsetDateTime(
            2020, 8, 15, 23, 12, 9, nanosecond=987_654_321, offset=3
        )
    )


def test_instant():
    d = OffsetDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=987_654_321, offset=3
    )
    assert d.to_instant() == Instant.from_utc(
        2020, 8, 15, 20, 12, 9, nanosecond=987_654_321
    )

    with pytest.deprecated_call():
        assert d.to_instant() == d.instant()  # type: ignore[attr-defined]


def test_to_fixed_offset():
    d = OffsetDateTime(
        2020, 8, 15, 23, 12, 9, nanosecond=987_654_321, offset=3
    )
    assert d.to_fixed_offset(5).exact_eq(
        OffsetDateTime(2020, 8, 16, 1, 12, 9, nanosecond=987_654_321, offset=5)
    )
    assert d.to_fixed_offset().exact_eq(d)
    assert d.to_fixed_offset(-3).exact_eq(
        OffsetDateTime(
            2020, 8, 15, 17, 12, 9, nanosecond=987_654_321, offset=-3
        )
    )

    with pytest.raises((ValueError, OverflowError)):
        OffsetDateTime(1, 1, 1, hour=3, minute=59, offset=0).to_fixed_offset(
            -4
        )

    with pytest.raises((ValueError, OverflowError)):
        OffsetDateTime(9999, 12, 31, hour=23, offset=0).to_fixed_offset(1)


def test_to_tz():
    d = OffsetDateTime(
        2020, 8, 15, 20, 12, 9, nanosecond=987_654_321, offset=3
    )
    assert d.to_tz("America/New_York").exact_eq(
        ZonedDateTime(
            2020,
            8,
            15,
            13,
            12,
            9,
            nanosecond=987_654_321,
            tz="America/New_York",
        )
    )
    with pytest.raises(TimeZoneNotFoundError):
        d.to_tz("America/Not_A_Real_Zone")

    small_dt = OffsetDateTime(1, 1, 1, offset=0)
    with pytest.raises((ValueError, OverflowError, OSError)):
        small_dt.to_tz("America/New_York")

    big_dt = OffsetDateTime(9999, 12, 31, hour=23, offset=0)
    with pytest.raises((ValueError, OverflowError, OSError)):
        big_dt.to_tz("Asia/Tokyo")


@system_tz_nyc()
def test_to_system_tz():
    d = OffsetDateTime(
        2020, 8, 15, 20, 12, 9, nanosecond=987_654_321, offset=3
    )
    assert d.to_system_tz().exact_eq(
        SystemDateTime(2020, 8, 15, 13, 12, 9, nanosecond=987_654_321)
    )

    small_dt = OffsetDateTime(1, 1, 1, offset=0)
    with pytest.raises((ValueError, OverflowError)):
        small_dt.to_system_tz()

    big_dt = OffsetDateTime(9999, 12, 31, hour=23, offset=0)
    with system_tz_ams():
        with pytest.raises((ValueError, OverflowError)):
            big_dt.to_system_tz()


def test_to_plain():
    d = OffsetDateTime(2020, 8, 15, 20, nanosecond=1, offset=3)
    assert d.to_plain() == PlainDateTime(2020, 8, 15, 20, nanosecond=1)

    with pytest.deprecated_call():
        assert d.local() == d.to_plain()  # type: ignore[attr-defined]


class TestParseStrptime:

    @pytest.mark.parametrize(
        "string, fmt, expected",
        [
            (
                "2020-08-15 23:12+0315",
                "%Y-%m-%d %H:%M%z",
                OffsetDateTime(
                    2020, 8, 15, 23, 12, offset=hours(3) + minutes(15)
                ),
            ),
            (
                "2020-08-15 23:12:09+05:50:12",
                "%Y-%m-%d %H:%M:%S%z",
                OffsetDateTime(
                    2020,
                    8,
                    15,
                    23,
                    12,
                    9,
                    offset=hours(5) + minutes(50) + seconds(12),
                ),
            ),
            (
                "2020-08-15 23:12:09Z",
                "%Y-%m-%d %H:%M:%S%z",
                OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=0),
            ),
            (
                "2020-08-15 23:12:09.234678Z",
                "%Y-%m-%d %H:%M:%S.%f%z",
                OffsetDateTime(
                    2020, 8, 15, 23, 12, 9, nanosecond=234_678_000, offset=0
                ),
            ),
        ],
    )
    def test_valid(self, string, fmt, expected):
        assert OffsetDateTime.parse_strptime(string, format=fmt) == expected

    def test_invalid(self):
        # no offset
        with pytest.raises(ValueError):
            OffsetDateTime.parse_strptime(
                "2020-08-15 23:12:09", format="%Y-%m-%d %H:%M:%S"
            )

        # format is keyword-only
        with pytest.raises(TypeError, match="format|argument"):
            OffsetDateTime.parse_strptime(
                "2020-08-15 23:12:09 +0400", "%Y-%m-%d %H:%M:%S %z"  # type: ignore[misc]
            )

        # out of range
        with pytest.raises(ValueError, match="range"):
            OffsetDateTime.parse_strptime(
                "0001-01-01 03:12:09+0550", format="%Y-%m-%d %H:%M:%S%z"
            )

        # sub-second offset
        with pytest.raises(ValueError):
            OffsetDateTime.parse_strptime(
                "2020-08-15 23:12:09 +01:22:01.43",
                format="%Y-%m-%d %H:%M:%S %z",
            )


@pytest.mark.parametrize(
    "d, expected",
    [
        (
            OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=5),
            "Sat, 15 Aug 2020 23:12:09 +0500",
        ),
        (
            OffsetDateTime(2020, 8, 5, 23, 12, 9, offset=5),
            "Wed, 05 Aug 2020 23:12:09 +0500",
        ),
        (
            OffsetDateTime(1, 1, 1, 9, 9, 9, offset=minutes(1)),
            "Mon, 01 Jan 0001 09:09:09 +0001",
        ),
        (
            OffsetDateTime(
                2020,
                8,
                15,
                23,
                12,
                9,
                offset=hours(5) + minutes(22) + seconds(45),
            ),
            "Sat, 15 Aug 2020 23:12:09 +0522",
        ),
        (
            OffsetDateTime(
                2020,
                8,
                15,
                23,
                12,
                9,
                offset=-(hours(5) + minutes(22) + seconds(45)),
            ),
            "Sat, 15 Aug 2020 23:12:09 -0522",
        ),
    ],
)
def test_rfc2822(d, expected):
    assert d.format_rfc2822() == expected


VALID_RFC2822 = [
    (
        "Sat, 15 Aug 2020 23:12:09 GMT",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=0),
    ),
    (
        "Sat, 15 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=0),
    ),
    (
        "Sat, 1 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 1, 23, 12, 9, offset=0),
    ),
    (
        "Sat, 01 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 1, 23, 12, 9, offset=0),
    ),
    (
        "Sat, 15 Aug 2020 23:12:09 -0000",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=0),
    ),
    (
        "Sat, 15 Aug 2020 23:12:09 UTC",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=0),
    ),
    (
        "Sat, 15 Aug 2020 23:12:09 -0100",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-1),
    ),
    (
        "Sat, 15 Aug 2020 23:12:09 +1200",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=12),
    ),
    (
        "Sun, 2 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 2, 23, 12, 9, offset=0),
    ),
    (
        "Mon, 3 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 3, 23, 12, 9, offset=0),
    ),
    (
        "Tue, 4 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 4, 23, 12, 9, offset=0),
    ),
    (
        "Wed, 5 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 5, 23, 12, 9, offset=0),
    ),
    (
        "Thu, 6 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 6, 23, 12, 9, offset=0),
    ),
    (
        "Fri, 7 Aug 2020 23:12:09 +0000",
        OffsetDateTime(2020, 8, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Jan 2020 23:12:09 +0000",
        OffsetDateTime(2020, 1, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Feb 2020 23:12:09 +0000",
        OffsetDateTime(2020, 2, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Mar 2020 23:12:09 +0000",
        OffsetDateTime(2020, 3, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Apr 2020 23:12:09 +0000",
        OffsetDateTime(2020, 4, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 May 2020 23:12:09 +0000",
        OffsetDateTime(2020, 5, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Jun 2020 23:12:09 +0000",
        OffsetDateTime(2020, 6, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Jul 2020 23:12:09 +0000",
        OffsetDateTime(2020, 7, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Sep 2020 23:12:09 +0000",
        OffsetDateTime(2020, 9, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Oct 2020 23:12:09 +0000",
        OffsetDateTime(2020, 10, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Nov 2020 23:12:09 +0000",
        OffsetDateTime(2020, 11, 7, 23, 12, 9, offset=0),
    ),
    (
        "7 Dec 2020 23:12:09 +0000",
        OffsetDateTime(2020, 12, 7, 23, 12, 9, offset=0),
    ),
    # named timezones
    (
        "Sat, 15 Aug 2020 23:12:09 MST",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-7),
    ),
    # non-4-digit years
    (
        "Sun, 15 Aug 49 23:12:09 +1200",
        OffsetDateTime(2000 + 49, 8, 15, 23, 12, 9, offset=12),
    ),
    (
        "Tue, 15 Aug 50 23:12:09 +1200",
        OffsetDateTime(1900 + 50, 8, 15, 23, 12, 9, offset=12),
    ),
    (
        "Mon, 15 Aug 049 23:12:09 +1200",
        OffsetDateTime(1900 + 49, 8, 15, 23, 12, 9, offset=12),
    ),
    (
        "Thu, 15 Aug 220 23:12:09 +1200",
        OffsetDateTime(1900 + 220, 8, 15, 23, 12, 9, offset=12),
    ),
    # various whitespace is allowed
    (
        "   15      Aug 2020\r\n  \r\n23:12 \t UTC   ",
        OffsetDateTime(2020, 8, 15, 23, 12, offset=0),
    ),
    (
        "Sat\t, 15 Aug 2020 23:12:09 MST",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-7),
    ),
    (
        "Sat, 15 Aug 2020 23 :12 : 09 MST",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-7),
    ),
    (
        "Sat, 15 Aug 2020 23: \t12:09\nMST",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-7),
    ),
    (
        "Sat,15 Aug 2020 23:12:09 MST",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-7),
    ),
    (
        "Sat   ,15 Aug 2020 23:12:09 MST",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-7),
    ),
    # technically not valid whitespace, but we accept it
    (
        "Sat\t,\n\r15 Aug 2020 23:\x0b\t12\x0c:09\nMST",
        OffsetDateTime(2020, 8, 15, 23, 12, 9, offset=-7),
    ),
    # According to the spec, unknown timezones should be interpreted as -0000.
    (
        "15 Aug 2020  23:12 FOO",
        OffsetDateTime(2020, 8, 15, 23, 12, offset=0),
    ),
    (
        "15 Aug 2020  23:12 a",
        OffsetDateTime(2020, 8, 15, 23, 12, offset=0),
    ),
    # Case insensitive
    (
        "TUe, 15 auG 1950 23:12:09 MsT",
        OffsetDateTime(1950, 8, 15, 23, 12, 9, offset=-7),
    ),
    # minimal required
    (
        "5 Aug 20 23:12 UT",
        OffsetDateTime(2020, 8, 5, 23, 12, offset=0),
    ),
]

INVALID_RFC2822 = [
    # Invalid timezone/offset
    "Sat, 15 Aug 2020 23:12:09",
    "Sat, 15 Aug 2020 23:12 -",
    "Sat, 15 Aug 2020 23:12 +",
    "Sat, 15 Aug 2020 23:12 0400",
    "Sat, 15 Aug 2020 23:12 +400",
    "Sat, 15 Aug 2020 23:12 +4060",
    "Sat, 15 Aug 2020 23:12 -4060",
    "Sat, 15 Aug 2020 23:12 +MST",
    "Sat, 15 Aug 2020 23:12 -MST",
    "Sat, 15 Aug 2020 23:12 MST4",
    "Sat, 15 Aug 2020 23:12 -04",
    "Sat, 15 Aug 2020 23:12 -   ",
    # whitespace problems
    "Sat, 15Aug 2020 23:12 -2100",
    "Sat, 15 Aug2020 23:12 -2100",
    "Sat, 15 Aug 202023:12 -2100",
    "Sat, 15 Aug 2020 23:12-2100",
    "Sat, 15 Aug 2020 23:12:00-2100",
    # Invalid values
    "Sun, 15 Aug 2020 23:12 +0400",
    "Foo, 15 Aug 2020 23:12 +0400",
    "Sat, 32 Aug 2020 23:12 +0400",
    "Sat, 31 Sep 2020 23:12 +0400",
    "Sat, 0 Sep 2020 23:12 +0400",
    "Sat, 1 Sep 0000 23:12 +0400",
    "Sat, 1 Sep 2020 24:12 +0400",
    "Sat, 1 Sep 2020 22:62 +0400",
    "Sat, 1 Sep 2020 22:22 +2400",
    "Sat, 1 Sep 2020 22:22 -2400",
    "Tue, 29 Feb 2023 22:22 -0400",
    "Wed, 30 Feb 2024 22:22 -0400",
    "Sat, 1 Foo 2020 14:12 +0400",
    "Sat, 15 Aug 2𝟘2𝟘 23:12:09 +0400",  # non-ascii
    # invalid comma
    "Mon 28 Feb 2023 22:22 -0400",
    "Sat. 15 Aug 2020 23:12:09 GMT",
    "Sat.15 Aug 2020 23:12:09 GMT",
    "Sat .15 Aug 2020 23:12:09 GMT",
    "Sat . 15 Aug 2020 23:12:09 GMT",
    "Tue, 028 Feb 2023 22:22 -0400",
    "Tue, 28 Feb 02023 22:22 -0400",
    "Tue, 28 Feb 2023 022:22 -0400",
    "Tue, 28 Feb 2023 22:022 -0400",
    "Tue, 28 Feb 2023 22:22 -00000400",
    # garbage strings
    "",
    "    \t\r\n ",
    "\t",
    " ",
    "garbage",
    # incomplete
    "S,",
    "Sa",
    "Sat",
    "Sat,",
    "Sat, ",
    "Sat, 1",
    "Sat, 1 ",
    "Sat, 1 Ja",
    "Sat, 1 Jan 20",
    "Sat, 1 Jan 89",
    "Sat, 1 Jan 198",
    "Sat, 1 Jan 1989 23",
    "Sat, 1 Jan 1989 23:",
    "Sat, 1 Jan 1989 23:2",
    "Sat, 1 Jan 1989 23:23",
    "Sat, 1 Jan 1989 23:23:0",
    "Sat, 1 Jan 1989 23:23:01 ",
    "Sat, 1 Jan 1989 23:23:01 +03 00",
    "Sat, 1 Jan 1989 23:23:01 +0300 ,",
    "Sat, 1 Jan 1989 23:23:01 +0300 MST",
]


class TestParseRFC2822:

    @pytest.mark.parametrize("s, expected", VALID_RFC2822)
    def test_valid(self, s, expected):
        assert OffsetDateTime.parse_rfc2822(s) == expected

    @pytest.mark.parametrize("s", INVALID_RFC2822)
    def test_invalid(self, s):
        with pytest.raises(ValueError, match=re.escape(repr(s))):
            OffsetDateTime.parse_rfc2822(s)

    @pytest.mark.parametrize(
        "s",
        [
            "Mon, 1 Jan 0001 03:12:09 +0400",
            "Fri, 31 Dec 9999 23:12:09 -0400",
        ],
    )
    def test_out_of_range(self, s):
        with pytest.raises(ValueError, match="range"):
            OffsetDateTime.parse_rfc2822(s)


class TestRound:

    @pytest.mark.parametrize(
        "d, increment, unit, floor, ceil, half_floor, half_ceil, half_even",
        [
            (
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 3, nanosecond=459_999_999, offset=2
                ),
                1,
                "nanosecond",
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 3, nanosecond=459_999_999, offset=2
                ),
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 3, nanosecond=459_999_999, offset=2
                ),
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 3, nanosecond=459_999_999, offset=2
                ),
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 3, nanosecond=459_999_999, offset=2
                ),
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 3, nanosecond=459_999_999, offset=2
                ),
            ),
            (
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 3, nanosecond=459_999_999, offset=2
                ),
                1,
                "second",
                OffsetDateTime(2023, 7, 14, 1, 2, 3, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 4, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 3, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 3, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 3, offset=2),
            ),
            (
                OffsetDateTime(
                    2023, 7, 14, 1, 2, 21, nanosecond=459_999_999, offset=2
                ),
                4,
                "second",
                OffsetDateTime(2023, 7, 14, 1, 2, 20, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 24, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 20, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 20, offset=2),
                OffsetDateTime(2023, 7, 14, 1, 2, 20, offset=2),
            ),
            (
                OffsetDateTime(
                    2023, 7, 14, 23, 52, 29, nanosecond=999_999_999, offset=2
                ),
                10,
                "minute",
                OffsetDateTime(2023, 7, 14, 23, 50, 0, offset=2),
                OffsetDateTime(2023, 7, 15, offset=2),
                OffsetDateTime(2023, 7, 14, 23, 50, 0, offset=2),
                OffsetDateTime(2023, 7, 14, 23, 50, 0, offset=2),
                OffsetDateTime(2023, 7, 14, 23, 50, 0, offset=2),
            ),
            (
                OffsetDateTime(
                    2023, 7, 14, 11, 59, 29, nanosecond=999_999_999, offset=2
                ),
                12,
                "hour",
                OffsetDateTime(2023, 7, 14, offset=2),
                OffsetDateTime(2023, 7, 14, 12, 0, 0, offset=2),
                OffsetDateTime(2023, 7, 14, 12, 0, 0, offset=2),
                OffsetDateTime(2023, 7, 14, 12, 0, 0, offset=2),
                OffsetDateTime(2023, 7, 14, 12, 0, 0, offset=2),
            ),
            (
                OffsetDateTime(2023, 7, 14, 12, offset=2),
                1,
                "day",
                OffsetDateTime(2023, 7, 14, offset=2),
                OffsetDateTime(2023, 7, 15, offset=2),
                OffsetDateTime(2023, 7, 14, offset=2),
                OffsetDateTime(2023, 7, 15, offset=2),
                OffsetDateTime(2023, 7, 14, offset=2),
            ),
            (
                OffsetDateTime(2023, 7, 14, offset=2),
                1,
                "day",
                OffsetDateTime(2023, 7, 14, offset=2),
                OffsetDateTime(2023, 7, 14, offset=2),
                OffsetDateTime(2023, 7, 14, offset=2),
                OffsetDateTime(2023, 7, 14, offset=2),
                OffsetDateTime(2023, 7, 14, offset=2),
            ),
        ],
    )
    def test_round(
        self,
        d: OffsetDateTime,
        increment,
        unit,
        floor,
        ceil,
        half_floor,
        half_ceil,
        half_even,
    ):
        assert d.round(unit, increment=increment, ignore_dst=True) == half_even
        assert (
            d.round(unit, increment=increment, mode="floor", ignore_dst=True)
            == floor
        )
        assert (
            d.round(unit, increment=increment, mode="ceil", ignore_dst=True)
            == ceil
        )
        assert (
            d.round(
                unit, increment=increment, mode="half_floor", ignore_dst=True
            )
            == half_floor
        )
        assert (
            d.round(
                unit, increment=increment, mode="half_ceil", ignore_dst=True
            )
            == half_ceil
        )
        assert (
            d.round(
                unit, increment=increment, mode="half_even", ignore_dst=True
            )
            == half_even
        )

    def test_default(self):
        d = OffsetDateTime(
            2023, 7, 14, 1, 2, 3, nanosecond=500_000_000, offset=2
        )
        assert d.round(ignore_dst=True) == OffsetDateTime(
            2023, 7, 14, 1, 2, 4, offset=2
        )
        assert d.replace(second=8, ignore_dst=True).round(
            ignore_dst=True
        ) == OffsetDateTime(2023, 7, 14, 1, 2, 8, offset=2)

        # ignore_dst is required
        with pytest.raises(ImplicitlyIgnoringDST):
            d.round()  # type: ignore[call-arg]

    def test_invalid_mode(self):
        d = OffsetDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000, offset=2)
        with pytest.raises(ValueError, match="Invalid.*mode.*foo"):
            d.round("second", mode="foo", ignore_dst=True)  # type: ignore[arg-type]

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
        d = OffsetDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000, offset=2)
        with pytest.raises(ValueError, match="[Ii]ncrement"):
            d.round(unit, increment=increment, ignore_dst=True)

    def test_default_increment(self):
        d = OffsetDateTime(2023, 7, 14, 1, 2, 3, nanosecond=800_000, offset=-9)
        assert d.round("millisecond", ignore_dst=True).exact_eq(
            OffsetDateTime(
                2023, 7, 14, 1, 2, 3, nanosecond=1_000_000, offset=-9
            )
        )

    def test_invalid_unit(self):
        d = OffsetDateTime(2023, 7, 14, 1, 2, 3, nanosecond=4_000, offset=2)
        with pytest.raises(ValueError, match="Invalid.*unit.*foo"):
            d.round("foo", ignore_dst=True)  # type: ignore[arg-type]

    def test_out_of_range(self):
        d = PlainDateTime.MAX.replace(nanosecond=0).assume_fixed_offset(0)
        with pytest.raises((ValueError, OverflowError), match="range"):
            d.round("second", increment=5, ignore_dst=True)


def test_cannot_subclass():
    with pytest.raises(TypeError):

        class Subclass(OffsetDateTime):  # type: ignore[misc]
            pass
