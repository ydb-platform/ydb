import pickle
import re
from copy import copy, deepcopy
from datetime import date as py_date, datetime as py_datetime
from itertools import chain, product

import pytest

from whenever import (
    Date,
    DateDelta,
    MonthDay,
    PlainDateTime,
    Time,
    Weekday,
    YearMonth,
    days,
)

from .common import AlwaysEqual, AlwaysLarger, AlwaysSmaller, NeverEqual

MAX_I64 = 1 << 63
MAX_I32 = 1 << 31


class TestInit:

    def test_args(self):
        d = Date(2021, 1, 2)
        assert d.year == 2021
        assert d.month == 1
        assert d.day == 2

    def test_kwargs(self):
        d = Date(year=2021, month=1, day=2)
        assert d.year == 2021
        assert d.month == 1
        assert d.day == 2

    @pytest.mark.parametrize(
        "args, kwargs",
        [
            ((2021, 1, 2), {}),
            ((), {"year": 2021, "month": 1, "day": 2}),
            ((2021,), {"month": 1, "day": 2}),
            ((2021, 3), {"day": 2}),
            ((2021, 3, 1), {}),
        ],
    )
    def test_valid_arg_kwargs(self, args, kwargs):
        assert Date(*args, **kwargs) is not None

    @pytest.mark.parametrize(
        "args, kwargs",
        [
            ((), {}),
            ((2021,), {"month": 1}),
            ((2021,), {"day": 2}),
            ((2021, 3, 4), {"day": 2}),
            ((2021, 3), {"day": 2, "month": 5}),
            ((2021, 3, 4), {"foo": 3}),
            ((2021, 3), {"day": 4, "foo": 3}),
            ((2021, 3), {9: 4, "foo": 3}),
            (("2021", 3, 1), {}),
        ],
    )
    def test_invalid_arg_kwargs(self, args, kwargs):
        with pytest.raises(TypeError):
            Date(*args, **kwargs)

    def test_not_enough_args(self):
        with pytest.raises(TypeError, match=r"day"):
            Date(2021, 1)  # type: ignore[call-arg]

        with pytest.raises(TypeError, match=r"month"):
            Date(2021)  # type: ignore[call-arg]

        with pytest.raises(TypeError, match=r"year"):
            Date()  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "year, month, day",
        [
            (0, 1, 2),
            (-1, 2, 28),
            (-MAX_I64 + 8, 2, 28),  # underflow
            (10_000, 2, 28),
            (MAX_I64 + 4, 2, 28),
            (-MAX_I64, 2, 28),
        ],
    )
    def test_invalid_year(self, year, month, day):
        with pytest.raises(
            (ValueError, OverflowError), match="int|range|date|year"
        ):
            Date(year, month, day)

    @pytest.mark.parametrize(
        "year, month, day",
        [
            (2021, 0, 1),
            (2021, 13, 1),
            (2021, -1, 1),
            (2021, MAX_I64, 1),
            (2021, -MAX_I64, 1),
        ],
    )
    def test_invalid_month(self, year, month, day):
        with pytest.raises(
            (ValueError, OverflowError), match="int|range|date|month"
        ):
            Date(year, month, day)

    @pytest.mark.parametrize(
        "year, month, day",
        [
            (2021, 1, 0),
            (2021, 12, 32),
            (2021, 12, -1),
            (2021, 1, MAX_I64),
            (2021, 1, -MAX_I64),
            # specific months
            (2021, 4, 31),
            (2021, 2, 29),
        ],
    )
    def test_invalid_day(self, year, month, day):
        with pytest.raises(
            (ValueError, OverflowError), match="int|range|date|day"
        ):
            Date(year, month, day)


def test_year_month():
    d = Date(2021, 1, 2)
    assert d.year_month() == YearMonth(2021, 1)


def test_month_day():
    d = Date(2021, 1, 2)
    assert d.month_day() == MonthDay(1, 2)


def test_py_date():
    d = Date(2021, 1, 2)
    assert d.py_date() == py_date(2021, 1, 2)


def test_today_in_system_tz():
    d = Date.today_in_system_tz()
    # NOTE: this may fail if the test is run *exactly* at midnight.
    # Mocking this out would make things more complicated than it's worth.
    assert d == Date.from_py_date(py_date.today())


def test_from_py_date():
    assert Date.from_py_date(py_date(2021, 1, 2)) == Date(2021, 1, 2)
    assert Date.from_py_date(py_datetime(2021, 1, 2, 3, 4, 5)) == Date(
        2021, 1, 2
    )

    class CustomDate(py_date):
        pass

    assert Date.from_py_date(CustomDate(2021, 1, 2)) == Date(2021, 1, 2)

    with pytest.raises(TypeError):
        Date.from_py_date(20210102)  # type: ignore[arg-type]


def test_format_common_iso():
    d = Date(2021, 1, 2)
    assert d.format_common_iso() == "2021-01-02"


def test_str():
    d = Date(2021, 1, 2)
    assert str(d) == "2021-01-02"


class TestParseCommonIso:

    @pytest.mark.parametrize(
        "s, expected",
        [
            # Extended ISO format
            ("0001-01-01", Date(1, 1, 1)),
            ("2000-01-01", Date(2000, 1, 1)),
            ("2015-11-22", Date(2015, 11, 22)),
            ("9999-12-31", Date(9999, 12, 31)),
            # "Basic" ISO format
            ("00010101", Date(1, 1, 1)),
            ("20000101", Date(2000, 1, 1)),
            ("20150902", Date(2015, 9, 2)),
            ("99991231", Date(9999, 12, 31)),
        ],
    )
    def test_valid(self, s, expected):
        assert Date.parse_common_iso(s) == expected

    @pytest.mark.parametrize(
        "s",
        [
            # non-digits
            "202A-01-02",
            "2022-a1-02",
            "2022-a1-02",
            "2023-01-3o",
            "2023Ww1-3",
            "2021-01-02T03:04:05",  # with a time
            # bad separators
            "2021-01/02",
            "2021/01-02",
            # wrong padding
            "2021-1-2",
            "021-1-002",
            # inconsistent dash use
            "2020W12-3",
            "2020-W123",
            "2020W01-3",
            "202011-12",
            "2020-1112",
            "2020-1112",
            "2020-w12-3",  # lowercase w
            # other
            "20-12-03",  # two-digit year
            "-012-12-03",  # negative year
            "312🧨-12-03",  # non-ASCII
            "202𝟙-11-02",  # non-ascii
            "999991112",  # too many digits
            "2023-W03-",  # empty day
            "YYYY-MM-DD",
            "2023/11/01",
            "2023.11.01",
            "",
            "1234",
            "1",
            "1_992101",
            # invalid dates
            "2021-02-29",
            "2021-366",
            "2000-00-03",
            "2000-13-03",
            "2000-01-32",
            "2000-01-00",
            "1989-W53",
            "1989-W22-8",
            "1989-W22-0",
            # Week and ordinal not implemented
            "2021-W01-01",
            "2021-344",
            "2021W134",
            "2021214",
            # incomplete
            "2021-01",
            "202101",
        ],
    )
    def test_invalid(self, s):
        with pytest.raises(
            ValueError,
            match=r"Invalid format.*" + re.escape(repr(s)),
        ):
            Date.parse_common_iso(s)

    def test_no_string(self):
        with pytest.raises((TypeError, AttributeError), match="(int|str)"):
            Date.parse_common_iso(20210102)  # type: ignore[arg-type]


def test_replace():
    d = Date(2021, 1, 2)
    assert d.replace(year=2022) == Date(2022, 1, 2)
    assert d.replace(month=2) == Date(2021, 2, 2)
    assert d.replace(day=3) == Date(2021, 1, 3)
    assert d == Date(2021, 1, 2)  # original is unchanged

    with pytest.raises(TypeError):
        d.replace(3)  # type: ignore[misc]

    with pytest.raises(TypeError, match="foo"):
        d.replace(foo=3)  # type: ignore[call-arg]

    with pytest.raises(TypeError, match="foo"):
        d.replace(foo="blabla")  # type: ignore[call-arg]

    with pytest.raises(ValueError, match="(date|year)"):
        d.replace(year=10_000)


def test_kwarg_interning_bug_issue_149():
    d = Date(2021, 1, 2)
    assert d.replace(**{"day": 4, "y" + (lambda: "ear")(): 2022}) == Date(
        2022, 1, 4
    )


def test_at():
    d = Date(2021, 1, 2)
    assert d.at(Time(3, 4, 5)) == PlainDateTime(2021, 1, 2, 3, 4, 5)


def test_repr():
    d = Date(221, 1, 2)
    assert repr(d) == "Date(0221-01-02)"


def test_hash():
    d = Date(2021, 1, 2)
    assert hash(d) == hash(Date(2021, 1, 2))
    assert hash(d) != hash(Date(2021, 1, 3))


def test_eq():
    d = Date(2021, 1, 2)
    same = Date(2021, 1, 2)
    different = Date(2021, 1, 3)

    assert d == same
    assert not d == different
    assert not d == NeverEqual()
    assert d == AlwaysEqual()

    assert not d != same
    assert d != different
    assert d != NeverEqual()
    assert not d != AlwaysEqual()
    assert d != None  # noqa: E711
    assert None != d  # noqa: E711
    assert not d == None  # noqa: E711
    assert not None == d  # noqa: E711

    assert hash(d) == hash(same)


def test_comparison():
    d = Date(2021, 5, 10)
    same = Date(2021, 5, 10)
    bigger = Date(2022, 2, 28)
    smaller = Date(2020, 12, 31)

    assert d <= same
    assert d <= bigger
    assert not d <= smaller
    assert d <= AlwaysLarger()
    assert not d <= AlwaysSmaller()

    assert not d < same
    assert d < bigger
    assert not d < smaller
    assert d < AlwaysLarger()
    assert not d < AlwaysSmaller()

    assert d >= same
    assert not d >= bigger
    assert d >= smaller
    assert not d >= AlwaysLarger()
    assert d >= AlwaysSmaller()

    assert not d > same
    assert not d > bigger
    assert d > smaller
    assert not d > AlwaysLarger()
    assert d > AlwaysSmaller()


class TestAdd:

    @pytest.mark.parametrize(
        "d, kwargs, expected",
        [
            (Date(2021, 1, 31), dict(), Date(2021, 1, 31)),
            (Date(2021, 1, 31), dict(days=1), Date(2021, 2, 1)),
            (Date(2021, 2, 1), dict(days=-1), Date(2021, 1, 31)),
            (Date(2021, 2, 28), dict(months=-2), Date(2020, 12, 28)),
            (Date(2021, 1, 31), dict(years=1), Date(2022, 1, 31)),
            (Date(2021, 1, 31), dict(months=37), Date(2024, 2, 29)),
            (Date(2020, 2, 29), dict(years=1), Date(2021, 2, 28)),
            (Date(2020, 2, 29), dict(years=1, days=1), Date(2021, 3, 1)),
            (Date(2020, 2, 29), dict(years=1, weeks=2), Date(2021, 3, 14)),
            (
                Date(2020, 1, 30),
                dict(years=1, months=1, days=1),
                Date(2021, 3, 1),
            ),
            (
                Date(2020, 1, 30),
                dict(years=1, months=1, weeks=1),
                Date(2021, 3, 7),
            ),
            # this checks that truncation isn't done after years, but after
            # months *and* years
            (
                Date(2020, 2, 29),
                dict(years=1, months=1),
                Date(2021, 3, 29),
            ),
        ],
    )
    def test_valid(self, d, kwargs, expected):
        assert d.add(**kwargs) == expected
        assert d + DateDelta(**kwargs) == expected
        assert d.add(DateDelta(**kwargs)) == expected

    @pytest.mark.parametrize(
        "d, kwargs",
        [
            (Date(2021, 1, 31), dict(years=8000)),
            (Date(2021, 1, 31), dict(days=8000 * 365)),
            (Date(2021, 1, 31), dict(years=-3000)),
            (Date(2021, 1, 31), dict(days=MAX_I64 + 3)),
            (Date(2021, 1, 31), dict(weeks=-MAX_I64 - 2)),
            (Date(2021, 1, 31), dict(months=MAX_I64 + 2)),
            (Date(2021, 1, 31), dict(months=MAX_I32 - 2, years=1)),
        ],
    )
    def test_out_of_range(self, d, kwargs):
        with pytest.raises((OverflowError, ValueError)):
            d.add(**kwargs)

        with pytest.raises((OverflowError, ValueError)):
            d + DateDelta(**kwargs)

        with pytest.raises((OverflowError, ValueError)):
            d.add(DateDelta(**kwargs))

    def test_invalid(self):
        with pytest.raises(TypeError):
            Date(2021, 1, 1) + None  # type: ignore[operator]

        with pytest.raises(TypeError):
            None + Date(2021, 1, 1)  # type: ignore[operator]

        with pytest.raises(TypeError):
            py_date(2020, 1, 1) + Date(2021, 1, 1)  # type: ignore[operator]

    def test_no_mix_arg_kwargs(self):
        d = Date(2020, 1, 1)
        with pytest.raises(TypeError):
            d.add(DateDelta(years=1), months=1)  # type: ignore[call-overload]


class TestDaysUntilAndSince:

    @pytest.mark.parametrize(
        "d1, d2, expected",
        [
            (Date(2021, 1, 1), Date(2021, 1, 31), 30),
            (Date(2020, 2, 28), Date(2020, 2, 28), 0),
            (Date(2020, 2, 28), Date(2020, 3, 1), 2),
            (Date(2020, 2, 28), Date(2020, 2, 1), -27),
            (Date(1990, 5, 2), Date(2021, 12, 1), 11536),
            (Date.MIN, Date.MAX, 3652058),
        ],
    )
    def test_days_until_and_since(self, d1, d2, expected):
        assert d1.days_until(d2) == expected
        assert d2.days_since(d1) == expected
        assert d1.days_since(d2) == -expected
        assert d2.days_until(d1) == -expected

    def test_invalid(self):
        with pytest.raises((TypeError, AttributeError)):
            Date(2021, 1, 1).days_until(PlainDateTime(2021, 1, 1, 1, 2, 3))  # type: ignore[arg-type]


_EXAMPLE_DATES = [
    *chain.from_iterable(
        [
            Date(y, 1, 1),
            Date(y, 1, 2),
            Date(y, 1, 4),
            Date(y, 1, 10),
            Date(y, 1, 28),
            Date(y, 1, 29),
            Date(y, 1, 30),
            Date(y, 1, 31),
            Date(y, 2, 1),
            Date(y, 2, 26),
            Date(y, 2, 27),
            Date(y, 2, 28),
            Date(y, 3, 1),
            Date(y, 3, 2),
            Date(y, 3, 31),
            Date(y, 4, 1),
            Date(y, 4, 2),
            Date(y, 4, 15),
            Date(y, 4, 30),
            Date(y, 5, 1),
            Date(y, 5, 31),
            Date(y, 8, 25),
            Date(y, 11, 30),
            Date(y, 12, 1),
            Date(y, 12, 2),
            Date(y, 12, 27),
            Date(y, 12, 28),
            Date(y, 12, 29),
            Date(y, 12, 30),
            Date(y, 12, 31),
        ]
        for y in (2020, 2021, 2022, 2023, 2024)
    ),
    Date(2024, 2, 29),
    Date(2020, 2, 29),
]


class TestSubtract:

    @pytest.mark.parametrize(
        "d, kwargs, expected",
        [
            (Date(2021, 1, 31), dict(), Date(2021, 1, 31)),
            (Date(2021, 1, 31), dict(days=1), Date(2021, 1, 30)),
            (Date(2021, 2, 1), dict(days=-1), Date(2021, 2, 2)),
            (Date(2021, 2, 28), dict(months=2), Date(2020, 12, 28)),
            (Date(2021, 1, 31), dict(years=1), Date(2020, 1, 31)),
            (Date(2021, 1, 31), dict(months=37), Date(2017, 12, 31)),
            (Date(2020, 2, 29), dict(years=1), Date(2019, 2, 28)),
            (Date(2020, 2, 29), dict(years=1, days=1), Date(2019, 2, 27)),
            (
                Date(2020, 1, 30),
                dict(years=1, months=1, days=1),
                Date(2018, 12, 29),
            ),
            (
                Date(2020, 1, 30),
                dict(years=1, months=1, weeks=1),
                Date(2018, 12, 23),
            ),
        ],
    )
    def test_valid_delta(self, d, kwargs, expected):
        assert d.subtract(**kwargs) == expected
        assert d - DateDelta(**kwargs) == expected
        assert d.subtract(DateDelta(**kwargs)) == expected

    @pytest.mark.parametrize(
        "delta",
        [
            {"years": 3000},
            {"days": 3000 * 365},
            {"years": -8000},
            {"days": MAX_I64 + 3},
            {"weeks": -MAX_I64 - 2},
            {"months": MAX_I64 + 2},
        ],
    )
    def test_delta_out_of_bounds(self, delta):
        with pytest.raises((OverflowError, ValueError)):
            Date(2021, 1, 1) - DateDelta(**delta)
        with pytest.raises((OverflowError, ValueError)):
            Date(2021, 1, 1).subtract(**delta)
        with pytest.raises((OverflowError, ValueError)):
            Date(2021, 1, 1).subtract(DateDelta(**delta))

    @pytest.mark.parametrize(
        "d1, d2, expected",
        [
            (Date(2021, 1, 31), Date(2021, 1, 1), days(30)),
            (Date(2021, 1, 1), Date(2021, 1, 31), -days(30)),
            (Date(2021, 1, 20), Date(2021, 1, 11), days(9)),
            (Date(2021, 2, 28), Date(2021, 2, 28), days(0)),
            (Date(2021, 2, 28), Date(2021, 2, 27), days(1)),
            (Date(2021, 2, 28), Date(2021, 2, 1), days(27)),
        ],
    )
    def test_days(self, d1, d2, expected):
        assert d1 - d2 == expected

    @pytest.mark.parametrize(
        "d1, d2, delta",
        [
            (Date(2021, 2, 1), Date(2020, 1, 29), DateDelta(years=1, days=3)),
            (Date(2021, 1, 31), Date(2020, 12, 31), DateDelta(months=1)),
            (Date(2020, 12, 31), Date(2021, 1, 31), DateDelta(months=-1)),
            (
                Date(2021, 1, 20),
                Date(2020, 12, 19),
                DateDelta(months=1, days=1),
            ),
            (Date(2024, 2, 28), Date(2024, 2, 29), -DateDelta(days=1)),
            (Date(2024, 2, 29), Date(2024, 2, 28), DateDelta(days=1)),
            (
                Date(2024, 2, 29),
                Date(2023, 3, 1),
                DateDelta(months=11, days=28),
            ),
            (
                Date(2024, 2, 29),
                Date(2023, 3, 2),
                DateDelta(months=11, days=27),
            ),
            (
                Date(2023, 3, 2),
                Date(2024, 2, 29),
                -DateDelta(months=11, days=27),
            ),
            (Date(2024, 1, 31), Date(2023, 1, 31), DateDelta(years=1)),
            (
                Date(2023, 1, 31),
                Date(2024, 2, 29),
                -DateDelta(years=1, days=28),
            ),
            (
                Date(2023, 1, 30),
                Date(2024, 2, 29),
                -DateDelta(years=1, days=29),
            ),
            (
                Date(2022, 12, 30),
                Date(2024, 2, 29),
                -DateDelta(years=1, months=1, days=30),
            ),
            (
                Date(2024, 2, 29),
                Date(2023, 1, 31),
                DateDelta(years=1, months=1),
            ),
            (Date(2024, 2, 29), Date(2023, 2, 28), DateDelta(years=1, days=1)),
            (Date(2023, 2, 28), Date(2024, 2, 29), -DateDelta(years=1)),
            (Date(2023, 2, 28), Date(2024, 2, 28), -DateDelta(years=1)),
            (Date(2025, 2, 28), Date(2024, 2, 29), DateDelta(years=1)),
            (
                Date(2024, 2, 29),
                Date(2025, 2, 28),
                -DateDelta(months=11, days=28),
            ),
            (
                Date(2023, 2, 28),
                Date(2024, 2, 29),
                DateDelta(years=-1),
            ),
        ],
    )
    def test_months_and_years(self, d1, d2, delta):
        assert d1 - d2 == delta
        assert d2 + delta == d1

    def test_invalid_type(self):
        with pytest.raises(TypeError, match="unsupported operand"):
            Date(2021, 1, 1) - 1  # type: ignore[operator]
        with pytest.raises(TypeError, match="unsupported operand"):
            Date(2021, 1, 1) - "2021-01-01"  # type: ignore[operator]

        with pytest.raises(TypeError):
            None - Date(2021, 1, 1)  # type: ignore[operator]

        with pytest.raises(TypeError):
            3 - Date(2021, 1, 1)  # type: ignore[operator]

        with pytest.raises(TypeError):
            DateDelta() - Date(2021, 1, 1)  # type: ignore[operator]

        with pytest.raises(TypeError):
            Date(2021, 1, 1) - PlainDateTime(2020, 3, 2)  # type: ignore[operator]

        with pytest.raises(TypeError):
            PlainDateTime(2021, 1, 1) - Date(2020, 3, 2)  # type: ignore[operator]

    def test_fuzzing(self):
        for d1, d2 in product(_EXAMPLE_DATES, _EXAMPLE_DATES):
            delta = d1 - d2
            assert d2 + delta == d1


def test_day_of_week():
    d = Date(2021, 1, 2)
    assert d.day_of_week() is Weekday.SATURDAY
    assert Date(2021, 1, 3).day_of_week() is Weekday.SUNDAY
    assert Date(2021, 1, 4).day_of_week() is Weekday.MONDAY
    assert Date(2021, 1, 5).day_of_week() is Weekday.TUESDAY
    assert Date(2021, 1, 6).day_of_week() is Weekday.WEDNESDAY
    assert Date(2021, 1, 7).day_of_week() is Weekday.THURSDAY
    assert Date(2021, 1, 8).day_of_week() is Weekday.FRIDAY

    # test that days can be imported directly from the module too
    from whenever import (
        FRIDAY,
        MONDAY,
        SATURDAY,
        SUNDAY,
        THURSDAY,
        TUESDAY,
        WEDNESDAY,
    )

    assert Date(1915, 7, 19).day_of_week() is MONDAY
    assert Date(1915, 7, 20).day_of_week() is TUESDAY
    assert Date(1915, 7, 21).day_of_week() is WEDNESDAY
    assert Date(1915, 7, 22).day_of_week() is THURSDAY
    assert Date(1915, 7, 23).day_of_week() is FRIDAY
    assert Date(1915, 7, 24).day_of_week() is SATURDAY
    assert Date(1915, 7, 25).day_of_week() is SUNDAY

    assert pickle.loads(pickle.dumps(SATURDAY)) is SATURDAY


def test_pickling():
    d = Date(2021, 1, 2)
    dumped = pickle.dumps(d)
    assert len(dumped) < len(pickle.dumps(d.py_date())) + 10
    assert pickle.loads(dumped) == d


def test_unpickle_compatibility():
    dumped = (
        b"\x80\x04\x95'\x00\x00\x00\x00\x00\x00\x00\x8c\x08whenever\x94\x8c\x0b_unp"
        b"kl_date\x94\x93\x94C\x04\xe5\x07\x01\x02\x94\x85\x94R\x94."
    )
    assert pickle.loads(dumped) == Date(2021, 1, 2)


def test_copy():
    d = Date(2021, 1, 2)
    assert copy(d) is d
    assert deepcopy(d) is d


def test_singletons():
    assert Date.MIN == Date(1, 1, 1)
    assert Date.MAX == Date(9999, 12, 31)


def test_cannot_subclass():
    with pytest.raises(TypeError):

        class SubclassDate(Date):  # type: ignore[misc]
            pass
