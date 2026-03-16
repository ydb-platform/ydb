"""Test cases for the isoduration module."""

from datetime import date, datetime, timedelta

import pytest

from isodate import (
    D_ALT_EXT,
    D_DEFAULT,
    D_WEEK,
    Duration,
    ISO8601Error,
    duration_isoformat,
    parse_duration,
)

# the following list contains tuples of ISO duration strings and the expected
# result from the parse_duration method. A result of None means an ISO8601Error
# is expected.
PARSE_TEST_CASES = (
    ("P18Y9M4DT11H9M8S", Duration(4, 8, 0, 0, 9, 11, 0, 9, 18), D_DEFAULT, None),
    ("P2W", timedelta(weeks=2), D_WEEK, None),
    ("P3Y6M4DT12H30M5S", Duration(4, 5, 0, 0, 30, 12, 0, 6, 3), D_DEFAULT, None),
    ("P23DT23H", timedelta(hours=23, days=23), D_DEFAULT, None),
    ("P4Y", Duration(years=4), D_DEFAULT, None),
    ("P1M", Duration(months=1), D_DEFAULT, None),
    ("PT1M", timedelta(minutes=1), D_DEFAULT, None),
    ("P0.5Y", Duration(years=0.5), D_DEFAULT, None),
    ("PT36H", timedelta(hours=36), D_DEFAULT, "P1DT12H"),
    ("P1DT12H", timedelta(days=1, hours=12), D_DEFAULT, None),
    ("+P11D", timedelta(days=11), D_DEFAULT, "P11D"),
    ("-P2W", timedelta(weeks=-2), D_WEEK, None),
    ("-P2.2W", timedelta(weeks=-2.2), D_DEFAULT, "-P15DT9H36M"),
    ("P1DT2H3M4S", timedelta(days=1, hours=2, minutes=3, seconds=4), D_DEFAULT, None),
    ("P1DT2H3M", timedelta(days=1, hours=2, minutes=3), D_DEFAULT, None),
    ("P1DT2H", timedelta(days=1, hours=2), D_DEFAULT, None),
    ("PT2H", timedelta(hours=2), D_DEFAULT, None),
    ("PT2.3H", timedelta(hours=2.3), D_DEFAULT, "PT2H18M"),
    ("PT2H3M4S", timedelta(hours=2, minutes=3, seconds=4), D_DEFAULT, None),
    ("PT3M4S", timedelta(minutes=3, seconds=4), D_DEFAULT, None),
    ("PT22S", timedelta(seconds=22), D_DEFAULT, None),
    ("PT22.22S", timedelta(seconds=22.22), "PT%S.%fS", "PT22.220000S"),
    ("-P2Y", Duration(years=-2), D_DEFAULT, None),
    ("-P3Y6M4DT12H30M5S", Duration(-4, -5, 0, 0, -30, -12, 0, -6, -3), D_DEFAULT, None),
    (
        "-P1DT2H3M4S",
        timedelta(days=-1, hours=-2, minutes=-3, seconds=-4),
        D_DEFAULT,
        None,
    ),
    # alternative format
    ("P0018-09-04T11:09:08", Duration(4, 8, 0, 0, 9, 11, 0, 9, 18), D_ALT_EXT, None),
    # 'PT000022.22', timedelta(seconds=22.22),
)

#                       d1                    d2           '+', '-', '>'
# A list of test cases to test addition and subtraction between datetime and
# Duration objects.
# each tuple contains 2 duration strings, and a result string for addition and
# one for subtraction. The last value says, if the first duration is greater
# than the second.
MATH_TEST_CASES = (
    (
        "P5Y7M1DT9H45M16.72S",
        "PT27M24.68S",
        "P5Y7M1DT10H12M41.4S",
        "P5Y7M1DT9H17M52.04S",
        None,
    ),
    ("PT28M12.73S", "PT56M29.92S", "PT1H24M42.65S", "-PT28M17.19S", False),
    (
        "P3Y7M23DT5H25M0.33S",
        "PT1H1.95S",
        "P3Y7M23DT6H25M2.28S",
        "P3Y7M23DT4H24M58.38S",
        None,
    ),
    (
        "PT1H1.95S",
        "P3Y7M23DT5H25M0.33S",
        "P3Y7M23DT6H25M2.28S",
        "-P3Y7M23DT4H24M58.38S",
        None,
    ),
    ("P1332DT55M0.33S", "PT1H1.95S", "P1332DT1H55M2.28S", "P1331DT23H54M58.38S", True),
    (
        "PT1H1.95S",
        "P1332DT55M0.33S",
        "P1332DT1H55M2.28S",
        "-P1331DT23H54M58.38S",
        False,
    ),
)


# A list of test cases to test addition and subtraction of date/datetime
# and Duration objects. They are tested against the results of an
# equal long timedelta duration.
DATE_TEST_CASES = (
    (
        date(2008, 2, 29),
        timedelta(days=10, hours=12, minutes=20),
        Duration(days=10, hours=12, minutes=20),
    ),
    (
        date(2008, 1, 31),
        timedelta(days=10, hours=12, minutes=20),
        Duration(days=10, hours=12, minutes=20),
    ),
    (
        datetime(2008, 2, 29),
        timedelta(days=10, hours=12, minutes=20),
        Duration(days=10, hours=12, minutes=20),
    ),
    (
        datetime(2008, 1, 31),
        timedelta(days=10, hours=12, minutes=20),
        Duration(days=10, hours=12, minutes=20),
    ),
    (
        datetime(2008, 4, 21),
        timedelta(days=10, hours=12, minutes=20),
        Duration(days=10, hours=12, minutes=20),
    ),
    (
        datetime(2008, 5, 5),
        timedelta(days=10, hours=12, minutes=20),
        Duration(days=10, hours=12, minutes=20),
    ),
    (datetime(2000, 1, 1), timedelta(hours=-33), Duration(hours=-33)),
    (
        datetime(2008, 5, 5),
        Duration(years=1, months=1, days=10, hours=12, minutes=20),
        Duration(months=13, days=10, hours=12, minutes=20),
    ),
    (
        datetime(2000, 3, 30),
        Duration(years=1, months=1, days=10, hours=12, minutes=20),
        Duration(months=13, days=10, hours=12, minutes=20),
    ),
)

# A list of test cases of addition of date/datetime and Duration. The results
# are compared against a given expected result.
DATE_CALC_TEST_CASES = (
    (date(2000, 2, 1), Duration(years=1, months=1), date(2001, 3, 1)),
    (date(2000, 2, 29), Duration(years=1, months=1), date(2001, 3, 29)),
    (date(2000, 2, 29), Duration(years=1), date(2001, 2, 28)),
    (date(1996, 2, 29), Duration(years=4), date(2000, 2, 29)),
    (date(2096, 2, 29), Duration(years=4), date(2100, 2, 28)),
    (date(2000, 2, 1), Duration(years=-1, months=-1), date(1999, 1, 1)),
    (date(2000, 2, 29), Duration(years=-1, months=-1), date(1999, 1, 29)),
    (date(2000, 2, 1), Duration(years=1, months=1, days=1), date(2001, 3, 2)),
    (date(2000, 2, 29), Duration(years=1, months=1, days=1), date(2001, 3, 30)),
    (date(2000, 2, 29), Duration(years=1, days=1), date(2001, 3, 1)),
    (date(1996, 2, 29), Duration(years=4, days=1), date(2000, 3, 1)),
    (date(2096, 2, 29), Duration(years=4, days=1), date(2100, 3, 1)),
    (date(2000, 2, 1), Duration(years=-1, months=-1, days=-1), date(1998, 12, 31)),
    (date(2000, 2, 29), Duration(years=-1, months=-1, days=-1), date(1999, 1, 28)),
    (date(2001, 4, 1), Duration(years=-1, months=-1, days=-1), date(2000, 2, 29)),
    (date(2000, 4, 1), Duration(years=-1, months=-1, days=-1), date(1999, 2, 28)),
    (
        Duration(years=1, months=2),
        Duration(years=0, months=0, days=1),
        Duration(years=1, months=2, days=1),
    ),
    (Duration(years=-1, months=-1, days=-1), date(2000, 4, 1), date(1999, 2, 28)),
    (Duration(years=1, months=1, weeks=5), date(2000, 1, 30), date(2001, 4, 4)),
    (parse_duration("P1Y1M5W"), date(2000, 1, 30), date(2001, 4, 4)),
    (parse_duration("P0.5Y"), date(2000, 1, 30), None),
    (
        Duration(years=1, months=1, hours=3),
        datetime(2000, 1, 30, 12, 15, 00),
        datetime(2001, 2, 28, 15, 15, 00),
    ),
    (
        parse_duration("P1Y1MT3H"),
        datetime(2000, 1, 30, 12, 15, 00),
        datetime(2001, 2, 28, 15, 15, 00),
    ),
    (
        Duration(years=1, months=2),
        timedelta(days=1),
        Duration(years=1, months=2, days=1),
    ),
    (
        timedelta(days=1),
        Duration(years=1, months=2),
        Duration(years=1, months=2, days=1),
    ),
    (datetime(2008, 1, 1, 0, 2), Duration(months=1), datetime(2008, 2, 1, 0, 2)),
    (
        datetime.strptime("200802", "%Y%M"),
        parse_duration("P1M"),
        datetime(2008, 2, 1, 0, 2),
    ),
    (datetime(2008, 2, 1), Duration(months=1), datetime(2008, 3, 1)),
    (datetime.strptime("200802", "%Y%m"), parse_duration("P1M"), datetime(2008, 3, 1)),
    # (date(2000, 1, 1),
    #  Duration(years=1.5),
    #  date(2001, 6, 1)),
    # (date(2000, 1, 1),
    #  Duration(years=1, months=1.5),
    #  date(2001, 2, 14)),
)

# A list of test cases of multiplications of durations
# are compared against a given expected result.
DATE_MUL_TEST_CASES = (
    (Duration(years=1, months=1), 3, Duration(years=3, months=3)),
    (Duration(years=1, months=1), -3, Duration(years=-3, months=-3)),
    (3, Duration(years=1, months=1), Duration(years=3, months=3)),
    (-3, Duration(years=1, months=1), Duration(years=-3, months=-3)),
    (5, Duration(years=2, minutes=40), Duration(years=10, hours=3, minutes=20)),
    (-5, Duration(years=2, minutes=40), Duration(years=-10, hours=-3, minutes=-20)),
    (7, Duration(years=1, months=2, weeks=40), Duration(years=8, months=2, weeks=280)),
)


def test_associative():
    """Adding 2 durations to a date is not associative."""
    days1 = Duration(days=1)
    months1 = Duration(months=1)
    start = date(2000, 3, 30)
    res1 = start + days1 + months1
    res2 = start + months1 + days1
    assert res1 != res2


def test_typeerror():
    """Test if TypError is raised with certain parameters."""
    with pytest.raises(TypeError):
        parse_duration(date(2000, 1, 1))
    with pytest.raises(TypeError):
        Duration(years=1) - date(2000, 1, 1)
    with pytest.raises(TypeError):
        "raise exc" - Duration(years=1)
    with pytest.raises(TypeError):
        Duration(years=1, months=1, weeks=5) + "raise exception"
    with pytest.raises(TypeError):
        "raise exception" + Duration(years=1, months=1, weeks=5)
    with pytest.raises(TypeError):
        Duration(years=1, months=1, weeks=5) * "raise exception"
    with pytest.raises(TypeError):
        "raise exception" * Duration(years=1, months=1, weeks=5)
    with pytest.raises(TypeError):
        Duration(years=1, months=1, weeks=5) * 3.14
    with pytest.raises(TypeError):
        3.14 * Duration(years=1, months=1, weeks=5)


def test_parseerror():
    """Test for unparseable duration string."""
    with pytest.raises(ISO8601Error):
        parse_duration("T10:10:10")


def test_repr():
    """Test __repr__ and __str__ for Duration objects."""
    dur = Duration(10, 10, years=10, months=10)
    assert "10 years, 10 months, 10 days, 0:00:10" == str(dur)
    assert "isodate.duration.Duration(10, 10, 0," " years=10, months=10)" == repr(dur)
    dur = Duration(months=0)
    assert "0:00:00" == str(dur)
    dur = Duration(months=1)
    assert "1 month, 0:00:00" == str(dur)


def test_hash():
    """Test __hash__ for Duration objects."""
    dur1 = Duration(10, 10, years=10, months=10)
    dur2 = Duration(9, 9, years=9, months=9)
    dur3 = Duration(10, 10, years=10, months=10)
    assert hash(dur1) != hash(dur2)
    assert id(dur1) != id(dur2)
    assert hash(dur1) == hash(dur3)
    assert id(dur1) != id(dur3)
    durSet = set()
    durSet.add(dur1)
    durSet.add(dur2)
    durSet.add(dur3)
    assert len(durSet) == 2


def test_neg():
    """Test __neg__ for Duration objects."""
    assert -Duration(0) == Duration(0)
    assert -Duration(years=1, months=1) == Duration(years=-1, months=-1)
    assert -Duration(years=1, months=1) == Duration(months=-13)
    assert -Duration(years=1) != timedelta(days=-365)
    assert -timedelta(days=365) != Duration(years=-1)
    # FIXME: this test fails in python 3... it seems like python3
    #        treats a == b the same b == a
    # assert -timedelta(days=10) != -Duration(days=10)


def test_format():
    """Test various other strftime combinations."""
    assert duration_isoformat(Duration(0)) == "P0D"
    assert duration_isoformat(-Duration(0)) == "P0D"
    assert duration_isoformat(Duration(seconds=10)) == "PT10S"
    assert duration_isoformat(Duration(years=-1, months=-1)) == "-P1Y1M"
    assert duration_isoformat(-Duration(years=1, months=1)) == "-P1Y1M"
    assert duration_isoformat(-Duration(years=-1, months=-1)) == "P1Y1M"
    assert duration_isoformat(-Duration(years=-1, months=-1)) == "P1Y1M"
    dur = Duration(years=3, months=7, days=23, hours=5, minutes=25, milliseconds=330)
    assert duration_isoformat(dur) == "P3Y7M23DT5H25M0.33S"
    assert duration_isoformat(-dur) == "-P3Y7M23DT5H25M0.33S"


def test_equal():
    """Test __eq__ and __ne__ methods."""
    assert Duration(years=1, months=1) == Duration(years=1, months=1)
    assert Duration(years=1, months=1) == Duration(months=13)
    assert Duration(years=1, months=2) != Duration(years=1, months=1)
    assert Duration(years=1, months=1) != Duration(months=14)
    assert Duration(years=1) != timedelta(days=365)
    assert (Duration(years=1, months=1) != Duration(years=1, months=1)) is False
    assert (Duration(years=1, months=1) != Duration(months=13)) is False
    assert (Duration(years=1, months=2) != Duration(years=1, months=1)) is True
    assert (Duration(years=1, months=1) != Duration(months=14)) is True
    assert (Duration(years=1) != timedelta(days=365)) is True
    assert Duration(days=1) == timedelta(days=1)
    # FIXME: this test fails in python 3... it seems like python3
    #        treats a != b the same b != a
    # assert timedelta(days=1) != Duration(days=1)


def test_totimedelta():
    """Test conversion form Duration to timedelta."""
    dur = Duration(years=1, months=2, days=10)
    assert dur.totimedelta(datetime(1998, 2, 25)) == timedelta(434)
    # leap year has one day more in february
    assert dur.totimedelta(datetime(2000, 2, 25)) == timedelta(435)
    dur = Duration(months=2)
    # march is longer than february, but april is shorter than
    # march (cause only one day difference compared to 2)
    assert dur.totimedelta(datetime(2000, 2, 25)) == timedelta(60)
    assert dur.totimedelta(datetime(2001, 2, 25)) == timedelta(59)
    assert dur.totimedelta(datetime(2001, 3, 25)) == timedelta(61)


@pytest.mark.parametrize(
    "durationstring, expectation, format, altstr",
    PARSE_TEST_CASES,
)
def test_parse(durationstring, expectation, format, altstr):
    """Parse an ISO duration string and compare it to the expected value."""
    result = parse_duration(durationstring)
    assert result == expectation


@pytest.mark.parametrize(
    "durationstring, expectation, format, altstr",
    PARSE_TEST_CASES,
)
def test_format_parse(durationstring, expectation, format, altstr):
    """Take duration/timedelta object and create ISO string from it.

    This is the reverse test to test_parse.
    """
    if altstr:
        assert duration_isoformat(expectation, format) == altstr
    else:
        # if durationstring == '-P2W':
        #     import pdb; pdb.set_trace()
        assert duration_isoformat(expectation, format) == durationstring


@pytest.mark.parametrize("dur1, dur2, resadd, ressub, resge", MATH_TEST_CASES)
def test_add(dur1, dur2, resadd, ressub, resge):
    dur1 = parse_duration(dur1)
    dur2 = parse_duration(dur2)
    resadd = parse_duration(resadd)
    assert dur1 + dur2 == resadd


@pytest.mark.parametrize("dur1, dur2, resadd, ressub, resge", MATH_TEST_CASES)
def test_sub(dur1, dur2, resadd, ressub, resge):
    """
    Test operator - (__sub__, __rsub__)
    """
    dur1 = parse_duration(dur1)
    dur2 = parse_duration(dur2)
    ressub = parse_duration(ressub)
    assert dur1 - dur2 == ressub


@pytest.mark.parametrize("dur1, dur2, resadd, ressub, resge", MATH_TEST_CASES)
def test_ge(dur1, dur2, resadd, ressub, resge):
    """Test operator > and <."""
    dur1 = parse_duration(dur1)
    dur2 = parse_duration(dur2)

    def dogetest():
        """Test greater than."""
        return dur1 > dur2

    def doletest():
        """Test less than."""
        return dur1 < dur2

    if resge is None:
        with pytest.raises(TypeError):
            dogetest()
        with pytest.raises(TypeError):
            doletest()
    else:
        assert dogetest() is resge
        assert doletest() is not resge


@pytest.mark.parametrize("start, tdelta, duration", DATE_TEST_CASES)
def test_add_date(start, tdelta, duration):
    assert start + tdelta == start + duration


@pytest.mark.parametrize("start, tdelta, duration", DATE_TEST_CASES)
def test_sub_date(start, tdelta, duration):
    assert start - tdelta == start - duration


@pytest.mark.parametrize("start, duration, expectation", DATE_CALC_TEST_CASES)
def test_calc_date(start, duration, expectation):
    """Test operator +."""
    if expectation is None:
        with pytest.raises(ValueError):
            start + duration
    else:
        assert start + duration == expectation


@pytest.mark.parametrize("operand1, operand2, expectation", DATE_MUL_TEST_CASES)
def test_mul_date(operand1, operand2, expectation):
    """Test operator *."""
    assert operand1 * operand2 == expectation
