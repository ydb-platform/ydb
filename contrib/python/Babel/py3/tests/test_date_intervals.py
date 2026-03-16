import datetime

from babel import dates
from babel.util import UTC

TEST_DT = datetime.datetime(2016, 1, 8, 11, 46, 15)
TEST_TIME = TEST_DT.time()
TEST_DATE = TEST_DT.date()


def test_format_interval_same_instant_1():
    assert dates.format_interval(TEST_DT, TEST_DT, "yMMMd", fuzzy=False, locale="fi") == "8.1.2016"


def test_format_interval_same_instant_2():
    assert dates.format_interval(TEST_DT, TEST_DT, "xxx", fuzzy=False, locale="fi") == "8.1.2016 11.46.15"


def test_format_interval_same_instant_3():
    assert dates.format_interval(TEST_TIME, TEST_TIME, "xxx", fuzzy=False, locale="fi") == "11.46.15"


def test_format_interval_same_instant_4():
    assert dates.format_interval(TEST_DATE, TEST_DATE, "xxx", fuzzy=False, locale="fi") == "8.1.2016"


def test_format_interval_no_difference():
    t1 = TEST_DT
    t2 = t1 + datetime.timedelta(minutes=8)
    assert dates.format_interval(t1, t2, "yMd", fuzzy=False, locale="fi") == "8.1.2016"


def test_format_interval_in_tz(timezone_getter):
    t1 = TEST_DT.replace(tzinfo=UTC)
    t2 = t1 + datetime.timedelta(minutes=18)
    hki_tz = timezone_getter("Europe/Helsinki")
    assert dates.format_interval(t1, t2, "Hmv", tzinfo=hki_tz, locale="fi") == "13.46\u201314.04 aikavyöhyke: Suomi"


def test_format_interval_12_hour():
    t2 = TEST_DT
    t1 = t2 - datetime.timedelta(hours=1)
    assert dates.format_interval(t1, t2, "hm", locale="en") == "10:46\u2009\u2013\u200911:46\u202fAM"


def test_format_interval_invalid_skeleton():
    t1 = TEST_DATE
    t2 = TEST_DATE + datetime.timedelta(days=1)
    assert dates.format_interval(t1, t2, "mumumu", fuzzy=False, locale="fi") == "8.1.2016\u20139.1.2016"
    assert dates.format_interval(t1, t2, fuzzy=False, locale="fi") == "8.1.2016\u20139.1.2016"


def test_issue_825():
    assert dates.format_timedelta(
        datetime.timedelta(hours=1),
        granularity='hour',
        threshold=100,
        format='short',
        locale='pt',
    ) == '1 h'
