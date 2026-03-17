from datetime import time

import pytest

import babel.dates as dates


@pytest.mark.parametrize("locale, time, expected_period_id", [
    ("de", time(7, 42), "morning1"),  # (from, before)
    ("de", time(3, 11), "night1"),  # (after, before)
    ("fi", time(0), "midnight"),  # (at)
    ("en_US", time(12), "noon"),  # (at)
    ("en_US", time(21), "night1"),  # (from, before) across 0:00
    ("en_US", time(5), "morning1"),  # (from, before) across 0:00
    ("en_US", time(6), "morning1"),  # (from, before)
    ("agq", time(10), "am"),  # no periods defined
    ("agq", time(22), "pm"),  # no periods defined
    ("am", time(14), "afternoon1"),  # (before, after)
])
def test_day_period_rules(locale, time, expected_period_id):
    assert dates.get_period_id(time, locale=locale) == expected_period_id
