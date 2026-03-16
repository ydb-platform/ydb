# -- encoding: UTF-8 --
from datetime import time

import babel.dates as dates
import pytest


@pytest.mark.parametrize("locale, time, expected_period_id", [
    ("de", time(7, 42), "morning1"),  # (from, before)
    ("de", time(3, 11), "night1"),  # (after, before)
    ("fi", time(0), "midnight"),  # (at)
    ("en_US", time(12), "noon"),  # (at)
    ("agq", time(10), "am"),  # no periods defined
    ("agq", time(22), "pm"),  # no periods defined
    ("am", time(14), "afternoon1"),  # (before, after)
])
def test_day_period_rules(locale, time, expected_period_id):
    assert dates.get_period_id(time, locale=locale) == expected_period_id
