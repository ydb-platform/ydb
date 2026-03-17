"""
These tests do not verify any results and should not be run when
looking at improving test coverage.  They just verify that basic
operations don't fail due to odd corner cases on any locale that
we ship.
"""
import datetime
import decimal

import pytest

from babel import Locale, dates, numbers, units

NUMBERS = (
    decimal.Decimal("-33.76"),  # Negative Decimal
    decimal.Decimal("13.37"),  # Positive Decimal
    1.2 - 1.0,  # Inaccurate float
    10,  # Plain old integer
    0,  # Zero
    1000,  # A thousand (previously raised KeyError in the nl locale for compact currencies)
)


@pytest.mark.all_locales
def test_smoke_dates(locale):
    locale = Locale.parse(locale)
    instant = datetime.datetime.now()
    for width in ("full", "long", "medium", "short"):
        assert dates.format_date(instant, format=width, locale=locale)
        assert dates.format_datetime(instant, format=width, locale=locale)
        assert dates.format_time(instant, format=width, locale=locale)
    # Interval test
    past = instant - datetime.timedelta(hours=23)
    assert dates.format_interval(past, instant, locale=locale)
    # Duration test - at the time of writing, all locales seem to have `short` width,
    # so let's test that.
    duration = instant - instant.replace(hour=0, minute=0, second=0)
    for granularity in ('second', 'minute', 'hour', 'day'):
        assert dates.format_timedelta(duration, granularity=granularity, format="short", locale=locale)


@pytest.mark.all_locales
def test_smoke_numbers(locale):
    locale = Locale.parse(locale)
    for number in NUMBERS:
        assert numbers.format_decimal(number, locale=locale)
        assert numbers.format_decimal(number, locale=locale, numbering_system="default")
        assert numbers.format_currency(number, "EUR", locale=locale)
        assert numbers.format_currency(number, "EUR", locale=locale, numbering_system="default")
        assert numbers.format_compact_currency(number, "EUR", locale=locale)
        assert numbers.format_compact_currency(number, "EUR", locale=locale, numbering_system="default")
        assert numbers.format_scientific(number, locale=locale)
        assert numbers.format_scientific(number, locale=locale, numbering_system="default")
        assert numbers.format_percent(number / 100, locale=locale)
        assert numbers.format_percent(number / 100, locale=locale, numbering_system="default")


@pytest.mark.all_locales
def test_smoke_units(locale):
    locale = Locale.parse(locale)
    for unit in ('length-meter', 'mass-kilogram', 'energy-calorie', 'volume-liter'):
        for number in NUMBERS:
            assert units.format_unit(number, measurement_unit=unit, locale=locale)
            assert units.format_unit(number, measurement_unit=unit, locale=locale, numbering_system="default")
