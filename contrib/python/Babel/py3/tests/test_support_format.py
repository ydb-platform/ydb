import datetime
from decimal import Decimal

import pytest

from babel import support


@pytest.fixture
def ar_eg_format() -> support.Format:
    return support.Format('ar_EG', numbering_system="default")


@pytest.fixture
def en_us_format(timezone_getter) -> support.Format:
    return support.Format('en_US', tzinfo=timezone_getter('US/Eastern'))


def test_format_datetime(en_us_format):
    when = datetime.datetime(2007, 4, 1, 15, 30)
    assert en_us_format.datetime(when) == 'Apr 1, 2007, 11:30:00\u202fAM'


def test_format_time(en_us_format):
    when = datetime.datetime(2007, 4, 1, 15, 30)
    assert en_us_format.time(when) == '11:30:00\u202fAM'


def test_format_number(ar_eg_format, en_us_format):
    assert en_us_format.number(1234) == '1,234'
    assert ar_eg_format.number(1234) == '1٬234'


def test_format_decimal(ar_eg_format, en_us_format):
    assert en_us_format.decimal(1234.5) == '1,234.5'
    assert en_us_format.decimal(Decimal("1234.5")) == '1,234.5'
    assert ar_eg_format.decimal(1234.5) == '1٬234٫5'
    assert ar_eg_format.decimal(Decimal("1234.5")) == '1٬234٫5'


def test_format_compact_decimal(ar_eg_format, en_us_format):
    assert en_us_format.compact_decimal(1234) == '1K'
    assert ar_eg_format.compact_decimal(1234, fraction_digits=1) == '1٫2\xa0ألف'
    assert ar_eg_format.compact_decimal(Decimal("1234"), fraction_digits=1) == '1٫2\xa0ألف'


def test_format_currency(ar_eg_format, en_us_format):
    assert en_us_format.currency(1099.98, 'USD') == '$1,099.98'
    assert en_us_format.currency(Decimal("1099.98"), 'USD') == '$1,099.98'
    assert ar_eg_format.currency(1099.98, 'EGP') == '\u200f1٬099٫98\xa0ج.م.\u200f'


def test_format_compact_currency(ar_eg_format, en_us_format):
    assert en_us_format.compact_currency(1099.98, 'USD') == '$1K'
    assert en_us_format.compact_currency(Decimal("1099.98"), 'USD') == '$1K'
    assert ar_eg_format.compact_currency(1099.98, 'EGP') == '1\xa0ألف\xa0ج.م.\u200f'


def test_format_percent(ar_eg_format, en_us_format):
    assert en_us_format.percent(0.34) == '34%'
    assert en_us_format.percent(Decimal("0.34")) == '34%'
    assert ar_eg_format.percent(134.5) == '13٬450%'


def test_format_scientific(ar_eg_format, en_us_format):
    assert en_us_format.scientific(10000) == '1E4'
    assert en_us_format.scientific(Decimal("10000")) == '1E4'
    assert ar_eg_format.scientific(10000) == '1أس4'
