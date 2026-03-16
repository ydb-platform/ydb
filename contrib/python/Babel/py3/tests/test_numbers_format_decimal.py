#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2025 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at https://github.com/python-babel/babel/blob/master/LICENSE.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at https://github.com/python-babel/babel/commits/master/.


import decimal

from babel import numbers


def test_patterns():
    assert numbers.format_decimal(12345, '##0', locale='en_US') == '12345'
    assert numbers.format_decimal(6.5, '0.00', locale='sv') == '6,50'
    assert numbers.format_decimal((10.0 ** 20), '#.00', locale='en_US') == '100000000000000000000.00'
    # regression test for #183, fraction digits were not correctly cut
    # if the input was a float value and the value had more than 7
    # significant digits
    assert numbers.format_decimal(12345678.051, '#,##0.00', locale='en_US') == '12,345,678.05'


def test_subpatterns():
    assert numbers.format_decimal((- 12345), '#,##0.##;-#', locale='en_US') == '-12,345'
    assert numbers.format_decimal((- 12345), '#,##0.##;(#)', locale='en_US') == '(12,345)'


def test_default_rounding():
    """
    Testing Round-Half-Even (Banker's rounding)

    A '5' is rounded to the closest 'even' number
    """
    assert numbers.format_decimal(5.5, '0', locale='sv') == '6'
    assert numbers.format_decimal(6.5, '0', locale='sv') == '6'
    assert numbers.format_decimal(1.2325, locale='sv') == '1,232'
    assert numbers.format_decimal(1.2335, locale='sv') == '1,234'


def test_significant_digits():
    """Test significant digits patterns"""
    assert numbers.format_decimal(123004, '@@', locale='en_US') == '120000'
    assert numbers.format_decimal(1.12, '@', locale='sv') == '1'
    assert numbers.format_decimal(1.1, '@@', locale='sv') == '1,1'
    assert numbers.format_decimal(1.1, '@@@@@##', locale='sv') == '1,1000'
    assert numbers.format_decimal(0.0001, '@@@', locale='sv') == '0,000100'
    assert numbers.format_decimal(0.0001234, '@@@', locale='sv') == '0,000123'
    assert numbers.format_decimal(0.0001234, '@@@#', locale='sv') == '0,0001234'
    assert numbers.format_decimal(0.0001234, '@@@#', locale='sv') == '0,0001234'
    assert numbers.format_decimal(0.12345, '@@@', locale='sv') == '0,123'
    assert numbers.format_decimal(3.14159, '@@##', locale='sv') == '3,142'
    assert numbers.format_decimal(1.23004, '@@##', locale='sv') == '1,23'
    assert numbers.format_decimal(1230.04, '@@,@@', locale='en_US') == '12,30'
    assert numbers.format_decimal(123.41, '@@##', locale='en_US') == '123.4'
    assert numbers.format_decimal(1, '@@', locale='en_US') == '1.0'
    assert numbers.format_decimal(0, '@', locale='en_US') == '0'
    assert numbers.format_decimal(0.1, '@', locale='en_US') == '0.1'
    assert numbers.format_decimal(0.1, '@#', locale='en_US') == '0.1'
    assert numbers.format_decimal(0.1, '@@', locale='en_US') == '0.10'


def test_decimals():
    """Test significant digits patterns"""
    assert numbers.format_decimal(decimal.Decimal('1.2345'), '#.00', locale='en_US') == '1.23'
    assert numbers.format_decimal(decimal.Decimal('1.2345000'), '#.00', locale='en_US') == '1.23'
    assert numbers.format_decimal(decimal.Decimal('1.2345000'), '@@', locale='en_US') == '1.2'
    assert numbers.format_decimal(decimal.Decimal('12345678901234567890.12345'), '#.00', locale='en_US') == '12345678901234567890.12'


def test_scientific_notation():
    assert numbers.format_scientific(0.1, '#E0', locale='en_US') == '1E-1'
    assert numbers.format_scientific(0.01, '#E0', locale='en_US') == '1E-2'
    assert numbers.format_scientific(10, '#E0', locale='en_US') == '1E1'
    assert numbers.format_scientific(1234, '0.###E0', locale='en_US') == '1.234E3'
    assert numbers.format_scientific(1234, '0.#E0', locale='en_US') == '1.2E3'
    # Exponent grouping
    assert numbers.format_scientific(12345, '##0.####E0', locale='en_US') == '1.2345E4'
    # Minimum number of int digits
    assert numbers.format_scientific(12345, '00.###E0', locale='en_US') == '12.345E3'
    assert numbers.format_scientific(-12345.6, '00.###E0', locale='en_US') == '-12.346E3'
    assert numbers.format_scientific(-0.01234, '00.###E0', locale='en_US') == '-12.34E-3'
    # Custom pattern suffix
    assert numbers.format_scientific(123.45, '#.##E0 m/s', locale='en_US') == '1.23E2 m/s'
    # Exponent patterns
    assert numbers.format_scientific(123.45, '#.##E00 m/s', locale='en_US') == '1.23E02 m/s'
    assert numbers.format_scientific(0.012345, '#.##E00 m/s', locale='en_US') == '1.23E-02 m/s'
    assert numbers.format_scientific(decimal.Decimal('12345'), '#.##E+00 m/s', locale='en_US') == '1.23E+04 m/s'
    # 0 (see ticket #99)
    assert numbers.format_scientific(0, '#E0', locale='en_US') == '0E0'


def test_formatting_of_very_small_decimals():
    # previously formatting very small decimals could lead to a type error
    # because the Decimal->string conversion was too simple (see #214)
    number = decimal.Decimal("7E-7")
    assert numbers.format_decimal(number, format="@@@", locale='en_US') == '0.000000700'


def test_nan_and_infinity():
    assert numbers.format_decimal(decimal.Decimal('Infinity'), locale='en_US') == '∞'
    assert numbers.format_decimal(decimal.Decimal('-Infinity'), locale='en_US') == '-∞'
    assert numbers.format_decimal(decimal.Decimal('NaN'), locale='en_US') == 'NaN'
    assert numbers.format_compact_decimal(decimal.Decimal('Infinity'), locale='en_US', format_type="short") == '∞'
    assert numbers.format_compact_decimal(decimal.Decimal('-Infinity'), locale='en_US', format_type="short") == '-∞'
    assert numbers.format_compact_decimal(decimal.Decimal('NaN'), locale='en_US', format_type="short") == 'NaN'
    assert numbers.format_currency(decimal.Decimal('Infinity'), 'USD', locale='en_US') == '$∞'
    assert numbers.format_currency(decimal.Decimal('-Infinity'), 'USD', locale='en_US') == '-$∞'


def test_group_separator():
    assert numbers.format_decimal(29567.12, locale='en_US', group_separator=False) == '29567.12'
    assert numbers.format_decimal(29567.12, locale='fr_CA', group_separator=False) == '29567,12'
    assert numbers.format_decimal(29567.12, locale='pt_BR', group_separator=False) == '29567,12'
    assert numbers.format_currency(1099.98, 'USD', locale='en_US', group_separator=False) == '$1099.98'
    assert numbers.format_currency(101299.98, 'EUR', locale='fr_CA', group_separator=False) == '101299,98\xa0€'
    assert numbers.format_currency(101299.98, 'EUR', locale='en_US', group_separator=False, format_type='name') == '101299.98 euros'
    assert numbers.format_percent(251234.1234, locale='sv_SE', group_separator=False) == '25123412\xa0%'

    assert numbers.format_decimal(29567.12, locale='en_US', group_separator=True) == '29,567.12'
    assert numbers.format_decimal(29567.12, locale='fr_CA', group_separator=True) == '29\xa0567,12'
    assert numbers.format_decimal(29567.12, locale='pt_BR', group_separator=True) == '29.567,12'
    assert numbers.format_currency(1099.98, 'USD', locale='en_US', group_separator=True) == '$1,099.98'
    assert numbers.format_currency(101299.98, 'EUR', locale='fr_CA', group_separator=True) == '101\xa0299,98\xa0€'
    assert numbers.format_currency(101299.98, 'EUR', locale='en_US', group_separator=True, format_type='name') == '101,299.98 euros'
    assert numbers.format_percent(251234.1234, locale='sv_SE', group_separator=True) == '25\xa0123\xa0412\xa0%'


def test_compact():
    assert numbers.format_compact_decimal(1, locale='en_US', format_type="short") == '1'
    assert numbers.format_compact_decimal(999, locale='en_US', format_type="short") == '999'
    assert numbers.format_compact_decimal(1000, locale='en_US', format_type="short") == '1K'
    assert numbers.format_compact_decimal(9000, locale='en_US', format_type="short") == '9K'
    assert numbers.format_compact_decimal(9123, locale='en_US', format_type="short", fraction_digits=2) == '9.12K'
    assert numbers.format_compact_decimal(10000, locale='en_US', format_type="short") == '10K'
    assert numbers.format_compact_decimal(10000, locale='en_US', format_type="short", fraction_digits=2) == '10K'
    assert numbers.format_compact_decimal(1000000, locale='en_US', format_type="short") == '1M'
    assert numbers.format_compact_decimal(9000999, locale='en_US', format_type="short") == '9M'
    assert numbers.format_compact_decimal(9000900099, locale='en_US', format_type="short", fraction_digits=5) == '9.0009B'
    assert numbers.format_compact_decimal(1, locale='en_US', format_type="long") == '1'
    assert numbers.format_compact_decimal(999, locale='en_US', format_type="long") == '999'
    assert numbers.format_compact_decimal(1000, locale='en_US', format_type="long") == '1 thousand'
    assert numbers.format_compact_decimal(9000, locale='en_US', format_type="long") == '9 thousand'
    assert numbers.format_compact_decimal(9000, locale='en_US', format_type="long", fraction_digits=2) == '9 thousand'
    assert numbers.format_compact_decimal(10000, locale='en_US', format_type="long") == '10 thousand'
    assert numbers.format_compact_decimal(10000, locale='en_US', format_type="long", fraction_digits=2) == '10 thousand'
    assert numbers.format_compact_decimal(1000000, locale='en_US', format_type="long") == '1 million'
    assert numbers.format_compact_decimal(9999999, locale='en_US', format_type="long") == '10 million'
    assert numbers.format_compact_decimal(9999999999, locale='en_US', format_type="long", fraction_digits=5) == '10 billion'
    assert numbers.format_compact_decimal(1, locale='ja_JP', format_type="short") == '1'
    assert numbers.format_compact_decimal(999, locale='ja_JP', format_type="short") == '999'
    assert numbers.format_compact_decimal(1000, locale='ja_JP', format_type="short") == '1000'
    assert numbers.format_compact_decimal(9123, locale='ja_JP', format_type="short") == '9123'
    assert numbers.format_compact_decimal(10000, locale='ja_JP', format_type="short") == '1万'
    assert numbers.format_compact_decimal(1234567, locale='ja_JP', format_type="short") == '123万'
    assert numbers.format_compact_decimal(-1, locale='en_US', format_type="short") == '-1'
    assert numbers.format_compact_decimal(-1234, locale='en_US', format_type="short", fraction_digits=2) == '-1.23K'
    assert numbers.format_compact_decimal(-123456789, format_type='short', locale='en_US') == '-123M'
    assert numbers.format_compact_decimal(-123456789, format_type='long', locale='en_US') == '-123 million'
    assert numbers.format_compact_decimal(2345678, locale='mk', format_type='long') == '2 милиони'
    assert numbers.format_compact_decimal(21000000, locale='mk', format_type='long') == '21 милион'
    assert numbers.format_compact_decimal(21345, locale="gv", format_type="short") == '21K'
    assert numbers.format_compact_decimal(1000, locale='it', format_type='long') == 'mille'
    assert numbers.format_compact_decimal(1234, locale='it', format_type='long') == '1 mila'
    assert numbers.format_compact_decimal(1000, locale='fr', format_type='long') == 'mille'
    assert numbers.format_compact_decimal(1234, locale='fr', format_type='long') == '1 millier'
    assert numbers.format_compact_decimal(
        12345, format_type="short", locale='ar_EG', fraction_digits=2, numbering_system='default',
    ) == '12٫34\xa0ألف'
    assert numbers.format_compact_decimal(
        12345, format_type="short", locale='ar_EG', fraction_digits=2, numbering_system='latn',
    ) == '12.34\xa0ألف'
