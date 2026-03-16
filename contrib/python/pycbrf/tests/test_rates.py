from pycbrf import ExchangeRates


def test_rates():

    rates = ExchangeRates('2016-06-26', locale_en=True)

    assert str(rates.date_requested) == '2016-06-26 00:00:00'
    assert str(rates.date_received) == '2016-06-25 00:00:00'
    assert not rates.dates_match

    assert rates['dummy'] is None
    assert rates['USD'].name == 'US Dollar'
    assert rates['R01235'].name == 'US Dollar'
    assert rates['840'].name == 'US Dollar'

    rates = ExchangeRates('2016-06-25')

    assert str(rates.date_requested) == '2016-06-25 00:00:00'
    assert str(rates.date_received) == '2016-06-25 00:00:00'
    assert rates.dates_match

    assert rates['USD'].name == 'Доллар США'
    assert rates['R01235'].name == 'Доллар США'
    assert rates['840'].name == 'Доллар США'
