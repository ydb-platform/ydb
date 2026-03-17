from operator import attrgetter

import click

from . import VERSION_STR, ExchangeRates, Banks


@click.group()
@click.version_option(version=VERSION_STR)
def entry_point():
    """Tools to query Bank of Russia."""


@entry_point.command()
@click.option('-c', '--currency', help='Currency to get info (e.g. USD, 840)')
@click.option('-d', '--date', help='Date to get rate for (e.g. 2016-06-27)')
def rates(currency, date):
    """Prints out exchange rates."""

    def print_rate(rate):
        click.secho(f'[{rate.code}] {rate.name} - {rate.rate}')

    rates = ExchangeRates(on_date=date, locale_en=True)

    click.secho(rates.date_received.strftime('%Y-%m-%d'))
    click.secho('=' * 10)

    if currency:
        rate = rates[currency]
        if rate:
            print_rate(rate)
        else:
            click.secho(f'No data for {currency}')
    else:
        for rate in sorted(rates.rates, key=attrgetter('code')):
            print_rate(rate)


@entry_point.command()
@click.option('-d', '--date', help='Date to get rate for (e.g. 2018-06-29)')
@click.option('-b', '--bic', help='BIC (Russian or SWIFT) to get information for.')
def banks(date, bic):
    """Prints out banks information."""

    banks = Banks(on_date=date)

    if bic:
        banks = [banks[bic]]

    else:
        banks = banks.banks

    for bank in Banks.annotate(banks):
        click.secho('')
        for title, value in bank.items():
            value and click.secho(f'{title}: {value}')


def main():
    entry_point(obj={})


if __name__ == '__main__':
    main()
