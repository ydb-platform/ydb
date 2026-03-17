from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import NamedTuple, Union, List, Optional
from xml.etree import ElementTree

from .utils import WithRequests

LOG = getLogger(__name__)
URL_BASE = 'http://www.cbr.ru/scripts/'


class ExchangeRate(NamedTuple):
    """Represents an exchange rate for a currency.

    Such objects will populate ExchangeRates().rates

    """
    id: str
    name: str
    code: str
    num: str
    value: Decimal
    par: Decimal
    rate: Decimal


class ExchangeRates(WithRequests):

    def __init__(self, on_date: Union[datetime, str] = None, locale_en: bool = False):
        """Fetches exchange rates.

        rates = ExchangeRates('2016-06-26', locale_en=True)

        Various indexing is supported:

        rates['USD']  # By ISO alpha code
        rates['R01235']  # By internal Bank of Russia code
        rates['840']  # By ISO numeric code.

        :param on_date: Date to get rates for.
            Python date objects and ISO date string are supported.
            If not set rates on latest available date will be returned (usually tomorrow).

        :param locale_en: Flag to get currency names in English.
            If not set names will be provided in Russian.

        """
        if isinstance(on_date, str):
            on_date = datetime.strptime(on_date, '%Y-%m-%d')

        self.dates_match: bool = False
        """
        Flag indicating whether rates were returned on exact same date as requested.
        Note that Bank of Russia won't issue different rates for every day of weekend.

        """

        raw_data = self._get_data(on_date, locale_en)
        parsed = self._parse(raw_data)

        self.date_requested = on_date
        """Date requested by user."""

        self.date_received: datetime = parsed['date']
        """Date returned by Bank of Russia."""

        if on_date is None:
            self.date_requested = self.date_received

        self.rates: List['ExchangeRate'] = parsed['rates']
        """Rates fetched from server as a list."""

        self.dates_match: bool = (self.date_requested == self.date_received)

    def __getitem__(self, item: str) -> Optional['ExchangeRate']:

        if item.isdigit():
            key = 'num'

        elif item.isalpha():
            key = 'code'

        else:
            key = 'id'

        indexed = {getattr(currency, key): currency for currency in self.rates}

        return indexed.get(item)

    @staticmethod
    def _parse(data):
        LOG.debug('Parsing data ...')

        xml = ElementTree.fromstring(data)
        meta = xml.attrib

        result = {
            'date': datetime.strptime(meta['Date'], '%d.%m.%Y'),
            'rates': [],
        }

        for currency in xml:
            props = {}
            for prop in currency:
                props[prop.tag] = prop.text

            par = Decimal(props['Nominal'])
            par_value = Decimal(props['Value'].replace(',', '.'))

            result['rates'].append(ExchangeRate(
                id=currency.attrib['ID'],
                name=props['Name'],
                code=props['CharCode'],
                num=props['NumCode'],
                value=par_value,
                par=par,
                rate=par_value / par,
            ))

        LOG.debug(f"Parsed: {len(result['rates'])} currencies")

        return result

    @classmethod
    def _get_data(cls, on_date: datetime = None, locale_en: bool = False) -> bytes:

        url = f"{URL_BASE}XML_daily{'_eng' if locale_en else ''}.asp"

        if on_date:
            url = f"{url}?date_req={on_date.strftime('%d/%m/%Y')}"

        LOG.debug(f'Getting exchange rates from {url} ...')

        response = cls._get_response(url)
        data = response.content

        return data
