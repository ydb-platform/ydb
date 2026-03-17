#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

from holidays.financial.national_stock_exchange_of_india import NationalStockExchangeOfIndia


class BombayStockExchange(NationalStockExchangeOfIndia):
    """Bombay Stock Exchange (BSE) holidays.

    References:
        * <https://web.archive.org/web/20251226065205/https://www.bseindia.com/static/markets/marketinfo/listholi.aspx>
    """

    market = "XBOM"
    parent_entity = NationalStockExchangeOfIndia
    start_year = 2001


class XBOM(BombayStockExchange):
    pass


class BSE(BombayStockExchange):
    pass
