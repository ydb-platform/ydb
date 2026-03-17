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

from gettext import gettext as tr

from holidays.calendars.gregorian import MAY, JUN, OCT, NOV
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class Bermuda(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Bermuda holidays.

    References:
        * [Public Holidays Act 1947](https://web.archive.org/web/20250527163956/https://www.bermudalaws.bm/Laws/Consolidated%20Law/1947/Public%20Holidays%20Act%201947)
        * [Public Holidays Amendment Act 1999](https://web.archive.org/web/20250527163749/https://www.bermudalaws.bm/Laws/Annual%20Law/Acts/1999/Public%20Holidays%20Amendment%20Act%201999)
        * [Public Holidays Amendment Act 2008](https://web.archive.org/web/20250527163810/https://www.bermudalaws.bm/Laws/Annual%20Law/Acts/2008/Public%20Holidays%20Amendment%20Act%202008)
        * [Public Holidays Amendment (No. 2) Act 2009](https://web.archive.org/web/20250527163816/https://www.bermudalaws.bm/Laws/Annual%20Law/Acts/2009/Public%20Holidays%20Amendment%20(No.%202)%20Act%202009)
        * [Public Holidays Amendment Act 2017](https://web.archive.org/web/20250527163819/https://www.bermudalaws.bm/Laws/Annual%20Law/Acts/2017/Public%20Holidays%20Amendment%20Act%202017)
        * [Public Holidays Amendment Act 2020](https://web.archive.org/web/20250527163836/https://www.bermudalaws.bm/Laws/Annual%20Law/Acts/2020/Public%20Holidays%20Amendment%20Act%202020)
        * [Government of Bermuda](https://web.archive.org/web/20250530055854/https://www.gov.bm/public-holidays)
        * [Proclamation BR 149 / 2018](https://web.archive.org/web/20250530114331/https://www.bermudalaws.bm/Laws/Annual%20Law/Statutory%20Instruments/2018/Proclamation%20-%20Bermuda%20Day%20Public%20Holiday)
    """

    country = "BM"
    default_language = "en_BM"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_BM", "en_US")
    start_year = 1948

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, BermudaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Bermuda Day.
        name = tr("Bermuda Day")
        if self._year <= 2017:
            self._add_observed(self._add_holiday_may_24(name))
        elif self._year == 2019:
            self._add_holiday_may_24(name)
        elif self._year <= 2020:
            self._add_holiday_last_fri_of_may(name)
        else:
            self._add_holiday_3_days_prior_last_mon_of_may(name)

        # Queen's Birthday.
        name = tr("Queen's Birthday")
        if self._year <= 1999:
            self._add_holiday_3rd_mon_of_jun(name)
        elif self._year <= 2008:
            self._add_holiday_2_days_past_2nd_sat_of_jun(name)

        # National Heroes Day.
        name = tr("National Heroes Day")
        if self._year == 2008:
            self._add_holiday_2nd_mon_of_oct(name)
        elif self._year >= 2009:
            self._add_holiday_3rd_mon_of_jun(name)

        self._add_holiday_4_days_prior_1st_mon_of_aug(
            # Cup Match Day.
            tr("Cup Match Day")
            if self._year <= 1999
            # Emancipation Day.
            else tr("Emancipation Day")
        )

        self._add_holiday_3_days_prior_1st_mon_of_aug(
            # Somers Day.
            tr("Somers Day")
            if self._year <= 2019
            # Mary Prince Day.
            else tr("Mary Prince Day")
        )

        # Labor Day.
        self._add_holiday_1st_mon_of_sep(tr("Labour Day"))

        # Remembrance Day.
        self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )


class BM(Bermuda):
    pass


class BMU(Bermuda):
    pass


class BermudaStaticHolidays:
    """Bermuda special holidays.

    References:
        * [June 5, 2007 Holiday](https://web.archive.org/web/20250530122818/https://www.bermudalaws.bm/Laws/Annual%20Law/Statutory%20Instruments/2007/Proclamation)
        * Portuguese Welcome 170th Anniversary
            * <https://web.archive.org/web/20241210201648/https://www.royalgazette.com/other/news/article/20191025/portuguese-welcome-170th-anniversary-holiday/>
            * <https://web.archive.org/web/20250530122008/https://www.bermudalaws.bm/Laws/Annual%20Law/Statutory%20Instruments/2018/Proclamation%20-%20Portguese%20Public%20Holiday>
        * Flora Duffy Day
            * <https://web.archive.org/web/20240613083451/https://bernews.com/2021/10/governor-signs-flora-duffy-day-proclamation/>
            * <https://web.archive.org/web/20250530113359/https://www.bermudalaws.bm/Laws/Annual%20Law/Statutory%20Instruments/2021/Proclamation%20for%2018%20October%202021%20Holiday>
        * The Coronation of His Majesty King Charles III Holiday
            * <https://web.archive.org/web/20250222035055/https://gov.bm/articles/coronation-his-majesty-king-charles-iii-her-majesty-queen-consort>
            * <https://web.archive.org/web/20250530112129/https://www.bermudalaws.bm/Laws/Annual%20Law/Statutory%20Instruments/2023/Proclamation%20for%208%20May%202023%20Holiday>
    """

    special_public_holidays = {
        # Public Holiday.
        2007: (JUN, 5, tr("Public Holiday")),
        # Portuguese Welcome 170th Anniversary.
        2019: (NOV, 4, tr("Portuguese Welcome 170th Anniversary")),
        # Flora Duffy Day.
        2021: (OCT, 18, tr("Flora Duffy Day")),
        # The Coronation of His Majesty King Charles III.
        2023: (MAY, 8, tr("The Coronation of His Majesty King Charles III")),
    }
