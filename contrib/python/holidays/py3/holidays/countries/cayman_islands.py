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

from holidays import APR, MAY, JUN, JUL, SEP, NOV, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class CaymanIslands(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Cayman Islands holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Cayman_Islands>
        * [Public Holidays Law (2007 Revision)](https://web.archive.org/web/20250227060525/https://legislation.gov.ky/cms/images/LEGISLATION/PRINCIPAL/1964/1964-0140/PublicHolidaysAct_2007%20Revision_g.pdf)
        * [Public Holidays Order, 2024](https://web.archive.org/web/20240518181823/https://legislation.gov.ky/cms/images/LEGISLATION/AMENDING/2024/2024-O004/PublicHolidaysOrder2024SL4of2024.pdf)
        * [2006-2015](https://web.archive.org/web/20151014061601/http://www.gov.ky/portal/page?_pageid=1142,1592653&_dad=portal&_schema=PORTAL)
        * [2016-2018](https://web.archive.org/web/20180330170202/http://www.gov.ky:80/portal/page/portal/cighome/cayman/islands/publicholidays)
        * [2021](http://archive.today/2025.07.09-033240/https://www.gov.ky/news/press-release-details/public-holidays-for-2021)
        * [2022](http://archive.today/2025.07.09-033515/https://www.gov.ky/news/press-release-details/public-holidays-2022)
        * [2024](http://archive.today/2025.01.06-110234/https://www.gov.ky/calendar/public-holidays)
        * [2025](http://archive.today/2025.07.09-033853/https://www.gov.ky/calendar/public-holidays)
    """

    country = "KY"
    default_language = "en_GB"
    # %s observed.
    observed_label = tr("%s (observed)")
    # Earliest year of holidays with an accessible online record.
    start_year = 2006
    supported_languages = ("en_GB", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=CaymanIslandsStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # National Heroes Day.
        self._add_holiday_4th_mon_of_jan(tr("National Heroes Day"))

        # Ash Wednesday.
        self._add_ash_wednesday(tr("Ash Wednesday"))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        if self._year >= 2024:
            # Emancipation Day.
            self._add_holiday_1st_mon_of_may(tr("Emancipation Day"))

        # Discovery Day.
        self._add_holiday_3rd_mon_of_may(tr("Discovery Day"))

        if self._year <= 2022:
            queens_birthday_dates = {
                2007: (JUN, 18),
                2012: (JUN, 18),
                2013: (JUN, 17),
                2017: (JUN, 19),
                2022: (JUN, 6),
            }
            # Queen's Birthday.
            name = tr("Queen's Birthday")
            if dt := queens_birthday_dates.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_holiday_2_days_past_2nd_sat_of_jun(name)
        else:
            # King's Birthday.
            self._add_holiday_2_days_past_3rd_sat_of_jun(tr("King's Birthday"))

        # Constitution Day.
        self._add_holiday_1st_mon_of_jul(tr("Constitution Day"))

        # Remembrance Day.
        self._add_holiday_2nd_mon_of_nov(tr("Remembrance Day"))

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


class KY(CaymanIslands):
    pass


class CYM(CaymanIslands):
    pass


class CaymanIslandsStaticHolidays:
    """Cayman Islands special holidays.

    References:
        * [Public Holidays Order, 2009](https://archive.org/details/public-holidays-order-2009-sl-33-of-2009)
        * [Public Holidays Order (No. 2), 2019](https://archive.org/details/public-holidays-no.-2-order-2019-sl-38-of-2019)
        * [Public Holidays Order, 2025](https://archive.org/details/public-holidays-order-2025-sl-15-of-2025)
        * [Referendum Day 2019](https://web.archive.org/web/20250711121821/https://www.facebook.com/ElectionsOffice/posts/reminder-the-referendum-vote-has-been-postponed-and-will-no-longer-take-place-th/2619995141431734/?_rdc=2&_rdr)
        * [UK Royal Wedding](https://en.wikipedia.org/wiki/Wedding_of_Prince_William_and_Catherine_Middleton)
        * [Queen Elizabeth II's Diamond Jubilee](https://web.archive.org/web/20210803202236/https://www.caymancompass.com/2012/06/06/queens-diamond-jubilee-feted/)
        * [Queen Elizabeth II's Funeral](https://web.archive.org/web/20231226055510/https://www.caymancompass.com/2022/09/12/cayman-declares-public-holiday-for-queens-funeral/)
        * [King Charles III's Coronation](https://web.archive.org/web/20250601214328/https://www.radiocayman.gov.ky/news/public-holidays-for-2023-unconfirmed)
    """

    # Referendum Day.
    referendum_day_name = tr("Referendum Day")
    # General Election Day.
    general_election_day_name = tr("General Election Day")
    special_public_holidays = {
        2009: (
            # 2009 Cayman Islands Constitution Day.
            (NOV, 6, tr("2009 Cayman Islands Constitution Day")),
            (MAY, 20, general_election_day_name),
        ),
        # UK Royal Wedding.
        2011: (APR, 29, tr("UK Royal Wedding")),
        2012: (
            # Queen Elizabeth II's Diamond Jubilee.
            (JUN, 4, tr("Queen Elizabeth II's Diamond Jubilee")),
            (JUL, 18, referendum_day_name),
        ),
        2013: (MAY, 22, general_election_day_name),
        2017: (MAY, 24, general_election_day_name),
        2019: (DEC, 19, referendum_day_name),
        2021: (APR, 14, general_election_day_name),
        # Queen Elizabeth II's Funeral.
        2022: (SEP, 19, tr("Queen Elizabeth II's Funeral")),
        # King Charles III's Coronation.
        2023: (MAY, 8, tr("Coronation of His Majesty King Charles III")),
        2025: (APR, 30, general_election_day_name),
    }
