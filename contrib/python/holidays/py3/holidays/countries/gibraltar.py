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

from holidays.calendars.gregorian import JAN, FEB, APR, MAY, JUN, AUG, SEP
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class Gibraltar(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Gibraltar holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Gibraltar>
        * [2000](https://web.archive.org/web/20250714134510/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/1999s073.pdf)
        * [2001](https://web.archive.org/web/20250714134609/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2000s041.pdf)
        * [2003](https://web.archive.org/web/20250713064759/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2002s094.pdf)
        * [2004](https://web.archive.org/web/20250713065008/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2003s105.pdf)
        * [2005](https://web.archive.org/web/20250713071914/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2004s069.pdf)
        * [2006](https://web.archive.org/web/20250713072021/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2005s111.pdf)
        * [2007](https://web.archive.org/web/20250713072021/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2006s122.pdf)
        * [2008](https://web.archive.org/web/20250713072026/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2007s113.pdf)
        * [2009](https://web.archive.org/web/20250713072221/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2008s073.pdf)
        * [2010](https://web.archive.org/web/20250713072222/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2009s049.pdf)
        * [2011](https://web.archive.org/web/20250713072228/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2010s149.pdf)
        * [2012](https://web.archive.org/web/20250713073334/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2011s189.pdf)
        * [2013](https://web.archive.org/web/20250713162959/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2012s146.pdf)
        * [2014](https://web.archive.org/web/20250713103345/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2013s149.pdf)
        * [2015](https://web.archive.org/web/20250713163521/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2014s203.pdf)
        * [2016](https://web.archive.org/web/20250713164519/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2015s196.pdf)
        * [2017](https://web.archive.org/web/20250713073752/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2016s217.pdf)
        * [2018](https://web.archive.org/web/20250714135250/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2017s224.pdf)
        * [2019](https://web.archive.org/web/20250713071343/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2018s229.pdf)
        * [2020](https://web.archive.org/web/20250714135425/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2019s217.pdf)
        * [2021](https://web.archive.org/web/20250421063152/https://www.gibraltarlaws.gov.gi/legislations/bank-and-public-holidays-order-2020-5593/download)
        * [2022](https://web.archive.org/web/20250414134257/https://www.gibraltarlaws.gov.gi/legislations/bank-and-public-holidays-order-2021-6286/version/21-10-2021/download)
        * [2023](https://web.archive.org/web/20240831014104/https://www.gibraltarlaws.gov.gi/legislations/bank-and-public-holidays-order-2022-6735/version/22-02-2023/download)
        * [2024](https://web.archive.org/web/20250502030403/https://www.gibraltarlaws.gov.gi/legislations/bank-and-public-holidays-order-2023-7215/version/06-11-2023/download)
        * [2025](https://web.archive.org/web/20250714135600/https://www.gibraltarlaws.gov.gi/legislations/bank-and-public-holidays-order-2024-7456/download)
    """

    country = "GI"
    default_language = "en_GB"
    # %s (observed).
    observed_label = tr("%s (observed)")
    # First holiday info available from 2000.
    start_year = 2000
    supported_languages = ("en_GB", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, GibraltarStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        name = (
            # Winter Midterm Bank Holiday.
            tr("Winter Midterm Bank Holiday")
            if self._year >= 2023
            # Commonwealth Day.
            else tr("Commonwealth Day")
        )
        winter_midterm_dts = {
            2024: (FEB, 12),
        }
        if dt := winter_midterm_dts.get(self._year):
            self._add_holiday(name, dt)
        elif self._year >= 2021:
            self._add_holiday_3rd_mon_of_feb(name)
        else:
            self._add_holiday_2nd_mon_of_mar(name)

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        if self._year >= 2013:
            # Workers' Memorial Day.
            name = tr("Workers' Memorial Day")
            workers_memorial_dts = {
                2013: (APR, 26),
                2015: (APR, 27),
            }
            if dt := workers_memorial_dts.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_observed(self._add_holiday_apr_28(name))

        # May Day.
        name = tr("May Day")
        may_day_dts = {
            2007: (MAY, 7),
            2008: (MAY, 5),
            2009: (MAY, 4),
        }
        if dt := may_day_dts.get(self._year):
            self._add_holiday(name, dt)
        else:
            self._add_observed(self._add_holiday_may_1(name))

        # Spring Bank Holiday.
        name = tr("Spring Bank Holiday")
        spring_bank_dts = {
            2012: (JUN, 4),
            2022: (JUN, 2),
        }
        if dt := spring_bank_dts.get(self._year):
            self._add_holiday(name, dt)
        else:
            self._add_holiday_last_mon_of_may(name)

        name = (
            # King's Birthday.
            tr("King's Birthday")
            if self._year >= 2023
            # Queen's Birthday.
            else tr("Queen's Birthday")
        )
        sovereign_birthday_dts = {
            2000: (JUN, 19),
            2001: (JUN, 18),
            2006: (JUN, 19),
            2007: (JUN, 18),
            2012: (JUN, 18),
            2013: (JUN, 17),
            2017: (JUN, 19),
            2019: (JUN, 17),
            2023: (JUN, 19),
            2024: (JUN, 17),
        }
        if dt := sovereign_birthday_dts.get(self._year):
            self._add_holiday(name, dt)
        else:
            self._add_holiday_2_days_past_2nd_sat_of_jun(name)

        self._add_holiday_last_mon_of_aug(
            # Summer Bank Holiday.
            tr("Summer Bank Holiday")
            if 2002 <= self._year <= 2007
            # Late Summer Bank Holiday.
            else tr("Late Summer Bank Holiday")
        )

        # Gibraltar National Day.
        name = tr("Gibraltar National Day")
        national_day_dts = {
            2016: (SEP, 5),
            2017: (SEP, 4),
        }
        if dt := national_day_dts.get(self._year):
            self._add_holiday(name, dt)
        else:
            self._add_observed(self._add_holiday_sep_10(name))

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


class GI(Gibraltar):
    pass


class GIB(Gibraltar):
    pass


class GibraltarStaticHolidays(StaticHolidays):
    """Gibraltar special holidays.

    References:
        * [Evacuation Commemoration Day, 2015](https://web.archive.org/web/20250714174537/https://www.gibraltarlaws.gov.gi/uploads/legislations/banking-and-financial-dealings/B&P%20Holidays/2015s112.pdf)
    """

    special_public_holidays = {
        # Tercentenary Holiday.
        2004: (AUG, 4, tr("Tercentenary Holiday")),
        # Bank Holiday.
        2009: (JAN, 12, tr("Bank Holiday")),
        # Queen's Diamond Jubilee.
        2012: (JUN, 5, tr("Queen's Diamond Jubilee")),
        # Evacuation Commemoration Day.
        2015: (SEP, 7, tr("Evacuation Commemoration Day")),
        # 75th Anniversary of VE Day.
        2020: (MAY, 8, tr("75th Anniversary of VE Day")),
        # Queen's Platinum Jubilee.
        2022: (JUN, 3, tr("Platinum Jubilee")),
        # Special King's Coronation Bank Holiday.
        2023: (MAY, 8, tr("Special King's Coronation Bank Holiday")),
    }
