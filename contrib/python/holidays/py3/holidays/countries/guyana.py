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

from holidays.calendars import _CustomHinduHolidays, _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.groups import (
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
    SUN_TO_NEXT_MON,
)


class Guyana(
    ObservedHolidayBase,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Guyana holidays.

    References:
        * [Public Holidays Amendment Act 1967](https://web.archive.org/web/20241125073357/https://parliament.gov.gy/documents/bills/11264-bill_6_of_1967_public_holidays.pdf)
        * [Public Holidays Amendment Act 1969](https://web.archive.org/web/20250424211718/https://parliament.gov.gy/documents/bills/11891-bill_26_of_1969_1.pdf)
        * [Public Holidays Act Consolidated as of 2012](https://web.archive.org/web/20250608202320/https://mola.gov.gy/laws/Volume%206%20Cap.%2018.01%20-%2023.011696968337.pdf)
        * [2021-2025](https://web.archive.org/web/20250614200338/https://moha.gov.gy/public-holidays/)

    The holidays below are not part of the latest Public Holidays Act but have been notified
    every year as a holiday by Gazette notifications for many years. The earliest and latest
    available notifications for each are linked below.

    * Independence Day:
        * [Extraordinary Gazette 14 May 2016](https://web.archive.org/web/20250210091920/https://officialgazette.gov.gy/images/gazettes-files/Extraordinary-gazette_14may161.pdf)
        * [Extraordinary Gazette 10 May 2025](https://web.archive.org/web/20250611200312/https://officialgazette.gov.gy/images/gazette2025/may/Extra_10MAY2025NotiPHA.pdf)
    * CARICOM Day:
        * [Extraordinary Gazette 29 June 2016](https://web.archive.org/web/20250209205727/https://officialgazette.gov.gy/images/gazettes-files/Extraordinary-gazette_29jun16.pdf)
        * [Extraordinary Gazette 22 June 2024](https://web.archive.org/web/20250209154128/https://officialgazette.gov.gy/images/gazette2024/jun/Extra_22JUNE2024NotiPHA.pdf)
    * Arrival Day:
        * [Extraordinary Gazette 16 April 2019](https://web.archive.org/web/20221118182239/https://officialgazette.gov.gy/images/gazette2019/apr/Extra_20APRIL2019NotPubHol.pdf)
        * [Extraordinary Gazette 26 April 2025](https://web.archive.org/web/20250611200310/https://officialgazette.gov.gy/images/gazette2025/apr/Extra_26APRIL2025PHA.pdf)
    * Commonwealth Day / Emancipation Day:
        * No amendment act removing Commonwealth Day and adding Emancipation Day it has been that
          way in annual Gazette notifications for years.
        * [Extraordinary Gazette 21 July 2016](https://web.archive.org/web/20250211095036/https://officialgazette.gov.gy/images/gazettes-files/Extraordinary-gazette_21jul16.pdf)
        * [Extraordinary Gazette 16 July 2022](https://web.archive.org/web/20231214011437/https://officialgazette.gov.gy/images/gazette2022/jul/Extra_16JULY2022NotificPHA.pdf)
    """

    country = "GY"
    default_language = "en_GY"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    # %s (observed).
    observed_label = tr("%s (observed)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observed, estimated)")
    start_year = 1968
    supported_languages = ("en_GY", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In Guyana, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=GuyanaHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self,
            cls=GuyanaIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        StaticHolidays.__init__(self, cls=GuyanaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")), rule=SAT_SUN_TO_NEXT_MON)

        if self._year <= 1969 or self._year >= 2016:
            # Independence Day.
            self._add_holiday_may_26(tr("Independence Day"))

        if self._year >= 1970:
            # Republic Day.
            self._add_holiday_feb_23(tr("Republic Day"))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Labor Day.
        self._add_observed(self._add_labor_day(tr("Labour Day")))

        if self._year >= 2019:
            # Arrival Day.
            self._add_observed(self._add_holiday_may_5(tr("Arrival Day")))

        if self._year >= 2016:
            # CARICOM Day.
            self._add_holiday_1st_mon_of_jul(tr("CARICOM Day"))

        if self._year >= 2016:
            # Emancipation Day.
            self._add_observed(self._add_holiday_aug_1(tr("Emancipation Day")))
        else:
            # Commonwealth Day.
            self._add_holiday_1st_mon_of_aug(tr("Commonwealth Day"))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Day after Christmas.
            self._add_christmas_day_two(tr("Day after Christmas")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        # Holi.
        self._add_observed(self._add_holi(tr("Phagwah")))

        # Diwali.
        self._add_observed(self._add_diwali_india(tr("Deepavali")))

        # Prophet's Birthday.
        for dt in self._add_mawlid_day(tr("Youman Nabi")):
            self._add_observed(dt)

        # Eid al-Adha.
        for dt in self._add_eid_al_adha_day(tr("Eid-Ul-Azha")):
            self._add_observed(dt)


class GY(Guyana):
    pass


class GUY(Guyana):
    pass


class GuyanaHinduHolidays(_CustomHinduHolidays):
    # https://web.archive.org/web/20250428060218/https://www.timeanddate.com/holidays/guyana/deepavali
    DIWALI_INDIA_DATES = {
        2016: (OCT, 30),
        2017: (OCT, 19),
        2018: (NOV, 7),
        2019: (OCT, 27),
        2020: (NOV, 14),
        2021: (NOV, 4),
        2022: (OCT, 24),
        2023: (NOV, 12),
        2024: (OCT, 31),
        2025: (OCT, 20),
    }

    # https://web.archive.org/web/20250324205940/https://www.timeanddate.com/holidays/guyana/phagwah
    HOLI_DATES = {
        2005: (MAR, 26),
        2006: (MAR, 15),
        2007: (MAR, 4),
        2008: (MAR, 22),
        2009: (MAR, 11),
        2010: (MAR, 1),
        2011: (MAR, 20),
        2012: (MAR, 8),
        2013: (MAR, 27),
        2014: (MAR, 17),
        2015: (MAR, 6),
        2016: (MAR, 23),
        2017: (MAR, 12),
        2018: (MAR, 2),
        2019: (MAR, 21),
        2020: (MAR, 10),
        2021: (MAR, 28),
        2022: (MAR, 18),
        2023: (MAR, 7),
        2024: (MAR, 25),
        2025: (MAR, 14),
        2026: (MAR, 3),
    }


class GuyanaIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250424074512/https://www.timeanddate.com/holidays/guyana/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_ADHA_DATES = {
        2005: (JAN, 21),
        2006: ((JAN, 10), (DEC, 31)),
        2007: (DEC, 20),
        2012: (OCT, 26),
        2013: (OCT, 15),
        2016: (SEP, 13),
        2019: (AUG, 11),
        2020: (JUL, 31),
        2022: (JUL, 9),
    }

    # https://web.archive.org/web/20241006104125/https://www.timeanddate.com/holidays/guyana/prophet-birthday
    MAWLID_DATES_CONFIRMED_YEARS = (2001, 2025)
    MAWLID_DATES = {
        2001: (JUN, 4),
        2002: (MAY, 24),
        2005: (APR, 21),
        2007: (MAR, 31),
        2008: (MAR, 20),
        2009: (MAR, 9),
        2010: (FEB, 26),
        2013: (JAN, 24),
        2015: ((JAN, 3), (DEC, 24)),
        2020: (OCT, 29),
    }


class GuyanaStaticHolidays(StaticHolidays):
    """Guyana special holidays.

    References:
        * [Public Holiday](https://web.archive.org/web/20250207012131/https://officialgazette.gov.gy/images/gazette2020/feb/Extra_27FEBRUARY2020NotPholA.pdf)
    """

    special_public_holidays = {
        # Public Holiday.
        2020: (MAR, 2, tr("Public Holiday")),
    }
