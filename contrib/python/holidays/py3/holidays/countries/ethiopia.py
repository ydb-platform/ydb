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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.ethiopian import ETHIOPIAN_CALENDAR, is_ethiopian_leap_year
from holidays.calendars.gregorian import AUG, SEP, NOV
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Ethiopia(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Ethiopia holidays.

    References:
        * [Proclamation No. 29/1996](https://web.archive.org/web/20240918183943/https://lawethiopia.com/images/federal_proclamation/proclamations_by_number/29.pdf)
        * [Proclamation No. 1334/2024](https://web.archive.org/web/20240823080839/https://lawethiopiacomment.wordpress.com/wp-content/uploads/2024/08/public-holiday-stamped.pdf)
        * <https://web.archive.org/web/20250427173714/https://www.edarabia.com/ethiopia/public-holidays/>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Ethiopia>
        * <https://en.wikipedia.org/wiki/Nations,_Nationalities_and_Peoples'_Day>
        * <https://web.archive.org/web/20250811041201/https://www.timeanddate.com/holidays/ethiopia/2025>
    """

    country = "ET"
    default_language = "am"
    # %s (estimated).
    estimated_label = tr("%s (ግምት)")
    # Negarit Gazeta Proclamation No. 16/1975.
    start_year = 1976
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("am", "ar", "en_ET", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, ETHIOPIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=EthiopiaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Christmas Day.
        self._add_christmas_day(tr("የገና ወይም የልደት በዓል"))

        # Epiphany.
        self._add_epiphany_day(tr("የጥምቀት በዓል"))

        if self._year >= 1996:
            # Adwa Victory Day.
            self._add_holiday_mar_2(tr("የአድዋ ድል በዓል"))

        # Good Friday.
        self._add_good_friday(tr("የስቅለት በዓል"))

        # Easter Sunday.
        self._add_easter_sunday(tr("የትንሳኤ(ፋሲካ) በዓል"))

        # International Workers' Day.
        self._add_labor_day(tr("የዓለም የሠራተኞች (የላብአደሮች) ቀን"))

        # Ethiopian Patriots' Victory Day.
        self._add_holiday_may_5(tr("የአርበኞች (የድል) ቀን በዓል"))

        if self._year >= 1992:
            # Downfall of the Dergue Regime Day.
            self._add_holiday_may_28(tr("ደርግ የወደቀበት ቀን"))

        # Ethiopian New Year.
        self._add_ethiopian_new_year(tr("የዘመን መለወጫ (እንቁጣጣሽ) በዓል"))

        # Finding of True Cross.
        self._add_finding_of_true_cross(tr("የመስቀል በዓል"))

        if self._year <= 1990:
            # Julian Date Drift shouldn't affect this one.
            self._add_holiday(
                # Popular Revolution Commemoration Day.
                tr("የአብዮት ቀን"),
                SEP,
                13 if is_ethiopian_leap_year(self._year) else 12,
            )

            # October Revolution Day.
            self._add_holiday_nov_7(tr("የጥቅምት አብዮት ቀን"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("የኢድ አልፈጥር"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("የኢድ አልአድሃ (አረፋ)"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("የመውሊድ በዓል"))

    def _populate_workday_holidays(self):
        # Ethiopian Martyrs' Day.
        self._add_holiday_feb_20(tr("የሰማዕታት ቀን"))

        if self._year >= 2006:
            # Nations, Nationalities and Peoples Day.
            self._add_holiday_dec_9(tr("የብሔር ብሔረሰቦች ቀን"))


class ET(Ethiopia):
    pass


class ETH(Ethiopia):
    pass


class EthiopiaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2018, 2025)
    EID_AL_ADHA_DATES = {
        2018: (AUG, 22),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2018, 2025)

    MAWLID_DATES_CONFIRMED_YEARS = (2018, 2025)
    MAWLID_DATES = {
        2018: (NOV, 21),
        2019: (NOV, 10),
        2025: (SEP, 5),
    }
