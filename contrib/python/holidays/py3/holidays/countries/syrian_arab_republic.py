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
from holidays.calendars.gregorian import GREGORIAN_CALENDAR, MAR, OCT, FRI, SAT
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class SyrianArabRepublic(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Syrian Arab Republic holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Syria>
        * [Central Bank of Syria - Public Holidays 2024](https://web.archive.org/web/20250813035705/https://cb.gov.sy/index.php?lang=1&dir=html&ex=1&page=holidays&act=598-)
        * [Christmas and Boxing Day](https://web.archive.org/web/20250414224148/https://en.royanews.tv/news/56308)
    """

    country = "SY"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # Earliest reliable record dates back to 2004.
    start_year = 2004
    supported_languages = ("ar", "en_US")
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, calendar=JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=SyriaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("عيد رأس السنة الميلادية"))

        # Revolution Day.
        self._add_holiday_mar_8(tr("الثورة السورية"))

        # Mother's Day.
        self._add_holiday_mar_21(tr("عيد الأم"))

        # Gregorian Easter Sunday.
        self._add_easter_sunday(tr("عيد الفصح حسب التقويم الغربي"), calendar=GREGORIAN_CALENDAR)

        # Julian Easter Sunday.
        self._add_easter_sunday(tr("عيد الفصح حسب التقويم الشرقي"))

        # Independence Day.
        self._add_holiday_apr_17(tr("عيد الاستقلال"))

        # Labor Day.
        self._add_labor_day(tr("عيد العمال"))

        # Martyrs' Day.
        self._add_holiday_may_6(tr("عيد الشهداء"))

        # Tishreen Liberation War Day.
        self._add_holiday_oct_6(tr("ذكرى حرب تشرين التحريرية"))

        if self._year >= 2024:
            # Christmas Day.
            self._add_christmas_day(tr("عيد الميلاد"), calendar=GREGORIAN_CALENDAR)

            # Boxing Day.
            self._add_christmas_day_two(tr("يوم الصناديق"), calendar=GREGORIAN_CALENDAR)

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("عيد المولد النبوي الشريف"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_arafah_day(name)
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)


class SY(SyrianArabRepublic):
    pass


class SYR(SyrianArabRepublic):
    pass


class SyriaIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250120140034/https://www.timeanddate.com/holidays/syria/muharram-new-year
    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2020, 2025)

    # https://web.archive.org/web/20240808150300/https://www.timeanddate.com/holidays/syria/prophet-birthday
    MAWLID_DATES_CONFIRMED_YEARS = (2020, 2024)
    MAWLID_DATES = {
        2021: (OCT, 19),
    }

    # https://web.archive.org/web/20250404011133/https://www.timeanddate.com/holidays/syria/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2020, 2025)
    EID_AL_FITR_DATES = {
        2025: (MAR, 31),
    }

    # https://web.archive.org/web/20250126132002/https://www.timeanddate.com/holidays/syria/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2020, 2025)
