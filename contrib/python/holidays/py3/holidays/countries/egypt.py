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
from holidays.calendars.gregorian import JAN, APR, MAY, JUL, AUG, SEP, MON, TUE, WED, FRI, SAT, SUN
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.constants import GOVERNMENT, PUBLIC, SCHOOL
from holidays.groups import (
    ChristianHolidays,
    IslamicHolidays,
    InternationalHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, ObservedRule

EG_OBSERVED_RULE = ObservedRule({MON: +3, TUE: +2, WED: +1, SUN: +4})


class Egypt(
    ObservedHolidayBase, ChristianHolidays, IslamicHolidays, InternationalHolidays, StaticHolidays
):
    """Egypt holidays.

    References:
        * <https://ar.wikipedia.org/wiki/قائمة_العطل_الرسمية_في_مصر>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Egypt>
        * [National Holidays (Arabic)](https://web.archive.org/web/20250614072551/https://www.presidency.eg/ar/مصر/العطلات-الرسمية/)
        * [National Holidays (English)](https://web.archive.org/web/20250529043734/https://www.presidency.eg/en/مصر/العطلات-الرسمية/)
        * [National Holidays (French)](https://web.archive.org/web/20250608173134/https://www.presidency.eg/fr/مصر/العطلات-الرسمية/)
        * [Ministerial Decision 1193](https://web.archive.org/web/20250423073350/https://manshurat.org/node/44922)
    """

    country = "EG"
    default_language = "ar_EG"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # %s (observed).
    observed_label = tr("%s (ملاحظة)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (المقدرة، ملاحظة)")
    # Republic of Egypt was declared on 18 June 1953.
    start_year = 1954
    supported_categories = (GOVERNMENT, PUBLIC, SCHOOL)
    supported_languages = ("ar_EG", "en_US", "fr")
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=EgyptIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=EgyptStaticHolidays)
        kwargs.setdefault("observed_rule", EG_OBSERVED_RULE)
        kwargs.setdefault("observed_since", 2020)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        if self._year >= 2002 and self._year not in {2022, 2023}:
            # Coptic Christmas Day.
            self._add_christmas_day(tr("عيد الميلاد المجيد"))

        if self._year >= 2009:
            self._move_holiday(
                self._add_holiday_jan_25(
                    # January 25th Revolution and National Police Day.
                    tr("ثورة ٢٥ يناير وعيد الشرطة")
                    if self._year >= 2012
                    # National Police Day.
                    else tr("عيد الشرطة")
                )
            )

        if self._year >= 1983:
            # Sinai Liberation Day.
            dt = self._add_holiday_apr_25(tr("عيد تحرير سيناء"))
            if self._year != 2022:
                self._move_holiday(dt)

        # Spring Festival.
        self._add_easter_monday(tr("عيد شم النسيم"))

        # Labor Day.
        if self._year != 2024:
            self._move_holiday(self._add_labor_day(tr("عيد العمال")))

        if self._year >= 2014:
            # June 30 Revolution Day.
            dt = self._add_holiday_jun_30(tr("عيد ثورة ٣٠ يونيو"))
            if self._year != 2024:
                self._move_holiday(dt)

        # July 23 Revolution Day.
        dt = self._add_holiday_jul_23(tr("عيد ثورة ٢٣ يوليو"))
        if self._year != 2023:
            self._move_holiday(dt)

        # Armed Forces Day.
        dt = self._add_holiday_oct_6(tr("عيد القوات المسلحة"))
        if self._year != 2024:
            self._move_holiday(dt)

        # Islamic New Year.
        for dt in self._add_islamic_new_year_day(tr("رأس السنة الهجرية")):
            self._move_holiday(dt)

        # Prophet's Birthday.
        self._add_mawlid_day(tr("المولد النبوي الشريف"))

        # Eid al-Fitr.
        name = tr("عيد الفطر المبارك")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)

        # Arafat Day.
        self._add_arafah_day(tr("وقفة عيد الأضحى المبارك"))

        # Eid al-Adha.
        name = tr("عيد الأضحى المبارك")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)

    def _populate_government_holidays(self):
        if self._year >= 2019:
            # Eid al-Fitr.
            name = tr("عيد الفطر المبارك")
            self._add_eid_al_fitr_eve(name)
            self._add_eid_al_fitr_day_three(name)

        if self._year >= 2018:
            # Eid al-Adha.
            self._add_eid_al_adha_day_four(tr("عيد الأضحى المبارك"))

    def _populate_school_holidays(self):
        if self._year >= 2019:
            # Taba Liberation Day.
            self._add_holiday_mar_19(tr("عيد تحرير طابا"))

            # Evacuation Day.
            self._add_holiday_jun_18(tr("عيد الجلاء"))


class EG(Egypt):
    pass


class EGY(Egypt):
    pass


class EgyptIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2020, 2025)
    EID_AL_ADHA_DATES = {
        2022: (JUL, 10),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2020, 2025)
    EID_AL_FITR_DATES = {
        2022: (MAY, 1),
    }

    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2020, 2025)
    HIJRI_NEW_YEAR_DATES = {
        2021: (AUG, 11),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2020, 2025)
    MAWLID_DATES = {
        2023: (SEP, 28),
    }


class EgyptStaticHolidays:
    """Egypt special holidays.

    References:
        * [2022 Coptic Christmas](https://web.archive.org/web/20240227004025/https://english.ahram.org.eg/NewsContent/1/2/454491/Egypt/Society/Thursday-paid-day-off-at-public-sector-in-Egypt-fo.aspx)
    """

    # Coptic Christmas Day.
    coptic_christmas_day = tr("عيد الميلاد المجيد")

    # Eid al-Adha.
    eid_al_adha = tr("عيد الأضحى المبارك")

    # Eid al-Fitr.
    eid_al_fitr = tr("عيد الفطر المبارك")

    special_public_holidays = {
        2021: (
            (JUL, 17, eid_al_adha),
            (JUL, 18, eid_al_adha),
        ),
        2022: (
            (JUL, 13, eid_al_adha),
            (JUL, 14, eid_al_adha),
        ),
    }

    special_public_holidays_observed = {
        2022: (
            (JAN, 6, coptic_christmas_day),
            (MAY, 4, eid_al_fitr),
        ),
        2023: (
            (JAN, 8, coptic_christmas_day),
            (APR, 24, eid_al_fitr),
            # June 30 Revolution Day.
            (JUL, 2, tr("عيد ثورة ٣٠ يونيو")),
            (JUL, 3, eid_al_adha),
        ),
        # Labor Day.
        2024: (MAY, 5, tr("عيد العمال")),
    }
