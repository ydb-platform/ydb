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

from __future__ import annotations

from gettext import gettext as tr
from typing import TYPE_CHECKING

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import (
    FEB,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
    FRI,
    SAT,
    SUN,
)
from holidays.constants import GOVERNMENT, OPTIONAL, PUBLIC
from holidays.groups import InternationalHolidays, IslamicHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase

if TYPE_CHECKING:
    from datetime import date


class UnitedArabEmirates(HolidayBase, InternationalHolidays, IslamicHolidays, StaticHolidays):
    """United Arab Emirates holidays.

    References:
    * [2017](https://web.archive.org/web/20230209125034/https://www.khaleejtimes.com/nation/uae-official-public-holidays-list-2017)
    * [2018](https://web.archive.org/web/20201111212817/https://www.khaleejtimes.com/nation/Here-are-the-holidays-remaining-in-2018-in-UAE)
    * [2019](https://web.archive.org/web/20240513133929/https://www.thenational.ae/uae/government/uae-public-holidays-for-2019-and-2020-announced-by-cabinet-1.833425)
    * [2020](https://web.archive.org/web/20250424164934/https://u.ae/en/information-and-services/public-holidays-and-religious-affairs/public-holidays)
    * [2021](https://web.archive.org/web/20210928140451/http://wam.ae/en/details/1395302957696)
    * [2022](https://web.archive.org/web/20250207171255/https://www.khaleejtimes.com/ramadan/eid-al-fitr-holiday-announced-in-uae-3)
    * 2023:
        * <https://web.archive.org/web/20231207191645/https://www.timeoutdubai.com/news/uae-public-holidays-in-2023>
        * <https://web.archive.org/web/20250125141309/https://www.khaleejtimes.com/uae/islamic-new-year-2023-uae-announces-official-holiday-for-public-sector>
    * [2024](https://web.archive.org/web/20241001104607/https://www.timeoutdubai.com/news/uae-public-holidays-in-2024)
    * [2025](https://web.archive.org/web/20250415131512/https://www.timeoutdubai.com/news/uae-public-holidays-2025)

    Holidays are regulated by the Article 74 of [Federal Law No.
    08](https://web.archive.org/web/20250304004437/https://www.ilo.org/dyn/natlex/docs/ELECTRONIC/11956/69376/F417089305/ARE11956.pdf)
    for the year 1980.

    However the law is not applied literally, and was amended often in the past few years.
    """

    country = "AE"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    supported_categories = (GOVERNMENT, OPTIONAL, PUBLIC)
    supported_languages = ("ar", "en_US", "th")
    # Founded on DEC 2, 1971.
    start_year = 1972

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=UnitedArabEmiratesIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, UnitedArabEmiratesStaticHolidays)
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # The resting days are Saturday and Sunday since Jan 1, 2022.
        # https://web.archive.org/web/20250216144205/https://time.com/6126260/uae-working-days-weekend/
        return {SAT, SUN} if dt.year >= 2022 else {FRI, SAT}

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        if 2015 <= self._year <= 2023:
            # Commemoration Day.
            name = tr("يوم الشهيد")
            if self._year >= 2019:
                self._add_holiday_dec_1(name)
            else:
                self._add_holiday_nov_30(name)

        # National Day.
        national_day = tr("اليوم الوطني")
        self._add_holiday_dec_2(national_day)
        self._add_holiday_dec_3(national_day)

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("عيد الفطر"))

        # Eid al-Fitr Holiday.
        eid_al_fitr_holiday = tr("عطلة عيد الفطر")
        self._add_eid_al_fitr_day_two(eid_al_fitr_holiday)
        self._add_eid_al_fitr_day_three(eid_al_fitr_holiday)
        if 2019 <= self._year <= 2024:
            # Ramadan 30 not confirmed yet for 2025 onwards.
            self._add_eid_al_fitr_eve(eid_al_fitr_holiday)

        # Arafat Day.
        self._add_arafah_day(tr("وقفة عرفة"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("عيد الأضحى"))

        # Eid al-Adha Holiday.
        eid_al_adha_holiday = tr("عطلة عيد الأضحى")
        self._add_eid_al_adha_day_two(eid_al_adha_holiday)
        self._add_eid_al_adha_day_three(eid_al_adha_holiday)

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        if self._year <= 2018:
            # Isra' and Mi'raj.
            self._add_isra_and_miraj_day(tr("ليلة المعراج"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("عيد المولد النبوي"))


class AE(UnitedArabEmirates):
    pass


class ARE(UnitedArabEmirates):
    pass


class UnitedArabEmiratesIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2017, 2025)
    EID_AL_ADHA_DATES = {
        2018: (AUG, 22),
        2025: (JUN, 4),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2017, 2025)

    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2017, 2025)
    HIJRI_NEW_YEAR_DATES = {
        2017: (SEP, 22),
        2020: (AUG, 23),
        2021: (AUG, 12),
        2023: (JUL, 21),
    }

    ISRA_AND_MIRAJ_DATES_CONFIRMED_YEARS = (2017, 2018)
    ISRA_AND_MIRAJ_DATES = {
        2017: (APR, 23),
        2018: (APR, 14),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2017, 2025)
    MAWLID_DATES = {
        2018: (NOV, 18),
        2021: (OCT, 21),
        2023: (SEP, 29),
        2025: (SEP, 5),
    }


class UnitedArabEmiratesStaticHolidays:
    """United Arab Emirates special holidays.

    Special Eid al-Fitr entries for Ramadan 29 from 2020 onwards:
        * [2019](https://web.archive.org/web/20250429081745/https://www.mohre.gov.ae/en/media-centre/news/30/1/2019/عطلة-رسمية-يوم-زيارة-البابا-للمشاركين-في-فعالياته-من-القطاع-الخاص.aspx?DisableResponsive=1)
        * [2020](https://web.archive.org/web/20210804023600/https://gulfbusiness.com/revealed-uae-private-sector-holidays-for-eid-al-fitr-2020/)
        * [2021](https://web.archive.org/web/20220508191126/https://www.timeoutdubai.com/news/466278-eid-al-fitr-holiday-2021-dubai)
        * 2022:
            * <https://web.archive.org/web/20241210072332/https://gulfnews.com/uae/eid-al-fitr-2022-holidays-for-private-and-public-sectors-in-uae-explained-1.1650951429432>
            * <https://web.archive.org/web/20220616134156/https://www.arabianbusiness.com/gcc/uae/uae-suspends-work-for-three-days-marks-40-days-of-mourning-over-sheikh-khalifas-death>
        * [2023](https://web.archive.org/web/20240301231911/https://hrme.economictimes.indiatimes.com/news/industry/uae-cabinet-announces-national-day-holiday-for-federal-government-from-2-to-4-december/105455071)
        * [2024](https://web.archive.org/web/20240720085937/http://www.timeoutdubai.com/news/eid-al-fitr-2024-expected-dates-ramadan)
        * [2025](https://web.archive.org/web/20250415131512/https://www.timeoutdubai.com/news/uae-public-holidays-2025)

    Cross-Checked With:
        * <https://web.archive.org/web/20250429081729/https://www.timeanddate.com/holidays/united-arab-emirates/2021?hol=134217729>
    """

    # Eid al-Fitr Holiday.
    eid_al_fitr_holiday = tr("عطلة عيد الفطر")
    # Death of Sheikh Khalifa bin Zayed Al Nahyan.
    sheikh_khalifa_bin_zayed_death = tr("وفاة الشيخ خليفة بن زايد آل نهيان")

    special_government_holidays = {
        2022: (
            (MAY, 5, eid_al_fitr_holiday),
            (MAY, 6, eid_al_fitr_holiday),
            (MAY, 7, eid_al_fitr_holiday),
            (MAY, 8, eid_al_fitr_holiday),
        ),
        # National Day.
        2023: (DEC, 4, tr("اليوم الوطني")),
    }
    special_optional_holidays = {
        # Pope Francis' Visit Day.
        2019: (FEB, 5, tr("يوم زيارة البابا فرنسيس")),
    }
    special_public_holidays = {
        2020: (MAY, 22, eid_al_fitr_holiday),
        2021: (MAY, 11, eid_al_fitr_holiday),
        2022: (
            (APR, 30, eid_al_fitr_holiday),
            (MAY, 14, sheikh_khalifa_bin_zayed_death),
            (MAY, 15, sheikh_khalifa_bin_zayed_death),
            (MAY, 16, sheikh_khalifa_bin_zayed_death),
        ),
        2024: (APR, 8, eid_al_fitr_holiday),
    }
