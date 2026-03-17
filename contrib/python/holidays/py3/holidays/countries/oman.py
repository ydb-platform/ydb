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

from datetime import date
from gettext import gettext as tr

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import (
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    THU,
    FRI,
    SAT,
    _timedelta,
)
from holidays.groups import IslamicHolidays
from holidays.holiday_base import HolidayBase


class Oman(HolidayBase, IslamicHolidays):
    """Oman holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Oman>
        * [Independence](https://web.archive.org/web/20250211055127/https://omaninfo.om/pages/175/show/572)
        * [Weekend](https://web.archive.org/web/20250213121638/https://abnnews.com/the-sultanate-of-oman-changes-weekend-days-from-01-may-2013/)
        * [Decree 56/2020](https://web.archive.org/web/20240522150742/https://decree.om/2020/rd20200056/)
        * [Decree 88/2022](https://web.archive.org/web/20221207194900/https://decree.om/2022/rd20220088/)
        * [Decree 15/2025 (National day is moved)](https://web.archive.org/web/20250121132446/https://decree.om/2025/rd20250015/)
    """

    country = "OM"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    start_year = 1970
    supported_languages = ("ar", "en_US")
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        IslamicHolidays.__init__(
            self, cls=OmanIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # Oman switches from THU-FRI to FRI-SAT on May 1, 2013.
        return {FRI, SAT} if dt >= date(2013, MAY, 1) else {THU, FRI}

    def _populate_public_holidays(self):
        if self._year >= 2020:
            # Sultan's Accession Day.
            self._add_holiday_jan_11(tr("اليوم الوطني لتولي السلطان"))

        if self._year <= 2019:
            # Renaissance Day.
            self._add_holiday_jul_23(tr("يوم النهضة"))

        if self._year >= 2020:
            # National Day.
            name = tr("يوم وطني")
            if self._year <= 2024:
                self._add_holiday_nov_18(name)
                self._add_holiday_nov_19(name)
            else:
                self._add_holiday_nov_20(name)
                self._add_holiday_nov_21(name)

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("مولد النبي"))

        # Isra' and Mi'raj.
        self._add_isra_and_miraj_day(tr("الإسراء والمعراج"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)
        for dt in self._add_holiday_29_ramadan(name):
            if _timedelta(dt, +1) not in self:
                self._add_eid_al_fitr_eve(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_arafah_day(name)
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)


class OM(Oman):
    pass


class OMN(Oman):
    pass


class OmanIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20240911084650/https://www.timeanddate.com/holidays/oman/muharram-new-year
    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2018, 2024)
    HIJRI_NEW_YEAR_DATES = {
        2019: (SEP, 1),
        2020: (AUG, 21),
        2021: (AUG, 10),
        2023: (JUL, 20),
    }

    # https://web.archive.org/web/20240810230306/https://www.timeanddate.com/holidays/oman/prophet-birthday
    MAWLID_DATES_CONFIRMED_YEARS = (2018, 2024)
    MAWLID_DATES = {
        2021: (OCT, 19),
        2022: (OCT, 9),
        2023: (SEP, 28),
    }

    # https://web.archive.org/web/20241231191036/https://www.timeanddate.com/holidays/oman/isra-miraj
    ISRA_AND_MIRAJ_DATES_CONFIRMED_YEARS = (2018, 2025)
    ISRA_AND_MIRAJ_DATES = {
        2022: (MAR, 1),
        2023: (FEB, 19),
    }

    # https://web.archive.org/web/20240913230603/https://www.timeanddate.com/holidays/oman/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2019, 2024)
    EID_AL_ADHA_DATES = {
        2024: (JUN, 17),
    }

    # https://web.archive.org/web/20241231191036/https://www.timeanddate.com/holidays/oman/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2019, 2025)
    EID_AL_FITR_DATES = {
        2023: (APR, 22),
        2025: (MAR, 31),
    }

    # https://web.archive.org/web/20240814104839/https://www.timeanddate.com/holidays/oman/ramadan-begins
    RAMADAN_BEGINNING_DATES_CONFIRMED_YEARS = (2023, 2025)
    RAMADAN_BEGINNING_DATES = {
        2024: (MAR, 12),
    }
