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
from holidays.calendars.gregorian import JAN, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC, FRI, SAT
from holidays.groups import InternationalHolidays, IslamicHolidays, PersianCalendarHolidays
from holidays.holiday_base import HolidayBase


class Afghanistan(HolidayBase, InternationalHolidays, IslamicHolidays, PersianCalendarHolidays):
    """Afghanistan holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Afghanistan>
        * <https://web.archive.org/web/20250903162748/https://www.timeanddate.com/holidays/afghanistan>
        * <https://en.wikipedia.org/wiki/Workweek_and_weekend>
    """

    country = "AF"
    default_language = "fa_AF"
    # %s (estimated).
    estimated_label = tr("%s (برآورد شده)")
    supported_languages = ("en_US", "fa_AF", "ps_AF")
    # Afghanistan's regaining of full independence from British influence.
    start_year = 1919
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=AfghanistanIslamicHolidays, show_estimated=islamic_show_estimated
        )
        PersianCalendarHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        if self._year >= 1989:
            # Liberation Day.
            self._add_holiday_feb_15(tr("روز آزادی"))

        # Afghanistan Independence Day.
        self._add_holiday_aug_19(tr("روز استقلال افغانستان"))

        if self._year <= 1996 or 2001 <= self._year <= 2020:
            # Nowruz.
            self._add_nowruz_day(tr("نوروز"))

        if self._year >= 1992:
            # Mojahedin's Victory Day.
            self._add_holiday_apr_28(tr("روز پیروزی مجاهدین"))

        if 1974 <= self._year <= 1996 or 2002 <= self._year <= 2021:
            # International Workers' Day.
            self._add_labor_day(tr("روز جهانی کارگر"))

        if 1978 <= self._year <= 1988:
            # Soviet Victory Day.
            self._add_holiday_may_9(tr("روز پیروزی شوروی"))

        if self._year >= 2022:
            # Islamic Emirate Victory Day.
            self._add_islamic_emirat_victory_day(tr("روز پیروزی امارت اسلامی"))

            # American Withdrawal Day.
            self._add_holiday_aug_31(tr("روز خروج آمریکایی ها"))

        if 2012 <= self._year <= 2020:
            # Martyrs' Day.
            self._add_holiday_sep_9(tr("روز شهیدان"))

        if self._year <= 2021:
            # Ashura.
            self._add_ashura_day(tr("عاشورا"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("میلاد پیامبر"))

        # First Day of Ramadan.
        self._add_ramadan_beginning_day(tr("اول رمضان"))

        # Eid al-Fitr.
        name = tr("عید فطر")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Day of Arafah.
        self._add_arafah_day(tr("روز عرفه"))

        # Eid al-Adha.
        name = tr("عید قربانی")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)


class AF(Afghanistan):
    pass


class AFG(Afghanistan):
    pass


class AfghanistanIslamicHolidays(_CustomIslamicHolidays):
    ASHURA_DATES_CONFIRMED_YEARS = (2014, 2021)
    ASHURA_DATES = {
        2015: (OCT, 24),
        2016: (OCT, 12),
        2017: (OCT, 1),
        2018: (SEP, 21),
        2019: (SEP, 10),
        2020: (AUG, 30),
        2021: (AUG, 19),
    }

    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2014, 2025)
    EID_AL_ADHA_DATES = {
        2014: (OCT, 5),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
        2024: (JUN, 17),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2014, 2025)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2022: (MAY, 1),
        2023: (APR, 22),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2014, 2025)
    MAWLID_DATES = {
        2014: (JAN, 14),
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2018: (NOV, 21),
        2019: (NOV, 10),
        2021: (OCT, 19),
        2024: (SEP, 16),
        2025: (SEP, 5),
    }

    RAMADAN_BEGINNING_DATES_CONFIRMED_YEARS = (2014, 2025)
    RAMADAN_BEGINNING_DATES = {
        2014: (JUN, 29),
        2016: (JUN, 7),
    }
