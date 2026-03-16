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
from holidays.calendars.gregorian import JAN, FEB, APR, MAY, JUL, AUG, SEP
from holidays.groups import (
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.holiday_base import HolidayBase


class Mauritius(
    HolidayBase,
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Mauritius holidays.

    References:
        * [Public Holidays Act Consolidated 1991](https://web.archive.org/web/20250618211720/https://natlex.ilo.org/dyn/natlex2/natlex2/files/download/101480/MUS101480.pdf)
        * [The Public Holidays (Amendment) Act 2015](https://web.archive.org/web/20240805131941/http://mauritiusassembly.govmu.org/mauritiusassembly/wp-content/uploads/2023/03/act2815.pdf)
        * Mauritius became independent in 1968, but the earliest accessible version of the Public
          Holidays Act is the one consolidated in 1991. The most recent update to the holiday
          schedule reflected in that version dates back to 1987. Therefore, 1988 is being used
          as the starting year.
        * [Ougadi](https://en.wikipedia.org/wiki/Ugadi) is another name for the Telugu holiday,
          Ugadi, which is celebrated on the same day as Gudi Padwa. Therefore, reusing Gudi Padwa
          for adding Ougadi.
        * [2021](https://web.archive.org/web/20210926053030/https://mauritius-paris.govmu.org/Pages/About%20Us/Public-Holidays-in-Mauritius-for-Year-2021.aspx)
        * [2022](https://web.archive.org/web/20240703234017/https://mauritius-kualalumpur.govmu.org/Documents/Public%20Holiday/Public%20holidays%20-%202022.pdf)
        * [2024](https://web.archive.org/web/20240703233951/https://mauritius-paris.govmu.org/Documents/Public%20Holidays/Notice%20-%20Final%20Public%20holidays%20-%202024.pdf)
        * [2025](https://web.archive.org/web/20250113100149/https://pmo.govmu.org/Communique/Notice-Public_Holidays_2025.pdf)
    """

    country = "MU"
    default_language = "en_MU"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    start_year = 1988
    supported_languages = ("en_MU", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In Mauritius, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        ChineseCalendarHolidays.__init__(self)
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=MauritiusHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self,
            cls=MauritiusIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        StaticHolidays.__init__(self, cls=MauritiusStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("New Year's Day"))

        # Day after New Year's Day.
        self._add_new_years_day_two(tr("Day after New Year's Day"))

        # Abolition of Slavery.
        self._add_holiday_feb_1(tr("Abolition of Slavery"))

        # Independence and Republic Day.
        self._add_holiday_mar_12(tr("Independence and Republic Day"))

        # Labor Day.
        self._add_labor_day(tr("Labour Day"))

        if self._year >= 2016 and self._year % 2 == 0:
            # Assumption Day.
            self._add_assumption_of_mary_day(tr("Assumption of the Blessed Virgin Mary"))

        if self._year <= 2015 or self._year % 2 != 0:
            # All Saints' Day.
            self._add_all_saints_day(tr("All Saints' Day"))

        # Arrival of Indentured Laborers.
        self._add_holiday_nov_2(tr("Arrival of Indentured Labourers"))

        # Christmas Day.
        self._add_christmas_day(tr("Christmas Day"))

        # Chinese Spring Festival.
        self._add_chinese_new_years_day(tr("Chinese Spring Festival"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Eid-ul-Fitr"))

        # Thaipusam.
        self._add_thaipusam(tr("Thaipoosam Cavadee"))

        # Maha Shivaratri.
        self._add_maha_shivaratri(tr("Maha Shivaratree"))

        # Ugadi.
        self._add_gudi_padwa(tr("Ougadi"))

        # Ganesh Chaturthi.
        self._add_ganesh_chaturthi(tr("Ganesh Chaturthi"))

        # Diwali.
        self._add_diwali_india(tr("Divali"))


class MU(Mauritius):
    pass


class MUS(Mauritius):
    pass


class MauritiusHinduHolidays(_CustomHinduHolidays):
    # https://web.archive.org/web/20241208154454/https://www.timeanddate.com/holidays/mauritius/ganesh-chaturthi
    GANESH_CHATURTHI_DATES = {
        2015: (SEP, 18),
        2016: (SEP, 6),
        2017: (AUG, 25),
        2018: (SEP, 14),
        2019: (SEP, 3),
        2020: (AUG, 23),
        2021: (SEP, 11),
        2022: (SEP, 1),
        2023: (SEP, 20),
        2024: (SEP, 8),
        2025: (AUG, 28),
    }

    # https://web.archive.org/web/20240910225221/https://www.timeanddate.com/holidays/mauritius/thaipoosam-cavadee
    THAIPUSAM_DATES = {
        2020: (FEB, 8),
        2021: (JAN, 28),
        2022: (JAN, 18),
        2023: (FEB, 4),
        2024: (JAN, 25),
        2025: (FEB, 11),
    }


class MauritiusIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250420201326/https://www.timeanddate.com/holidays/mauritius/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2016, 2025)
    EID_AL_FITR_DATES = {
        2016: (JUL, 6),
        2020: (MAY, 24),
        2025: (APR, 1),
    }


class MauritiusStaticHolidays(StaticHolidays):
    """Mauritius special holidays.

    References:
        * [July 29, 2019 and September 9, 2019](https://web.archive.org/web/20250618211631/https://mauritiuslii.org/akn/mu/officialGazette/government-gazette/2019-07-27/78/eng@2019-07-27)
    """

    # Public Holiday.
    public_holiday = tr("Public Holiday")

    special_public_holidays = {
        2019: (
            (JUL, 29, public_holiday),
            (SEP, 9, public_holiday),
        )
    }
