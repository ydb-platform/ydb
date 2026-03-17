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
from holidays.calendars.gregorian import JUL, SEP, FRI, SAT
from holidays.groups import InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Bahrain(HolidayBase, InternationalHolidays, IslamicHolidays):
    """Bahrain holidays.

    References:
        * <https://web.archive.org/web/20250415063947/https://www.cbb.gov.bh/official-bank-holidays>
    """

    country = "BH"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # Independence declared on August 15, 1971.
    start_year = 1972
    supported_languages = ("ar", "en_US")
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
            self, cls=BahrainIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        # Labor Day.
        self._add_labor_day(tr("عيد العمال"))

        # National Day.
        national_day = tr("العيد الوطني")
        self._add_holiday_dec_16(national_day)
        self._add_holiday_dec_17(national_day)

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Ashura.
        ashura = tr("عاشوراء")
        self._add_ashura_eve(ashura)
        self._add_ashura_day(ashura)

        # Prophet's Birthday.
        self._add_mawlid_day(tr("المولد النبوي الشريف"))

        # Eid al-Fitr.
        eid_al_fitr = tr("عيد الفطر")
        self._add_eid_al_fitr_day(eid_al_fitr)
        self._add_eid_al_fitr_day_two(eid_al_fitr)
        self._add_eid_al_fitr_day_three(eid_al_fitr)

        # Eid al-Adha.
        eid_al_adha = tr("عيد الأضحى")
        self._add_eid_al_adha_day(eid_al_adha)
        self._add_eid_al_adha_day_two(eid_al_adha)
        self._add_eid_al_adha_day_three(eid_al_adha)


class BH(Bahrain):
    pass


class BAH(Bahrain):
    pass


class BahrainIslamicHolidays(_CustomIslamicHolidays):
    ASHURA_DATES_CONFIRMED_YEARS = (2019, 2025)
    ASHURA_DATES = {
        2019: (SEP, 10),
        2023: (JUL, 29),
    }

    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2019, 2025)
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2019, 2025)
    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2019, 2025)
    MAWLID_DATES_CONFIRMED_YEARS = (2019, 2025)
