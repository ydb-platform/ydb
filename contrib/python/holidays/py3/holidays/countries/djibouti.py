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

from holidays.calendars.gregorian import FRI, SAT
from holidays.groups import ChristianHolidays, IslamicHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Djibouti(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Djibouti holidays."""

    country = "DJ"
    default_language = "fr"
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    supported_languages = ("ar", "en_US", "fr")
    weekend = {FRI, SAT}
    # On 27 June 1977, Djibouti gained independence from France.
    start_year = 1978

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nouvel an"))

        # Labor Day.
        self._add_labor_day(tr("Fête du travail"))

        # Independence Day.
        self._add_holiday_jun_27(tr("Fête de l'indépendance"))

        # Independence Day Holiday.
        self._add_holiday_jun_28(tr("Fête de l'indépendance deuxième jour"))

        # Christmas Day.
        self._add_christmas_day(tr("Noël"))

        # Isra' and Mi'raj.
        self._add_isra_and_miraj_day(tr("Al Isra et Al Mirague"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Eid al-Fitr"))

        # Eid al-Fitr Holiday.
        self._add_eid_al_fitr_day_two(tr("Eid al-Fitr deuxième jour"))

        # Arafat Day.
        self._add_arafah_day(tr("Arafat"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Eid al-Adha"))

        # Eid al-Adha Holiday.
        self._add_eid_al_adha_day_two(tr("Eid al-Adha deuxième jour"))

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("Nouvel an musulman"))

        # Prophet Muhammad's Birthday.
        self._add_mawlid_day(tr("Anniversaire du prophète Muhammad"))


class DJ(Djibouti):
    pass


class DJI(Djibouti):
    pass
