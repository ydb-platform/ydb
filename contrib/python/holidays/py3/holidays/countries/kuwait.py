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

from holidays.calendars.gregorian import SEP, THU, FRI, SAT
from holidays.groups import InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Kuwait(HolidayBase, InternationalHolidays, IslamicHolidays):
    """Kuwait holidays.

    References:
        * <https://en.wikipedia.org/wiki/2024_in_Kuwait>
        * <https://web.archive.org/web/20250414072705/https://www.officeholidays.com/countries/kuwait>
        * <https://web.archive.org/web/20250414072609/https://www.timeanddate.com/holidays/kuwait/2024>
    """

    country = "KW"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
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
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # The resting days are Friday and Saturday since Sep 1, 2007.
        # https://web.archive.org/web/20250414072729/https://www.arabnews.com/node/298933
        return {FRI, SAT} if dt >= date(2007, SEP, 1) else {THU, FRI}

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        # National Day.
        self._add_holiday_feb_25(tr("اليوم الوطني"))

        # Liberation Day.
        self._add_holiday_feb_26(tr("يوم التحرير"))

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("عيد المولد النبوي"))

        # Isra' and Mi'raj.
        self._add_isra_and_miraj_day(tr("ليلة المعراج"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("عيد الفطر"))
        # Eid al-Fitr Holiday.
        self._add_eid_al_fitr_day_two(tr("عطلة عيد الفطر"))
        self._add_eid_al_fitr_day_three(tr("عطلة عيد الفطر"))

        # Arafat Day.
        self._add_arafah_day(tr("يوم عرفة"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("عيد الأضحى"))
        # Eid al-Adha Holiday.
        self._add_eid_al_adha_day_two(tr("عطلة عيد الأضحى"))
        self._add_eid_al_adha_day_three(tr("عطلة عيد الأضحى"))


class KW(Kuwait):
    pass


class KWT(Kuwait):
    pass
