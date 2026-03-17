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

from holidays.calendars.gregorian import JAN, THU, FRI, SAT
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Jordan(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Jordan holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Jordan>
        * <https://web.archive.org/web/20250116061815/https://www.mfa.gov.jo/content/public-holidays>
    """

    country = "JO"
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
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # The resting days are Friday and Saturday since Jan 6, 2000.
        # https://web.archive.org/web/20241226195649/http://archive.wfn.org/2000/01/msg00078.html
        return {FRI, SAT} if dt >= date(2000, JAN, 6) else {THU, FRI}

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        # Labor Day.
        self._add_labor_day(tr("عيد العمال"))

        # Independence Day.
        self._add_holiday_may_25(tr("عيد الإستقلال"))

        # Christmas Day.
        self._add_christmas_day(tr("عيد الميلاد المجيد"))

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("عيد المولد النبوي"))

        # Isra' and Mi'raj.
        self._add_isra_and_miraj_day(tr("ليلة المعراج"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("عيد الفطر"))
        # Eid al-Fitr Holiday.
        name = tr("عطلة عيد الفطر")
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Arafat Day.
        self._add_arafah_day(tr("يوم عرفة"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("عيد الأضحى"))
        # Eid al-Adha Holiday.
        name = tr("عطلة عيد الأضحى")
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)


class JO(Jordan):
    pass


class JOR(Jordan):
    pass
