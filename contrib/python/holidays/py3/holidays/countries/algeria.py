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

from holidays.calendars.gregorian import THU, FRI, SAT, SUN
from holidays.constants import CHRISTIAN, HEBREW, PUBLIC
from holidays.groups import (
    ChristianHolidays,
    HebrewCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
)
from holidays.holiday_base import HolidayBase

if TYPE_CHECKING:
    from datetime import date


class Algeria(
    HolidayBase, ChristianHolidays, HebrewCalendarHolidays, InternationalHolidays, IslamicHolidays
):
    """Algeria holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Algeria>
        * [Loi n° 63-278 du 26 juillet 1963](https://web.archive.org/web/20250903142053/https://drive.google.com/file/d/1rWkxBEjs9aNkbiTTRXBET3Qo_3HQOJR9/view)
        * [Ordonnance n° 68-419 du 26 juin 1968](https://web.archive.org/web/20250903142828/https://drive.google.com/file/d/1fMGyGVnunGkACWhm0tICANEZqao2pOV3/view)
        * [Loi n° 05-06 du 17 Rabie El Aoeul 1426](https://web.archive.org/web/20250903143453/https://drive.google.com/file/d/1AXISrPGJzgDO8G3uB1TyICdtnOYW6ewm/view)
        * [Loi n° 18-12 du 18 Chaoual 1439](https://web.archive.org/web/20250903141752/https://drive.google.com/file/d/1qxcrF3J-SUXmLdGZLVF0gdrN0aQWwykb/view)
        * [Loi n° 23-10 du 8 Dhou El Hidja 1444](https://web.archive.org/web/20250903141052/https://drive.google.com/file/d/1hZJtxofzjFAphOX09dGGtvHxl6IN5aNz/view)
        * <https://web.archive.org/web/20241231091630/https://www.thenationalnews.com/mena/2021/12/07/when-is-the-weekend-in-the-arab-world/>
        * <https://web.archive.org/web/20250903160618/https://www.lemonde.fr/afrique/article/2018/01/12/en-algerie-le-nouvel-an-berbere-ferie-pour-la-premiere-fois_5241028_3212.html>
    """

    country = "DZ"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # Loi n° 63-278 du 26 juillet 1963.
    start_year = 1964
    supported_categories = (CHRISTIAN, HEBREW, PUBLIC)
    supported_languages = ("ar", "en_US", "fr")
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        HebrewCalendarHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # The resting days are Friday and Saturday since 2009.
        # Previously, these were on Thursday and Friday as implemented in 1976.
        if dt.year >= 2009:
            return {FRI, SAT}
        elif dt.year >= 1976:
            return {THU, FRI}
        else:
            return {SAT, SUN}

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        # First Celebrated in 2018.
        # Reaffirmed via Loi n° 18-12 du 18 Chaoual 1439.
        if self._year >= 2018:
            # Amazigh New Year.
            self._add_holiday_jan_12(tr("رأس السنة الأمازيغية"))

        # Labor Day.
        self._add_labor_day(tr("عيد العمال"))

        # Name changed in Loi n° 05-06 du 17 Rabie El Aoeul 1426.
        self._add_holiday_jul_5(
            # Independence Day.
            tr("عيد الاستقلال")
            if self._year >= 2005
            # Independence and National Liberation Front Day.
            else tr("عيد الاستقلال وجبهة التحرير الوطني")
        )

        # Revolution Day.
        self._add_holiday_nov_1(tr("عيد الثورة"))

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Ashura.
        self._add_ashura_day(tr("عاشورة"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("عيد المولد النبوي"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        # Added via Loi n° 23-10 du 8 Dhou El Hidja 1444.
        if self._year >= 2024:
            self._add_eid_al_fitr_day_three(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_eid_al_adha_day(name)
        # Added via Ordonnance n° 68-419 du 26 juin 1968.
        if self._year >= 1969:
            self._add_eid_al_adha_day_two(name)
            # Added via Loi n° 23-10 du 8 Dhou El Hidja 1444.
            if self._year >= 2023:
                self._add_eid_al_adha_day_three(name)

    def _populate_christian_holidays(self):
        # As outlined in Loi n° 63-278 du 26 juillet 1963.

        # Easter Monday.
        self._add_easter_monday(tr("إثنين الفصح"))

        # Ascension Day.
        self._add_ascension_thursday(tr("عيد الصعود"))

        # Whit Monday.
        self._add_whit_monday(tr("إثنين العنصرة"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("عيد انتقال السيدة العذراء"))

        # Christmas Day.
        self._add_christmas_day(tr("عيد الميلاد"))

    def _populate_hebrew_holidays(self):
        # As outlined in Loi n° 63-278 du 26 juillet 1963.

        # Rosh Hashanah.
        self._add_rosh_hashanah(tr("رأس السنة العبرية"))

        # Yom Kippur.
        self._add_yom_kippur(tr("يوم الغفران"))

        # Pesach.
        self._add_passover(tr("عيد الفصح اليهودي"))


class DZ(Algeria):
    pass


class DZA(Algeria):
    pass
