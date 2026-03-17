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

from holidays.groups import InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Tunisia(HolidayBase, InternationalHolidays, IslamicHolidays):
    """Tunisia holidays."""

    country = "TN"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    supported_languages = ("ar", "en_US")

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

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        # Revolution and Youth Day - January 14
        self._add_holiday_jan_14(tr("عيد الثورة والشباب"))

        # Independence Day.
        self._add_holiday_mar_20(tr("عيد الإستقلال"))

        # Martyrs' Day.
        self._add_holiday_apr_9(tr("عيد الشهداء"))

        # Labor Day.
        self._add_labor_day(tr("عيد العمال"))

        # Republic Day.
        self._add_holiday_jul_25(tr("عيد الجمهورية"))

        # Women's Day.
        self._add_holiday_aug_13(tr("عيد المرأة"))

        # Evacuation Day.
        self._add_holiday_oct_15(tr("عيد الجلاء"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        # Eid al-Fitr Holiday.
        self._add_eid_al_fitr_day_two(tr("عطلة عيد الفطر"))
        self._add_eid_al_fitr_day_three(tr("عطلة عيد الفطر"))

        # Arafat Day.
        self._add_arafah_day(tr("يوم عرفة"))

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_eid_al_adha_day(name)
        # Eid al-Adha Holiday.
        self._add_eid_al_adha_day_two(tr("عطلة عيد الأضحى"))
        self._add_eid_al_adha_day_three(tr("عطلة عيد الأضحى"))

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("عيد المولد النبوي"))


class TN(Tunisia):
    pass


class TUN(Tunisia):
    pass
