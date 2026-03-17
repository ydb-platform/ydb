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

from holidays.groups import IslamicHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Morocco(HolidayBase, InternationalHolidays, IslamicHolidays):
    """Morocco holidays.

    References:
        * <https://fr.wikipedia.org/wiki/Fêtes_et_jours_fériés_au_Maroc>
        * <https://web.archive.org/web/20230303001626/http://www.mmsp.gov.ma/fr/pratiques.aspx?id=38>
    """

    country = "MA"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    supported_languages = ("ar", "en_US", "fr")

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

        if self._year >= 1945:
            # Proclamation of Independence Day.
            self._add_holiday_jan_11(tr("ذكرى تقديم وثيقة الاستقلال"))

        # In May 2023, Morocco recognized Berber New Year as official holiday.
        # https://web.archive.org/web/20230515114330/https://www.diplomatie.ma/en/statement-royal-office-12
        if self._year >= 2024:
            # Amazigh New Year.
            self._add_holiday_jan_13(tr("رأس السنة الأمازيغية"))

        # Labor Day.
        self._add_labor_day(tr("عيد العمال"))

        # Throne day.
        name = tr("عيد العرش")
        if self._year >= 2001:
            self._add_holiday_jul_30(name)
        elif self._year >= 1963:
            self._add_holiday_mar_3(name)
        else:
            self._add_holiday_nov_18(name)

        # Oued Ed-Dahab Day.
        self._add_holiday_aug_14(tr("ذكرى استرجاع إقليم وادي الذهب"))

        # Revolution Day.
        self._add_holiday_aug_20(tr("ذكرى ثورة الملك و الشعب"))

        # Youth Day.
        name = tr("عيد الشباب")
        if self._year >= 2001:
            self._add_holiday_aug_21(name)
        else:
            self._add_holiday_jul_9(name)

        if self._year >= 1976:
            # Green March.
            self._add_holiday_nov_6(tr("ذكرى المسيرة الخضراء"))

        if self._year >= 1957:
            # Independence Day.
            self._add_holiday_nov_18(tr("عيد الإستقلال"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)

        # Islamic New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        name = tr("عيد المولد النبوي")
        self._add_mawlid_day(name)
        self._add_mawlid_day_two(name)


class MA(Morocco):
    pass


class MOR(Morocco):
    pass
