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
from holidays.calendars.gregorian import GREGORIAN_CALENDAR
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.constants import CATHOLIC, ORTHODOX, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Palestine(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Palestine holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Palestine>
        * <https://web.archive.org/web/20250705114216/https://info.wafa.ps/pages/details/29601>
        * [Declaration of Palestine independence](https://en.wikipedia.org/wiki/Palestine#Uprising,_declaration_and_peace_treaty)
    """

    country = "PS"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # State of Palestine declared in November 1988.
    start_year = 1989
    supported_categories = (CATHOLIC, ORTHODOX, PUBLIC)
    supported_languages = ("ar", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = False, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, calendar=JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=PalestineIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادي"))

        # Orthodox Christmas Day.
        self._add_christmas_day(tr("عيد الميلاد المجيد الشرقي"))

        # International Women's Day.
        self._add_womens_day(tr("يوم المراة العالمي"))

        # Easter.
        self._add_easter_sunday(tr("عيد الفصح المجيد"), GREGORIAN_CALENDAR)

        # Easter.
        self._add_easter_sunday(tr("عيد الفصح المجيد"))

        # Labor Day.
        self._add_labor_day(tr("عيد العمال"))

        # Independence Day.
        self._add_holiday_nov_15(tr("عيد الإستقلال"))

        # Catholic Christmas Day.
        self._add_christmas_day(tr("عيد الميلاد المجيد الغربي"), GREGORIAN_CALENDAR)

        # Hijri New Year.
        self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("ذكرى المولد النبوي الشريف"))

        # Isra' and Mi'raj.
        self._add_isra_and_miraj_day(tr("ذكرى الإسراء والمعراج"))

        # Eid al-Fitr.
        name = tr("عيد الفطر السعيد")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى المبارك")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)
        self._add_eid_al_adha_day_four(name)

    def _populate_catholic_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادي"))

        # Epiphany.
        self._add_epiphany_day(tr("عيد الغطاس"), GREGORIAN_CALENDAR)

        # Palm Sunday.
        self._add_palm_sunday(tr("أحد الشعانين"), GREGORIAN_CALENDAR)

        # Holy Thursday.
        self._add_holy_thursday(tr("خميس الغسل"), GREGORIAN_CALENDAR)

        # Good Friday.
        self._add_good_friday(tr("الجمعة العظيمة"), GREGORIAN_CALENDAR)

        # Holy Saturday.
        self._add_holy_saturday(tr("سبت النور"), GREGORIAN_CALENDAR)

        # Easter.
        self._add_easter_monday(tr("عيد الفصح المجيد"), GREGORIAN_CALENDAR)

        # Ascension Day.
        self._add_ascension_thursday(tr("خميس الصعود"), GREGORIAN_CALENDAR)

        # Pentecost.
        self._add_whit_sunday(tr("أحد العنصرة"), GREGORIAN_CALENDAR)

        # Catholic Christmas Day.
        self._add_christmas_day_two(tr("عيد الميلاد المجيد الغربي"), GREGORIAN_CALENDAR)

    def _populate_orthodox_holidays(self):
        # Orthodox New Year's Day.
        self._add_holiday_jan_14(tr("عيد رأس السنة الشرقي"))

        # Orthodox Christmas Day.
        self._add_christmas_day_two(tr("عيد الميلاد المجيد الشرقي"))

        # Epiphany.
        self._add_epiphany_day(tr("عيد الغطاس"))

        # Palm Sunday.
        self._add_palm_sunday(tr("أحد الشعانين"))

        # Holy Thursday.
        self._add_holy_thursday(tr("خميس الغسل"))

        # Good Friday.
        self._add_good_friday(tr("الجمعة العظيمة"))

        # Holy Saturday.
        self._add_holy_saturday(tr("سبت النور"))

        # Easter.
        self._add_easter_monday(tr("عيد الفصح المجيد"))

        # Ascension Day.
        self._add_ascension_thursday(tr("خميس الصعود"))

        # Pentecost.
        self._add_whit_sunday(tr("أحد العنصرة"))


class PS(Palestine):
    pass


class PSE(Palestine):
    pass


class PalestineIslamicHolidays(_CustomIslamicHolidays):
    MAWLID_DATES_CONFIRMED_YEARS = (2020, 2024)
