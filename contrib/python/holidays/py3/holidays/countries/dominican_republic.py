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

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    TUE_WED_TO_PREV_MON,
    THU_FRI_TO_NEXT_MON,
    THU_FRI_SUN_TO_NEXT_MON,
)


class DominicanRepublic(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Dominican Republic holidays.

    References:
        * <https://web.archive.org/web/20220314033234/http://ojd.org.do/Normativas/LABORAL/Leyes/Ley%20No.%20%20139-97.pdf>
        * <https://es.wikipedia.org/wiki/República_Dominicana#Días_festivos_nacionales>
    """

    country = "DO"
    default_language = "es"
    supported_languages = ("en_US", "es", "uk")
    # Law No. 139-97 - Holidays Dominican Republic (Jun 27, 1997).
    start_year = 1998

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", TUE_WED_TO_PREV_MON + THU_FRI_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Día de los Santos Reyes")))

        # Lady of Altagracia.
        self._add_holiday_jan_21(tr("Día de la Altagracia"))

        # Juan Pablo Duarte Day.
        self._move_holiday(self._add_holiday_jan_26(tr("Día de Duarte")))

        # Independence Day.
        self._add_holiday_feb_27(tr("Día de Independencia"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        self._move_holiday(
            # Labor Day.
            self._add_labor_day(tr("Día del Trabajo")),
            rule=TUE_WED_TO_PREV_MON + THU_FRI_SUN_TO_NEXT_MON,
        )

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Corpus Christi"))

        # Restoration Day.
        aug_16 = self._add_holiday_aug_16(tr("Día de la Restauración"))
        if self._year % 4 != 0:
            self._move_holiday(aug_16)

        # Our Lady of Mercedes Day.
        self._add_holiday_sep_24(tr("Día de las Mercedes"))

        # Constitution Day.
        self._move_holiday(self._add_holiday_nov_6(tr("Día de la Constitución")))

        # Christmas Day.
        self._add_christmas_day(tr("Día de Navidad"))


class DO(DominicanRepublic):
    pass


class DOM(DominicanRepublic):
    pass
