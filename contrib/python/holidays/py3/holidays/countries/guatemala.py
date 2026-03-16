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

from holidays.calendars.gregorian import OCT
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, ALL_TO_NEAREST_MON_LATAM


class Guatemala(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Guatemala holidays.

    References:
        * <https://web.archive.org/web/20240419130706/http://www.bvnsa.com.gt/bvnsa/calendario_dias_festivos.php>
        * <https://web.archive.org/web/20250426043930/https://www.minfin.gob.gt/images/downloads/leyes_acuerdos/decretocong19_101018.pdf>

    Moving holidays:
        * [Decree 19-2018 start 18 oct 2018](https://web.archive.org/web/20250426043930/https://www.minfin.gob.gt/images/downloads/leyes_acuerdos/decretocong19_101018.pdf)
        * [Case 5536-2018 (CC) start 17 abr 2020](https://web.archive.org/web/20240625093244/https://leyes.infile.com/index.php?id=182&id_publicacion=81055)
    """

    country = "GT"
    default_language = "es"
    supported_languages = ("en_US", "es")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", ALL_TO_NEAREST_MON_LATAM)
        super().__init__(*args, **kwargs)

    def _is_observed(self, dt: date) -> bool:
        return dt >= date(2018, OCT, 18)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Holy Saturday.
        self._add_holy_saturday(tr("Sábado Santo"))

        # Labor Day.
        dt = self._add_labor_day(tr("Día del Trabajo"))
        if self._year == 2019:
            self._move_holiday(dt)

        # Army Day.
        self._move_holiday(self._add_holiday_jun_30(tr("Día del Ejército")))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Día de la Asunción"))

        # Independence Day.
        self._add_holiday_sep_15(tr("Día de la Independencia"))

        # Revolution Day.
        dt = self._add_holiday_oct_20(tr("Día de la Revolución"))
        if self._year in {2018, 2019}:
            self._move_holiday(dt)

        # All Saints' Day.
        self._add_all_saints_day(tr("Día de Todos los Santos"))

        # Christmas Day.
        self._add_christmas_day(tr("Día de Navidad"))


class GT(Guatemala):
    pass


class GUA(Guatemala):
    pass
