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

from holidays.calendars.gregorian import JUL
from holidays.constants import BANK, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Panama(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Panama holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Panama>
        * [Labor Code 1947](https://web.archive.org/web/20250429081529/https://docs.panama.justia.com/federales/leyes/67-de-1947-nov-26-1947.pdf)
        * [Cabinet Decree #347 of 1969](https://web.archive.org/web/20250429081519/https://docs.panama.justia.com/federales/decretos-de-gabinete/decreto-de-gabinete-347-de-1969-nov-14-1969.pdf)
        * [Labor Code 1971](https://web.archive.org/web/20240420050417/https://www.mitradel.gob.pa/wp-content/uploads/2016/12/código-detrabajo.pdf)
        * [Law #4 of Jun 25, 1990](https://web.archive.org/web/20250429081538/https://s3-legispan.asamblea.gob.pa/legispan/NORMAS/1990/1990/LEY/Administrador%20Legispan_21570_1990_7_2_ASAMBLEA%20LEGISLATIVA_4.pdf)
        * [Law #55 of Nov 7, 2001](https://web.archive.org/web/20230401210617/https://docs.panama.justia.com/federales/leyes/55-de-2001-nov-14-2001.pdf)
    """

    country = "PA"
    default_language = "es"
    # %s (observed).
    observed_label = tr("%s (puente)")
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("en_US", "es", "uk")
    start_year = 1948

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, PanamaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 1972)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("Año Nuevo")))

        if self._year >= 1972:
            # Martyrs' Day.
            self._add_observed(self._add_holiday_jan_9(tr("Día de los Mártires")))

        if self._year <= 1971:
            # Constitution Day.
            self._add_holiday_mar_1(tr("Día de la Constitución"))

        # Carnival Tuesday.
        self._add_carnival_tuesday(tr("Martes de Carnaval"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Labor Day.
        self._add_observed(self._add_labor_day(tr("Día del Trabajo")))

        # Separation Day.
        self._add_observed(self._add_holiday_nov_3(tr("Separación de Panamá de Colombia")))

        # Law #55 of Nov 7, 2001.
        if self._year >= 2002:
            # Colon Day.
            self._add_observed(self._add_holiday_nov_5(tr("Día de Colón")))

        # Cabinet Decree #347 of 1969.
        if self._year >= 1969:
            # Los Santos Uprising Day.
            self._add_observed(self._add_holiday_nov_10(tr("Primer Grito de Independencia")))

        # Independence Day.
        self._add_observed(self._add_holiday_nov_28(tr("Independencia de Panamá de España")))

        # Mother's Day.
        self._add_observed(self._add_holiday_dec_8(tr("Día de la Madre")))

        if self._year >= 2022:
            # National Mourning Day.
            self._add_observed(self._add_holiday_dec_20(tr("Día de Duelo Nacional")))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Navidad")))

    def _populate_bank_holidays(self):
        # Carnival Monday.
        self._add_carnival_monday(tr("Lunes de Carnaval"))

        # National Symbols Day.
        self._add_holiday_nov_4(tr("Día de los Símbolos Patrios"))


class PA(Panama):
    pass


class PAN(Panama):
    pass


class PanamaStaticHolidays:
    # Presidential Inauguration Day.
    presidential_inauguration_day = tr("Toma posesión del Presidente de la república")
    special_public_holidays = {
        2014: (JUL, 1, presidential_inauguration_day),
        2019: (JUL, 1, presidential_inauguration_day),
        2024: (JUL, 1, presidential_inauguration_day),
    }
