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
    TUE_TO_PREV_MON,
    THU_TO_NEXT_FRI,
    SUN_TO_NEXT_MON,
)


class Bolivia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Bolivia holidays.

    References:
        * [Supreme Decree #14260](https://web.archive.org/web/20250428205639/https://bolivia.infoleyes.com/norma/1141/decreto-supremo-14260)
        * [Supreme Decree #21060](https://web.archive.org/web/20250428210031/https://bolivia.infoleyes.com/norma/1211/decreto-supremo-21060)
        * [Supreme Decree #22352](https://web.archive.org/web/20250428210103/https://bolivia.infoleyes.com/norma/1310/decreto-supremo-22352)
        * [Supreme Decree #0173](https://web.archive.org/web/20250428210129/https://bolivia.infoleyes.com/norma/829/decreto-supremo-0173)
        * [Supreme Decree #0405](https://web.archive.org/web/20250428210132/https://bolivia.infoleyes.com/norma/1252/decreto-supremo-0405)
        * [Supreme Decree #1210](https://web.archive.org/web/20250428210105/https://bolivia.infoleyes.com/norma/3756/decreto-supremo-1210)
        * [Supreme Decree #2750](https://web.archive.org/web/20250428210116/https://bolivia.infoleyes.com/norma/6023/decreto-supremo-2750)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Bolivia>
        * <https://web.archive.org/web/20250120171827/https://www.officeholidays.com/countries/bolivia>
    """

    country = "BO"
    default_language = "es"
    # %s (observed).
    observed_label = tr("%s (observado)")
    start_year = 1825
    subdivisions = (
        "B",  # El Beni.
        "C",  # Cochabamba.
        "H",  # Chuquisaca.
        "L",  # La Paz.
        "N",  # Pando.
        "O",  # Oruro.
        "P",  # Potosí.
        "S",  # Santa Cruz.
        "T",  # Tarija.
    )
    subdivisions_aliases = {
        "El Beni": "B",
        "Cochabamba": "C",
        "Chuquisaca": "H",
        "La Paz": "L",
        "Pando": "N",
        "Oruro": "O",
        "Potosí": "P",
        "Santa Cruz": "S",
        "Tarija": "T",
    }
    supported_languages = ("en_US", "es", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        # Supreme Decree #14260.
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 1977)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("Año Nuevo")))

        # Supreme Decree #0405.
        if self._year >= 2010:
            self._add_observed(
                self._add_holiday_jan_22(
                    # Plurinational State Foundation Day.
                    tr("Día de la Creación del Estado Plurinacional de Bolivia")
                )
            )

        # Carnival.
        name = tr("Carnaval")
        self._add_carnival_monday(name)
        self._add_carnival_tuesday(name)

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Labor Day.
        self._add_observed(may_1 := self._add_labor_day(tr("Día del Trabajo")))
        # Supreme Decree #1210.
        if 2012 <= self._year <= 2015:
            self._add_observed(may_1, rule=TUE_TO_PREV_MON + THU_TO_NEXT_FRI)

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Corpus Christi"))

        # Supreme Decree #0173.
        if self._year >= 2009:
            # Aymara New Year.
            self._add_observed(self._add_holiday_jun_21(tr("Año Nuevo Aymara Amazónico")))

        # Independence Day.
        self._add_observed(self._add_holiday_aug_6(tr("Día de la Independencia de Bolivia")))

        if self._year >= 2020:
            # National Dignity Day.
            self._add_holiday_oct_17(tr("Día de la Dignidad Nacional"))

        # Supreme Decree #21060.
        if 1985 <= self._year <= 1988:
            # All Saints' Day.
            self._add_all_saints_day(tr("Día de Todos los Santos"))

        # Supreme Decree #22352.
        if self._year >= 1989:
            # All Souls' Day.
            nov_2 = self._add_all_souls_day(tr("Día de Todos los Difuntos"))
            if self._year <= 2015:
                self._add_observed(nov_2)

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Navidad")))

    def _populate_subdiv_b_public_holidays(self):
        # Beni Day.
        self._add_holiday_nov_18(tr("Día del departamento de Beni"))

    def _populate_subdiv_c_public_holidays(self):
        # Cochabamba Day.
        self._add_holiday_sep_14(tr("Día del departamento de Cochabamba"))

    def _populate_subdiv_h_public_holidays(self):
        # Chuquisaca Day.
        self._add_holiday_may_25(tr("Día del departamento de Chuquisaca"))

    def _populate_subdiv_l_public_holidays(self):
        # La Paz Day.
        self._add_holiday_jul_16(tr("Día del departamento de La Paz"))

    def _populate_subdiv_n_public_holidays(self):
        # Pando Day.
        self._add_holiday_oct_11(tr("Día del departamento de Pando"))

    def _populate_subdiv_p_public_holidays(self):
        # Potosí Day.
        self._add_holiday_nov_10(tr("Día del departamento de Potosí"))

    def _populate_subdiv_o_public_holidays(self):
        # Carnival in Oruro.
        self._add_holiday_51_days_prior_easter(tr("Carnaval de Oruro"))

    def _populate_subdiv_s_public_holidays(self):
        # Santa Cruz Day.
        self._add_holiday_sep_24(tr("Día del departamento de Santa Cruz"))

    def _populate_subdiv_t_public_holidays(self):
        # La Tablada.
        self._add_holiday_apr_15(tr("La Tablada"))


class BO(Bolivia):
    pass


class BOL(Bolivia):
    pass
