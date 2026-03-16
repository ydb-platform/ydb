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

from holidays.constants import OPTIONAL, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Portugal(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Portugal holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Portugal>
        * [Labour Day](https://web.archive.org/web/20240502184140/https://www.e-konomista.pt/dia-do-trabalhador/)
        * Portugal Day - Decreto 17.171
        * Restoration of Independence Day - Gazeta de Lisboa, 8 de Dezembro
            de 1823 (n.º 290), pp. 1789 e 1790
        * Azores:
            * <https://web.archive.org/web/20240811191206/https://files.dre.pt/1s/1980/08/19200/23052305.pdf>
        * Madeira:
            * <https://web.archive.org/web/20230729205620/https://files.dre.pt/1s/1979/11/25900/28782878.pdf>
            * <https://web.archive.org/web/20230730094358/https://files.dre.pt/1s/1989/02/02800/04360436.pdf>
            * <https://web.archive.org/web/20230729210821/https://files.dre.pt/1s/2002/11/258a00/71837183.pdf>
    """

    country = "PT"
    default_language = "pt_PT"
    supported_categories = (OPTIONAL, PUBLIC)
    start_year = 1801
    subdivisions = (
        "01",  # Aveiro.
        "02",  # Beja.
        "03",  # Braga.
        "04",  # Bragança.
        "05",  # Castelo Branco.
        "06",  # Coimbra.
        "07",  # Évora.
        "08",  # Faro.
        "09",  # Guarda.
        "10",  # Leiria.
        "11",  # Lisboa.
        "12",  # Portalegre.
        "13",  # Porto.
        "14",  # Santarém.
        "15",  # Setúbal.
        "16",  # Viana do Castelo.
        "17",  # Vila Real.
        "18",  # Viseu.
        "20",  # Região Autónoma dos Açores.
        "30",  # Região Autónoma da Madeira.
    )
    subdivisions_aliases = {
        "Aveiro": "01",
        "Beja": "02",
        "Braga": "03",
        "Bragança": "04",
        "Castelo Branco": "05",
        "Coimbra": "06",
        "Évora": "07",
        "Faro": "08",
        "Guarda": "09",
        "Leiria": "10",
        "Lisboa": "11",
        "Portalegre": "12",
        "Porto": "13",
        "Santarém": "14",
        "Setúbal": "15",
        "Viana do Castelo": "16",
        "Vila Real": "17",
        "Viseu": "18",
        "Região Autónoma dos Açores": "20",
        "Região Autónoma da Madeira": "30",
    }
    supported_languages = ("en_US", "pt_PT", "uk")
    _deprecated_subdivisions = ("Ext",)

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Ano Novo"))

        # Carnival is no longer a holiday, but some companies let workers off.
        # TODO: recollect the years in which it was a public holiday.

        # Good Friday.
        self._add_good_friday(tr("Sexta-feira Santa"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Páscoa"))

        # Revoked holidays in 2013-2015.
        if self._year <= 2012 or self._year >= 2016:
            # Corpus Christi.
            self._add_corpus_christi_day(tr("Corpo de Deus"))

            if self._year >= 1910:
                # Republic Day.
                self._add_holiday_oct_5(tr("Implantação da República"))

            # All Saints' Day.
            self._add_all_saints_day(tr("Dia de Todos os Santos"))

            if self._year >= 1823:
                # Restoration of Independence Day.
                self._add_holiday_dec_1(tr("Restauração da Independência"))

        if self._year >= 1974:
            # Freedom Day.
            self._add_holiday_apr_25(tr("Dia da Liberdade"))

            # Labor Day.
            self._add_labor_day(tr("Dia do Trabalhador"))

        if self._year >= 1911:
            if 1933 <= self._year <= 1973:
                # Day of Camões, Portugal, and the Portuguese Race.
                name = tr("Dia de Camões, de Portugal e da Raça")
            elif self._year >= 1978:
                # Day of Portugal, Camões, and the Portuguese Communities.
                name = tr("Dia de Portugal, de Camões e das Comunidades Portuguesas")
            else:
                # Portugal Day.
                name = tr("Dia de Portugal")
            self._add_holiday_jun_10(name)

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assunção de Nossa Senhora"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Imaculada Conceição"))

        # Christmas Day.
        self._add_christmas_day(tr("Dia de Natal"))

    def _populate_optional_holidays(self):
        """
        Adds extended days that most people have as a bonus from their
        companies:

        - Carnival
        - the day before and after xmas
        - the day before the new year
        - Lisbon's city holiday
        """

        # TODO: add bridging days:
        # - get Holidays that occur on Tuesday  and add Monday (-1 day)
        # - get Holidays that occur on Thursday and add Friday (+1 day)

        # Carnival.
        self._add_carnival_tuesday(tr("Carnaval"))

        # Saint Anthony's Day.
        self._add_saint_anthonys_day(tr("Dia de Santo António"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Véspera de Natal"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("26 de Dezembro"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Véspera de Ano Novo"))

    def _populate_subdiv_holidays(self):
        if self._year >= 1911:
            super()._populate_subdiv_holidays()

        if self.subdiv == "Ext":
            self._populate_optional_holidays()

    def _populate_subdiv_01_public_holidays(self):
        # Saint Joanna's Day.
        self._add_holiday_may_12(tr("Dia de Santa Joana"))

    def _populate_subdiv_02_public_holidays(self):
        # Ascension Day.
        self._add_ascension_thursday(tr("Quinta-feira da Ascensão"))

    def _populate_subdiv_03_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("Dia de São João"))

    def _populate_subdiv_04_public_holidays(self):
        # Feast of Our Lady of Graces.
        self._add_holiday_aug_22(tr("Dia de Nossa Senhora das Graças"))

    def _populate_subdiv_05_public_holidays(self):
        # Feast of Our Lady of Mércoles.
        self._add_holiday_16_days_past_easter(tr("Dia de Nossa Senhora de Mércoles"))

    def _populate_subdiv_06_public_holidays(self):
        # Saint Elizabeth's Day.
        self._add_holiday_jul_4(tr("Dia de Santa Isabel"))

    def _populate_subdiv_07_public_holidays(self):
        # Saint Peter's Day.
        self._add_saints_peter_and_paul_day(tr("Dia de São Pedro"))

    def _populate_subdiv_08_public_holidays(self):
        # Municipal Holiday of Faro.
        self._add_holiday_sep_7(tr("Dia do Município de Faro"))

    def _populate_subdiv_09_public_holidays(self):
        # Municipal Holiday of Guarda.
        self._add_holiday_nov_27(tr("Dia do Município da Guarda"))

    def _populate_subdiv_10_public_holidays(self):
        # Municipal Holiday of Leiria.
        self._add_holiday_may_22(tr("Dia do Município de Leiria"))

    def _populate_subdiv_11_public_holidays(self):
        # Saint Anthony's Day.
        self._add_saint_anthonys_day(tr("Dia de Santo António"))

    def _populate_subdiv_12_public_holidays(self):
        # Municipal Holiday of Portalegre.
        self._add_holiday_may_23(tr("Dia do Município de Portalegre"))

    def _populate_subdiv_13_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("Dia de São João"))

    def _populate_subdiv_14_public_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Dia de São José"))

    def _populate_subdiv_15_public_holidays(self):
        # Bocage Day.
        self._add_holiday_sep_15(tr("Dia de Bocage"))

    def _populate_subdiv_16_public_holidays(self):
        # Feast of Our Lady of Sorrows.
        self._add_holiday_aug_20(tr("Dia de Nossa Senhora da Agonia"))

    def _populate_subdiv_17_public_holidays(self):
        # Saint Anthony's Day.
        self._add_saint_anthonys_day(tr("Dia de Santo António"))

    def _populate_subdiv_18_public_holidays(self):
        # Saint Matthew's Day.
        self._add_holiday_sep_21(tr("Dia de São Mateus"))

    def _populate_subdiv_20_public_holidays(self):
        if self._year >= 1981:
            # Day of the Autonomous Region of the Azores.
            self._add_whit_monday(tr("Dia da Região Autónoma dos Açores"))

    def _populate_subdiv_30_public_holidays(self):
        if self._year >= 1979:
            self._add_holiday_jul_1(
                # Day of the Autonomous Region of Madeira and the Madeiran Communities.
                tr("Dia da Região Autónoma da Madeira e das Comunidades Madeirenses")
                if self._year >= 1989
                # Day of the Autonomous Region of Madeira.
                else tr("Dia da Região Autónoma da Madeira")
            )

        if self._year >= 2002:
            # 1st Octave.
            self._add_christmas_day_two(tr("Primeira Oitava"))


class PT(Portugal):
    pass


class PRT(Portugal):
    pass
