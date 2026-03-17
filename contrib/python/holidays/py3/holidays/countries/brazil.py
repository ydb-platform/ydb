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

from holidays.calendars.gregorian import JAN, MAR, SEP, NOV
from holidays.constants import OPTIONAL, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, TUE_WED_THU_TO_NEXT_FRI


class Brazil(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Brazil holidays.

    References:
        * <https://pt.wikipedia.org/wiki/Feriados_no_Brasil>
        * [Decreto n. 155-B, de 14.01.1890](https://web.archive.org/web/20241226133739/https://www2.camara.leg.br/legin/fed/decret/1824-1899/decreto-155-b-14-janeiro-1890-517534-publicacaooriginal-1-pe.html)
        * [Decreto n. 19.488, de 15.12.1930](https://web.archive.org/web/20241006041503/http://camara.leg.br/legin/fed/decret/1930-1939/decreto-19488-15-dezembro-1930-508040-republicacao-85201-pe.html)
        * [Lei n. 662, de 6.04.1949](https://web.archive.org/web/20240913060643/https://www2.camara.leg.br/legin/fed/lei/1940-1949/lei-662-6-abril-1949-347136-publicacaooriginal-1-pl.html)
        * [Lei n. 14.759, de 21.12.2023](https://web.archive.org/web/20250402234552/https://www2.camara.leg.br/legin/fed/lei/2023/lei-14759-21-dezembro-2023-795091-publicacaooriginal-170522-pl.html)
        * São Paulo Capital:
            * [Anexo Único Integrante do Decreto n. 56.756, de 4.01.2016](https://web.archive.org/web/20251224031221/https://legislacao.prefeitura.sp.gov.br/leis/decreto-56756-de-04-de-janeiro-de-2016/anexo/5ec3d7a71411926001a56c56/Anexo%20Único%20do%20Decreto%20nº%2056.756_2016.pdf)
    """

    country = "BR"
    default_language = "pt_BR"
    # Decreto n. 155-B, de 14.01.1890
    start_year = 1890
    subdivisions = (
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
        # Cities.
        "São Paulo Capital",
    )
    subdivisions_aliases = {
        "Acre": "AC",
        "Alagoas": "AL",
        "Amazonas": "AM",
        "Amapá": "AP",
        "Bahia": "BA",
        "Ceará": "CE",
        "Distrito Federal": "DF",
        "Espírito Santo": "ES",
        "Goiás": "GO",
        "Maranhão": "MA",
        "Minas Gerais": "MG",
        "Mato Grosso do Sul": "MS",
        "Mato Grosso": "MT",
        "Pará": "PA",
        "Paraíba": "PB",
        "Pernambuco": "PE",
        "Piauí": "PI",
        "Paraná": "PR",
        "Rio de Janeiro": "RJ",
        "Rio Grande do Norte": "RN",
        "Rondônia": "RO",
        "Roraima": "RR",
        "Rio Grande do Sul": "RS",
        "Santa Catarina": "SC",
        "Sergipe": "SE",
        "São Paulo": "SP",
        "Tocantins": "TO",
    }
    supported_categories = (OPTIONAL, PUBLIC)
    supported_languages = ("en_US", "pt_BR", "uk")

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Universal Fraternization Day.
        self._add_new_years_day(tr("Confraternização Universal"))

        if 1892 <= self._year <= 1930:
            # Republic Constitution Day.
            self._add_holiday_feb_24(tr("Constituição da República"))

        # Good Friday.
        self._add_good_friday(tr("Sexta-feira Santa"))

        if self._year not in {1931, 1932}:
            # Tiradentes' Day.
            self._add_holiday_apr_21(tr("Tiradentes"))

        if self._year >= 1925:
            # Worker's Day.
            self._add_labor_day(tr("Dia do Trabalhador"))

        if self._year <= 1930 or 1936 <= self._year <= 1948:
            # Discovery of Brazil.
            self._add_holiday_may_3(tr("Descobrimento do Brasil"))

        if self._year <= 1930:
            # Abolition of slavery in Brazil.
            self._add_holiday_may_13(tr("Abolição da escravidão no Brasil"))

            # Freedom and Independence of American Peoples.
            self._add_holiday_jul_14(tr("Liberdade e Independência dos Povos Americanos"))

        # Independence Day.
        self._add_holiday_sep_7(tr("Independência do Brasil"))

        if self._year <= 1930 or 1936 <= self._year <= 1948:
            # Discovery of America.
            self._add_columbus_day(tr("Descobrimento da América"))

        if self._year >= 1980:
            # Our Lady of Aparecida.
            self._add_holiday_oct_12(tr("Nossa Senhora Aparecida"))

        # All Souls' Day.
        self._add_all_souls_day(tr("Finados"))

        # Republic Proclamation Day.
        self._add_holiday_nov_15(tr("Proclamação da República"))

        if self._year >= 2024:
            # National Day of Zumbi and Black Awareness.
            self._add_holiday_nov_20(tr("Dia Nacional de Zumbi e da Consciência Negra"))

        if self._year >= 1922:
            # Christmas Day.
            self._add_christmas_day(tr("Natal"))

        if self.subdiv == "São Paulo Capital":
            self._populate_subdiv_sao_paulo_capital_public_holidays()

    def _populate_optional_holidays(self):
        # Carnival.
        name = tr("Carnaval")
        self._add_carnival_monday(name)
        self._add_carnival_tuesday(name)

        # Ash Wednesday.
        self._add_ash_wednesday(tr("Início da Quaresma"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Corpus Christi"))

        # Public Servant's Day.
        self._add_holiday_oct_28(tr("Dia do Servidor Público"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Véspera de Natal"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Véspera de Ano-Novo"))

    def _populate_subdiv_holidays(self):
        # Lei n. 9.093, de 12.09.1995
        # Lei Municipal n. 7.008, de 6.04.1967
        if self._year >= 1996 or self.subdiv == "São Paulo Capital":
            super()._populate_subdiv_holidays()

    def _populate_subdiv_ac_public_holidays(self):
        def get_movable_acre(*args) -> date:
            dt = date(self._year, *args)
            return (
                dt_observed
                if self._year >= 2009
                and (dt_observed := self._get_observed_date(dt, TUE_WED_THU_TO_NEXT_FRI))
                else dt
            )

        if self._year >= 2005:
            # Evangelical Day.
            self._add_holiday(tr("Dia do Evangélico"), get_movable_acre(JAN, 23))

        if self._year >= 2002:
            # International Women's Day.
            self._add_holiday(tr("Dia Internacional da Mulher"), get_movable_acre(MAR, 8))

        # Founding of Acre.
        self._add_holiday_jun_15(tr("Aniversário do Acre"))

        if self._year >= 2004:
            # Amazonia Day.
            self._add_holiday(tr("Dia da Amazônia"), get_movable_acre(SEP, 5))

        # Signing of the Petropolis Treaty.
        self._add_holiday(tr("Assinatura do Tratado de Petrópolis"), get_movable_acre(NOV, 17))

    def _populate_subdiv_al_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("São João"))

        # Saint Peter's Day.
        self._add_saints_peter_and_paul_day(tr("São Pedro"))

        # Political Emancipation of Alagoas.
        self._add_holiday_sep_16(tr("Emancipação Política de Alagoas"))

        if self._year <= 2023:
            # Black Awareness Day.
            self._add_holiday_nov_20(tr("Consciência Negra"))

        if self._year >= 2013:
            # Evangelical Day.
            self._add_holiday_nov_30(tr("Dia do Evangélico"))

    def _populate_subdiv_am_public_holidays(self):
        # Elevation of Amazonas to province.
        self._add_holiday_sep_5(tr("Elevação do Amazonas à categoria de província"))

        if 2010 <= self._year <= 2023:
            # Black Awareness Day.
            self._add_holiday_nov_20(tr("Consciência Negra"))

    def _populate_subdiv_ap_public_holidays(self):
        if self._year >= 2003:
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("São José"))

        if self._year >= 2012:
            # Saint James' Day.
            self._add_saint_james_day(tr("São Tiago"))

        # Creation of the Federal Territory.
        self._add_holiday_sep_13(tr("Criação do Território Federal"))

        if 2008 <= self._year <= 2023:
            # Black Awareness Day.
            self._add_holiday_nov_20(tr("Consciência Negra"))

    def _populate_subdiv_ba_public_holidays(self):
        # Bahia Independence Day.
        self._add_holiday_jul_2(tr("Independência da Bahia"))

    def _populate_subdiv_ce_public_holidays(self):
        self._add_saint_josephs_day(tr("São José"))

        # Abolition of slavery in Ceará.
        self._add_holiday_mar_25(tr("Abolição da escravidão no Ceará"))

        if self._year >= 2004:
            # Our Lady of Assumption.
            self._add_assumption_of_mary_day(tr("Nossa Senhora da Assunção"))

    def _populate_subdiv_df_public_holidays(self):
        # Founding of Brasilia.
        self._add_holiday_apr_21(tr("Fundação de Brasília"))

        # Evangelical Day.
        self._add_holiday_nov_30(tr("Dia do Evangélico"))

    def _populate_subdiv_es_public_holidays(self):
        if self._year >= 2020:
            # Our Lady of Penha.
            self._add_holiday_8_days_past_easter(tr("Nossa Senhora da Penha"))

    def _populate_subdiv_go_public_holidays(self):
        # Foundation of Goiás city.
        self._add_holiday_jul_26(tr("Fundação da cidade de Goiás"))

        # Foundation of Goiânia.
        self._add_holiday_oct_24(tr("Pedra fundamental de Goiânia"))

    def _populate_subdiv_ma_public_holidays(self):
        # Maranhão joining to independence of Brazil.
        self._add_holiday_jul_28(tr("Adesão do Maranhão à independência do Brasil"))

    def _populate_subdiv_mg_public_holidays(self):
        # Tiradentes' Execution.
        self._add_holiday_apr_21(tr("Execução de Tiradentes"))

    def _populate_subdiv_ms_public_holidays(self):
        # State Creation Day.
        self._add_holiday_oct_11(tr("Criação do Estado"))

    def _populate_subdiv_mt_public_holidays(self):
        if 2003 <= self._year <= 2023:
            # Black Awareness Day.
            self._add_holiday_nov_20(tr("Consciência Negra"))

    def _populate_subdiv_pa_public_holidays(self):
        # Grão-Pará joining to independence of Brazil.
        self._add_holiday_aug_15(tr("Adesão do Grão-Pará à independência do Brasil"))

    def _populate_subdiv_pb_public_holidays(self):
        # State Founding Day.
        self._add_holiday_aug_5(tr("Fundação do Estado"))

    def _populate_subdiv_pe_public_holidays(self):
        if self._year >= 2008:
            # Pernambuco Revolution.
            self._add_holiday_1st_sun_of_mar(tr("Revolução Pernambucana"))

    def _populate_subdiv_pi_public_holidays(self):
        # Piauí Day.
        self._add_holiday_oct_19(tr("Dia do Piauí"))

    def _populate_subdiv_pr_public_holidays(self):
        # Political Emancipation of Paraná.
        self._add_holiday_dec_19(tr("Emancipação do Paraná"))

    def _populate_subdiv_rj_public_holidays(self):
        if self._year >= 2008:
            # Saint George's Day.
            self._add_saint_georges_day(tr("São Jorge"))

        if 2002 <= self._year <= 2023:
            # Black Awareness Day.
            self._add_holiday_nov_20(tr("Consciência Negra"))

    def _populate_subdiv_rn_public_holidays(self):
        if self._year >= 2000:
            # Rio Grande do Norte Day.
            self._add_holiday_aug_7(tr("Dia do Rio Grande do Norte"))

        if self._year >= 2007:
            # Uruaçu and Cunhaú Martyrs Day.
            self._add_holiday_oct_3(tr("Mártires de Cunhaú e Uruaçu"))

    def _populate_subdiv_ro_public_holidays(self):
        # State Creation Day.
        self._add_holiday_jan_4(tr("Criação do Estado"))

        if self._year >= 2002:
            # Evangelical Day.
            self._add_holiday_jun_18(tr("Dia do Evangélico"))

    def _populate_subdiv_rr_public_holidays(self):
        # State Creation Day.
        self._add_holiday_oct_5(tr("Criação do Estado"))

    def _populate_subdiv_rs_public_holidays(self):
        # Gaucho Day.
        self._add_holiday_sep_20(tr("Dia do Gaúcho"))

    def _populate_subdiv_sc_public_holidays(self):
        if self._year >= 2004:
            # Santa Catarina State Day.
            name = tr("Dia do Estado de Santa Catarina")
            if self._year >= 2005:
                self._add_holiday_1st_sun_from_aug_11(name)
            else:
                self._add_holiday_aug_11(name)

        # Saint Catherine of Alexandria Day.
        name = tr("Dia de Santa Catarina de Alexandria")
        if 1999 <= self._year != 2004:
            self._add_holiday_1st_sun_from_nov_25(name)
        else:
            self._add_holiday_nov_25(name)

    def _populate_subdiv_se_public_holidays(self):
        # Sergipe Political Emancipation Day.
        self._add_holiday_jul_8(tr("Emancipação política de Sergipe"))

    def _populate_subdiv_sp_public_holidays(self):
        if self._year >= 1997:
            # Constitutionalist Revolution.
            self._add_holiday_jul_9(tr("Revolução Constitucionalista"))

    def _populate_subdiv_sao_paulo_capital_public_holidays(self):
        self._populate_subdiv_sp_public_holidays()

        # Lei Municipal n. 7.008, de 6.04.1967
        if self._year >= 1968:
            # São Paulo City Anniversary.
            self._add_holiday_jan_25(tr("Aniversário da Cidade de São Paulo"))

    def _populate_subdiv_to_public_holidays(self):
        if self._year >= 1998:
            # Autonomy Day.
            self._add_holiday_mar_18(tr("Dia da Autonomia"))

        # Our Lady of Nativity.
        self._add_nativity_of_mary_day(tr("Nossa Senhora da Natividade"))

        # State Creation Day.
        self._add_holiday_oct_5(tr("Criação do Estado"))


class BR(Brazil):
    pass


class BRA(Brazil):
    pass
