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


class CaboVerde(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Cabo Verde holidays.

    References:
        * [Public holidays in Cape Verde](https://en.wikipedia.org/wiki/Public_holidays_in_Cape_Verde)
        * [Legislação Municipal Cabo-Verdiana](https://web.archive.org/web/20180820172827/http://www.interface.gov.cv/index.php?option=com_docman&task=doc_download&gid=12&Itemid=314)
        * [Feriados Bancários - Banco de Cabo Verde](https://web.archive.org/web/20250629042854/https://www.bcv.cv/pt/SistemadePagamentos/feriados_bancarios/Paginas/FeriadosBancarios.aspx)
        * [Feriados Públicos - Feel Cabo Verde](https://web.archive.org/web/20250419201823/https://feelcaboverde.com/feriados-publicos/)
        * [Public Holidays - Feel Cape Verde](https://web.archive.org/web/20250419202739/https://feelcaboverde.com/en/public-holidays-cape-verde/)
        * [Democracy Day](https://web.archive.org/web/20250420093850/https://cmsv.cv/dia-da-liberdade-e-democracia-de-cabo-verde/)
    """

    country = "CV"
    default_language = "pt_CV"
    start_year = 1976
    subdivisions = (
        "BR",  # Brava.
        "BV",  # Boa Vista.
        "CA",  # Santa Catarina.
        "CF",  # Santa Catarina do Fogo.
        "CR",  # Santa Cruz.
        "MA",  # Maio.
        "MO",  # Mosteiros.
        "PA",  # Paul.
        "PN",  # Porto Novo.
        "PR",  # Praia.
        "RB",  # Ribeira Brava
        "RG",  # Ribeira Grande.
        "RS",  # Ribeira Grande de Santiago.
        "SD",  # São Domingos.
        "SF",  # São Filipe.
        "SL",  # Sal.
        "SM",  # São Miguel.
        "SO",  # São Lourenço dos Órgãos.
        "SS",  # São Salvador do Mundo.
        "SV",  # São Vicente.
        "TA",  # Tarrafal.
        "TS",  # Tarrafal de São Nicolau.
    )

    subdivisions_aliases = {
        "Brava": "BR",
        "Boa Vista": "BV",
        "Santa Catarina": "CA",
        "Santa Catarina do Fogo": "CF",
        "Santa Cruz": "CR",
        "Maio": "MA",
        "Mosteiros": "MO",
        "Paul": "PA",
        "Porto Novo": "PN",
        "Praia": "PR",
        "Ribeira Brava": "RB",
        "Ribeira Grande": "RG",
        "Ribeira Grande de Santiago": "RS",
        "São Domingos": "SD",
        "São Filipe": "SF",
        "Sal": "SL",
        "São Miguel": "SM",
        "São Lourenço dos Órgãos": "SO",
        "São Salvador do Mundo": "SS",
        "São Vicente": "SV",
        "Tarrafal": "TA",
        "Tarrafal de São Nicolau": "TS",
    }
    supported_categories = (OPTIONAL, PUBLIC)
    supported_languages = ("de", "en_US", "es", "fr", "pt_CV")

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Ano Novo"))

        # Law # 95/V/99 of March 22nd.
        if self._year >= 2000:
            # Democracy and Freedom Day.
            self._add_holiday_jan_13(tr("Dia da Liberdade e da Democracia"))

        # National Heroes Day.
        self._add_holiday_jan_20(tr("Dia da Nacionalidade e dos Heróis Nacionais"))

        # Ash Wednesday.
        self._add_ash_wednesday(tr("Quarta-feira de Cinzas"))

        # Good Friday.
        self._add_good_friday(tr("Sexta-feira Santa"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Páscoa"))

        # Worker's Day.
        self._add_labor_day(tr("Dia do Trabalhador"))

        # Law # 69/VI/2005 of May 31st.
        if self._year >= 2005:
            # International Children's Day.
            self._add_childrens_day(tr("Dia Mundial da Criança"))

        # Independence Day.
        self._add_holiday_jul_5(tr("Dia da Independência Nacional"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Dia da Assunção"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Dia de Todos os Santos"))

        # Christmas Day.
        self._add_christmas_day(tr("Dia do Natal"))

    def _populate_optional_holidays(self):
        # Holy Thursday.
        self._add_holy_thursday(tr("Quinta-Feira Santa"))

        # Mother's Day.
        self._add_holiday_2nd_sun_of_may(tr("Dia das Mães"))

        # Father's Day.
        self._add_holiday_3rd_sun_of_jun(tr("Dia dos Pais"))

    def _populate_subdiv_br_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Brava Municipality Day.
            self._add_holiday_jun_24(tr("Dia do Município da Brava"))

    def _populate_subdiv_bv_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Boa Vista Municipality Day.
            self._add_holiday_jul_4(tr("Dia do Município da Boa Vista"))

    def _populate_subdiv_ca_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1982:
            # Santa Catarina de Santiago Municipality Day.
            self._add_holiday_nov_25(tr("Dia do Município de Santa Catarina de Santiago"))

    def _populate_subdiv_cf_public_holidays(self):
        # Law # 66/VI/2005 of May 9th.
        if self._year >= 2005:
            # Santa Catarina do Fogo Municipality Day.
            self._add_holiday_nov_25(tr("Dia do Município de Santa Catarina do Fogo"))

    def _populate_subdiv_cr_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Santa Cruz Municipality Day.
            self._add_holiday_jul_25(tr("Dia do Município de Santa Cruz"))

    def _populate_subdiv_ma_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Maio Municipality Day.
            self._add_holiday_sep_8(tr("Dia do Município do Maio"))

    def _populate_subdiv_mo_public_holidays(self):
        # Law # 23/IV/91 of December 30th.
        if self._year >= 1992:
            # Mosteiros Municipality Day.
            self._add_holiday_aug_15(tr("Dia do Município dos Mosteiros"))

    def _populate_subdiv_pa_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Santo Antão Island Day.
            self._add_holiday_jan_17(tr("Dia da Ilha de Santo Antão"))

            # Paúl Municipality Day.
            self._add_holiday_jun_13(tr("Dia do Município do Paúl"))

    def _populate_subdiv_pn_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Santo Antão Island Day.
            self._add_holiday_jan_17(tr("Dia da Ilha de Santo Antão"))

            # Porto Novo Municipality Day.
            self._add_holiday_sep_2(tr("Dia do Município do Porto Novo"))

    def _populate_subdiv_pr_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Praia Municipality Day.
            self._add_holiday_may_19(tr("Dia do Município da Praia"))

    def _populate_subdiv_rb_public_holidays(self):
        # Law # 67/VI/2005 of May 9th.
        if self._year >= 2005:
            # Ribeira Brava Municipality Day.
            self._add_holiday_dec_6(tr("Dia do Município de Ribeira Brava"))

    def _populate_subdiv_rg_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Santo Antão Island Day.
            self._add_holiday_jan_17(tr("Dia da Ilha de Santo Antão"))

            # Ribeira Grande Municipality Day.
            self._add_holiday_may_7(tr("Dia do Município de Ribeira Grande"))

    def _populate_subdiv_rs_public_holidays(self):
        # Law # 63/VI/2005 of May 9th.
        if self._year >= 2006:
            # Ribeira Grande de Santiago Municipality Day.
            self._add_holiday_jan_31(tr("Dia do Município de Ribeira Grande de Santiago"))

    def _populate_subdiv_sd_public_holidays(self):
        # Law # 96/IV/93 of December 31st.
        if self._year >= 1994:
            # São Domingos Municipality Day.
            self._add_holiday_mar_13(tr("Dia do Município de São Domingos"))

    def _populate_subdiv_sf_public_holidays(self):
        # Law # 23/IV/91 of December 30th.
        if self._year >= 1992:
            # São Filipe Municipality Day.
            self._add_holiday_may_1(tr("Dia do Município de São Filipe"))

    def _populate_subdiv_sl_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Sal Municipality Day.
            self._add_holiday_sep_15(tr("Dia do Município do Sal"))

    def _populate_subdiv_sm_public_holidays(self):
        # Law # 11/V/96 of November 11th.
        if self._year >= 1997:
            # São Miguel Municipality Day.
            self._add_holiday_sep_29(tr("Dia do Município de São Miguel"))

    def _populate_subdiv_so_public_holidays(self):
        # Law # 64/VI/2005 of May 9th.
        if self._year >= 2005:
            # São Lourenço dos Órgãos Municipality Day.
            self._add_holiday_may_9(tr("Dia do Município de São Lourenço dos Órgãos"))

    def _populate_subdiv_ss_public_holidays(self):
        # Law # 65/VI/2005 of May 9th.
        if self._year >= 2005:
            # São Salvador do Mundo Municipality Day.
            self._add_holiday_jul_19(tr("Dia do Município de São Salvador do Mundo"))

    def _populate_subdiv_sv_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # São Vicente Municipality Day.
            self._add_holiday_jan_22(tr("Dia do Município de São Vicente"))

            # Carnival Tuesday.
            self._add_carnival_tuesday(tr("Terça-feira de Carnaval"))

    def _populate_subdiv_ta_public_holidays(self):
        # Law # 93/82 of November 6th.
        if self._year >= 1983:
            # Tarrafal de Santiago Municipality Day.
            self._add_holiday_jan_15(tr("Dia do Município do Tarrafal de Santiago"))

    def _populate_subdiv_ts_public_holidays(self):
        # Law # 67/VI/2005 of May 9th.
        if self._year >= 2005:
            # Tarrafal de São Nicolau Municipality Day.
            self._add_holiday_aug_2(tr("Dia do Município do Tarrafal de São Nicolau"))


class CV(CaboVerde):
    pass


class CPV(CaboVerde):
    pass
