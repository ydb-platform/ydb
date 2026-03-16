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

from holidays.calendars.gregorian import MAR, MAY, JUN, JUL, AUG, SEP, OCT, NOV
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Italy(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Italy holidays.

    References:
        * <https://it.wikipedia.org/wiki/Festività_in_Italia>
        * <https://web.archive.org/web/20260115032712/https://www.normattiva.it/>
        * [Royal Decree 5342 of Oct 17, 1869](https://web.archive.org/web/20240405131328/https://www.gazzettaufficiale.it/eli/gu/1869/11/23/320/sg/pdf)
        * [Law 1968 of Jun 23, 1874](https://web.archive.org/web/20240101200400/https://www.gazzettaufficiale.it/eli/gu/1874/07/11/164/sg/pdf)
        * [Law 401 of Jul 19, 1895](https://web.archive.org/web/20240405131313/https://www.gazzettaufficiale.it/eli/gu/1895/07/19/169/sg/pdf)
        * [Royal Decree 1027 of Aug 4, 1913](https://web.archive.org/web/20240404233907/https://www.gazzettaufficiale.it/eli/gu/1913/09/10/211/sg/pdf)
        * [Royal Decree-Law 1354 of Oct 23, 1922](https://web.archive.org/web/20240101212042/https://www.gazzettaufficiale.it/eli/gu/1922/10/26/252/sg/pdf)
        * [Royal Decree-Law 833 of Apr 19, 1923](https://web.archive.org/web/20240423171524/https://www.gazzettaufficiale.it/eli/gu/1923/04/20/93/sg/pdf)
        * [Royal Decree-Law 2859 of Dec 30, 1923](https://web.archive.org/web/20250125021721/https://www.gazzettaufficiale.it/eli/gu/1924/01/15/12/sg/pdf)
        * [Royal Decree-Law 1779 of Oct 21, 1926](https://web.archive.org/web/20221021200745/https://www.gazzettaufficiale.it/eli/gu/1926/10/23/247/sg/pdf)
        * [Royal Decree-Law 1922 of Oct 23, 1927](https://web.archive.org/web/20250124092407/https://www.gazzettaufficiale.it/eli/gu/1927/10/25/247/sg/pdf)
        * [Law 2765 of Dec 6, 1928](https://web.archive.org/web/20251026200018/https://www.gazzettaufficiale.it/eli/gu/1928/12/19/294/sg/pdf)
        * [Royal Decree-Law 1827 of Oct 13, 1929](https://web.archive.org/web/20250124071534/https://www.gazzettaufficiale.it/eli/gu/1929/10/22/246/sg/pdf)
        * [Law 1726 of Dec 27, 1930](https://web.archive.org/web/20240406172031/https://www.gazzettaufficiale.it/eli/gu/1931/01/13/9/sg/pdf)
        * [Law 661 of May 5, 1939](https://web.archive.org/web/20240405131312/https://www.gazzettaufficiale.it/eli/gu/1939/05/08/109/sg/pdf)
        * [Royal Decree-Law 781 of Jul 24, 1941](https://web.archive.org/web/20250123213649/https://www.gazzettaufficiale.it/eli/gu/1941/08/16/192/sg/pdf)
        * [Legislative Decree 185 of Apr 22, 1946](https://web.archive.org/web/20230424155628/http://www.gazzettaufficiale.it/eli/gu/1946/04/24/96/sg/pdf)
        * [Legislative Decree 387 of May 28, 1947](https://web.archive.org/web/20231117073456/http://www.gazzettaufficiale.it/eli/gu/1947/05/31/123/sg/pdf)
        * [Law 260 of May 27, 1949](https://web.archive.org/web/20250806020941/https://www.gazzettaufficiale.it/eli/gu/1949/05/31/124/sg/pdf)
        * [Law 54 of Mar 5, 1977](https://web.archive.org/web/20250426211938/https://www.gazzettaufficiale.it/eli/gu/1977/03/07/63/sg/pdf)
        * [Presidential Decree 792 of Dec 28, 1985](https://web.archive.org/web/20230421223502/https://www.gazzettaufficiale.it/eli/gu/1985/12/31/306/sg/pdf)
        * [Law 336 of Nov 20, 2000](https://web.archive.org/web/20230604111415/https://www.gazzettaufficiale.it/eli/gu/2000/11/22/273/sg/pdf)
        * [Law 151 of Oct 8, 2025](https://web.archive.org/web/20251027040938/https://www.gazzettaufficiale.it/eli/gu/2025/10/10/236/sg/pdf)
        * [Provinces holidays](https://it.wikipedia.org/wiki/Santi_patroni_cattolici_delle_città_capoluogo_di_provincia_italiane)
        * [Bolzano Province Law 36 of Oct 16, 1992](https://web.archive.org/web/20260121223715/https://www.edizionieuropee.it/LAW/HTML/103/bz2_01_100.html)
    """

    country = "IT"
    default_language = "it_IT"
    # Royal Decree 5342 of Oct 17, 1869.
    start_year = 1870
    subdivisions = (
        # Provinces.
        "AG",  # Agrigento.
        "AL",  # Alessandria.
        "AN",  # Ancona.
        "AO",  # Aosta (deprecated).
        "AP",  # Ascoli Piceno.
        "AQ",  # L'Aquila.
        "AR",  # Arezzo.
        "AT",  # Asti.
        "AV",  # Avellino.
        "BA",  # Bari.
        "BG",  # Bergamo.
        "BI",  # Biella.
        "BL",  # Belluno.
        "BN",  # Benevento.
        "BO",  # Bologna.
        "BR",  # Brindisi.
        "BS",  # Brescia.
        "BT",  # Barletta-Andria-Trani.
        "BZ",  # Bolzano.
        "CA",  # Cagliari.
        "CB",  # Campobasso.
        "CE",  # Caserta.
        "CH",  # Chieti.
        "CL",  # Caltanissetta.
        "CN",  # Cuneo.
        "CO",  # Como.
        "CR",  # Cremona.
        "CS",  # Cosenza.
        "CT",  # Catania.
        "CZ",  # Catanzaro.
        "EN",  # Enna.
        "FC",  # Forlì-Cesena.
        "FE",  # Ferrara.
        "FG",  # Foggia.
        "FI",  # Firenze.
        "FM",  # Fermo.
        "FR",  # Frosinone.
        "GE",  # Genova.
        "GO",  # Gorizia.
        "GR",  # Grosseto.
        "IM",  # Imperia.
        "IS",  # Isernia.
        "KR",  # Crotone.
        "LC",  # Lecco.
        "LE",  # Lecce.
        "LI",  # Livorno.
        "LO",  # Lodi.
        "LT",  # Latina.
        "LU",  # Lucca.
        "MB",  # Monza e Brianza.
        "MC",  # Macerata.
        "ME",  # Messina.
        "MI",  # Milano.
        "MN",  # Mantova.
        "MO",  # Modena.
        "MS",  # Massa-Carrara.
        "MT",  # Matera.
        "NA",  # Napoli.
        "NO",  # Novara.
        "NU",  # Nuoro.
        "OR",  # Oristano.
        "PA",  # Palermo.
        "PC",  # Piacenza.
        "PD",  # Padova.
        "PE",  # Pescara.
        "PG",  # Perugia.
        "PI",  # Pisa.
        "PN",  # Pordenone.
        "PO",  # Prato.
        "PR",  # Parma.
        "PT",  # Pistoia.
        "PU",  # Pesaro e Urbino.
        "PV",  # Pavia.
        "PZ",  # Potenza.
        "RA",  # Ravenna.
        "RC",  # Reggio Calabria.
        "RE",  # Reggio Emilia.
        "RG",  # Ragusa.
        "RI",  # Rieti.
        "RM",  # Roma.
        "RN",  # Rimini.
        "RO",  # Rovigo.
        "SA",  # Salerno.
        "SI",  # Siena.
        "SO",  # Sondrio.
        "SP",  # La Spezia.
        "SR",  # Siracusa.
        "SS",  # Sassari.
        "SU",  # Sud Sardegna.
        "SV",  # Savona.
        "TA",  # Taranto.
        "TE",  # Teramo.
        "TN",  # Trento.
        "TO",  # Torino.
        "TP",  # Trapani.
        "TR",  # Terni.
        "TS",  # Trieste.
        "TV",  # Treviso.
        "UD",  # Udine.
        "VA",  # Varese.
        "VB",  # Verbano-Cusio-Ossola.
        "VC",  # Vercelli.
        "VE",  # Venezia.
        "VI",  # Vicenza.
        "VR",  # Verona.
        "VT",  # Viterbo.
        "VV",  # Vibo Valentia.
        # Cities.
        "Andria",
        "Barletta",
        "Cesena",
        "Forli",
        "Pesaro",
        "Trani",
        "Urbino",
    )
    subdivisions_aliases = {
        # Provinces.
        "Agrigento": "AG",
        "Alessandria": "AL",
        "Ancona": "AN",
        "Aosta": "AO",
        "Ascoli Piceno": "AP",
        "L'Aquila": "AQ",
        "Arezzo": "AR",
        "Asti": "AT",
        "Avellino": "AV",
        "Bari": "BA",
        "Bergamo": "BG",
        "Biella": "BI",
        "Belluno": "BL",
        "Benevento": "BN",
        "Bologna": "BO",
        "Brindisi": "BR",
        "Brescia": "BS",
        "Barletta-Andria-Trani": "BT",
        "Bolzano": "BZ",
        "Cagliari": "CA",
        "Campobasso": "CB",
        "Caserta": "CE",
        "Chieti": "CH",
        "Caltanissetta": "CL",
        "Cuneo": "CN",
        "Como": "CO",
        "Cremona": "CR",
        "Cosenza": "CS",
        "Catania": "CT",
        "Catanzaro": "CZ",
        "Enna": "EN",
        "Forli-Cesena": "FC",
        "Forlì-Cesena": "FC",
        "Ferrara": "FE",
        "Foggia": "FG",
        "Firenze": "FI",
        "Fermo": "FM",
        "Frosinone": "FR",
        "Genova": "GE",
        "Gorizia": "GO",
        "Grosseto": "GR",
        "Imperia": "IM",
        "Isernia": "IS",
        "Crotone": "KR",
        "Lecco": "LC",
        "Lecce": "LE",
        "Livorno": "LI",
        "Lodi": "LO",
        "Latina": "LT",
        "Lucca": "LU",
        "Monza e Brianza": "MB",
        "Macerata": "MC",
        "Messina": "ME",
        "Milano": "MI",
        "Mantova": "MN",
        "Modena": "MO",
        "Massa-Carrara": "MS",
        "Matera": "MT",
        "Napoli": "NA",
        "Novara": "NO",
        "Nuoro": "NU",
        "Oristano": "OR",
        "Palermo": "PA",
        "Piacenza": "PC",
        "Padova": "PD",
        "Pescara": "PE",
        "Perugia": "PG",
        "Pisa": "PI",
        "Pordenone": "PN",
        "Prato": "PO",
        "Parma": "PR",
        "Pistoia": "PT",
        "Pesaro e Urbino": "PU",
        "Pavia": "PV",
        "Potenza": "PZ",
        "Ravenna": "RA",
        "Reggio Calabria": "RC",
        "Reggio Emilia": "RE",
        "Ragusa": "RG",
        "Rieti": "RI",
        "Roma": "RM",
        "Rimini": "RN",
        "Rovigo": "RO",
        "Salerno": "SA",
        "Siena": "SI",
        "Sondrio": "SO",
        "La Spezia": "SP",
        "Siracusa": "SR",
        "Sassari": "SS",
        "Sud Sardegna": "SU",
        "Savona": "SV",
        "Taranto": "TA",
        "Teramo": "TE",
        "Trento": "TN",
        "Torino": "TO",
        "Trapani": "TP",
        "Terni": "TR",
        "Trieste": "TS",
        "Treviso": "TV",
        "Udine": "UD",
        "Varese": "VA",
        "Verbano-Cusio-Ossola": "VB",
        "Vercelli": "VC",
        "Venezia": "VE",
        "Vicenza": "VI",
        "Verona": "VR",
        "Viterbo": "VT",
        "Vibo Valentia": "VV",
        # Cities.
        "Forlì": "Forli",
    }
    supported_languages = ("en_US", "it_IT")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=ItalyStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Established by Law 1968 of June 23, 1874.
        if self._year >= 1875:
            # New Year's Day.
            self._add_new_years_day(tr("Capodanno"))

        # Established by Royal Decree 5342 of Oct 17, 1869.
        # Abolished by Law 54 of Mar 5, 1977.
        # Restored by Presidential Decree 792 of Dec 28, 1985.
        if self._year <= 1977 or self._year >= 1986:
            # Epiphany.
            self._add_epiphany_day(tr("Epifania"))

        # Established by Law 2765 of Dec 6, 1928.
        # Abolished by Law 54 of Mar 5, 1977.
        if 1929 <= self._year <= 1976:
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("San Giuseppe"))

        # Established by Law 260 of May 27, 1949.
        if self._year >= 1950:
            # Easter Monday.
            self._add_easter_monday(tr("Lunedì dell'Angelo"))

        # Established by Royal Decree-Law 833 of Apr 19, 1923.
        # Suspended by Royal Decree-Law 781 of Jul 24, 1941.
        if 1923 <= self._year <= 1941:
            # Foundation of Rome.
            self._add_holiday_apr_21(tr("Natale di Roma"))

        # Established by Legislative Decree 185 of Apr 22, 1946.
        # Confirmed by Law 260 of May 27, 1949.
        if self._year >= 1946:
            # Liberation Day.
            self._add_holiday_apr_25(tr("Anniversario della Liberazione"))

            # Labor Day.
            self._add_labor_day(tr("Festa del Lavoro"))

        # Established by Legislative Decree 185 of Apr 22, 1946.
        # Abolished by Law 260 of May 27, 1949.
        if 1946 <= self._year <= 1949:
            # Anniversary of Victory in Europe.
            self._add_world_war_two_victory_day(tr("Anniversario della Vittoria in Europa"))

        # Established by Law 661 of May 5, 1939.
        # Suspended by Royal Decree-Law 781 of Jul 24, 1941.
        if 1939 <= self._year <= 1941:
            # Anniversary of the founding of the Empire.
            self._add_holiday_may_9(tr("Anniversario della fondazione dell'Impero"))

        # 1947 - Legislative Decree 387 of May 28, 1947.
        # 1948 - Decree of May 19, 1948 (?).
        # Established by Law 260 of May 27, 1949.
        # Made movable by Law 54 of Mar 5, 1977.
        # Made fixed by Law 336 of Nov 20, 2000.
        if self._year >= 1947:
            # Republic Day.
            name = tr("Festa della Repubblica")
            if 1977 <= self._year <= 2000:
                self._add_holiday_1st_sun_of_jun(name)
            else:
                self._add_holiday_jun_2(name)

        # Established by Royal Decree 5342 of Oct 17, 1869.
        # Abolished by Law 54 of Mar 5, 1977.
        if self._year <= 1976:
            # Ascension Day.
            self._add_ascension_thursday(tr("Ascensione"))

        # Established by Royal Decree 5342 of Oct 17, 1869.
        # Abolished by Royal Decree 1027 of Aug 4, 1913.
        # Restored by Royal Decree-Law 2859 of Dec 30, 1923.
        # Abolished by Law 54 of Mar 5, 1977.
        if self._year <= 1913 or 1924 <= self._year <= 1976:
            # Saints Peter and Paul.
            self._add_saints_peter_and_paul_day(tr("Santi Apostoli Pietro e Paolo"))

            # Corpus Christi.
            self._add_corpus_christi_day(tr("Corpus Domini"))

        # Established by Royal Decree 5342 of Oct 17, 1869.

        # Assumption Of Mary Day.
        self._add_assumption_of_mary_day(tr("Assunzione della Beata Vergine Maria"))

        # Established by Royal Decree 5342 of Oct 17, 1869.
        # Abolished by Royal Decree 1027 of Aug 4, 1913.
        if self._year <= 1912:
            # Nativity of Mary.
            self._add_nativity_of_mary_day(tr("Natività della Beata Vergine Maria"))

        # Established by Law 401 of Jul 19, 1895.
        # Abolished by Law 1726 of Dec 27, 1930.
        if 1895 <= self._year <= 1930:
            # Anniversary of the capture of Rome.
            self._add_holiday_sep_20(tr("Anniversario della Presa di Roma"))

        # Established by Law 151 of Oct 8, 2025.
        if self._year >= 2026:
            self._add_holiday_oct_4(
                # Saint Francis of Assisi, Patron Saint of Italy.
                tr("Festa nazionale di San Francesco d'Assisi, patrono d'Italia")
            )

        # Established by Royal Decree-Law 1779 of Oct 21, 1926.
        # Date shifted in 1927 by Royal Decree-Law 1922 of Oct 23, 1927.
        # Date changed in 1929 by Royal Decree-Law 1827 of Oct 13, 1929.
        # Suspended by Royal Decree-Law 781 of Jul 24, 1941.
        if 1926 <= self._year <= 1940:
            dates_obs = {
                1927: (OCT, 30),
                1929: (OCT, 27),
            }
            self._add_holiday(
                # Anniversary of the March on Rome.
                tr("Anniversario della Marcia su Roma"),
                dates_obs.get(self._year, (OCT, 28)),
            )

        # Established by Royal Decree 5342 of Oct 17, 1869.

        # All Saints' Day.
        self._add_all_saints_day(tr("Ognissanti"))

        # Established by Royal Decree-Law 1354 of Oct 23, 1922.
        # Date shifted in 1927 by Royal Decree-Law 1922 of Oct 23, 1927.
        # Date changed in 1929 by Royal Decree-Law 1827 of Oct 13, 1929.
        # Suspended by Royal Decree-Law 781 of Jul 24, 1941.
        # Restored by Legislative Decree 185 of Apr 22, 1946.
        # Renamed by Law 260 of May 27, 1949.
        # Made movable by Law 54 of Mar 5, 1977.
        if 1922 <= self._year <= 1940 or self._year >= 1946:
            name = (
                # National Unity Day.
                tr("Giorno dell'unità nazionale")
                if self._year >= 1950
                # Victory Day.
                else tr("Anniversario della Vittoria")
            )
            if self._year >= 1977:
                self._add_holiday_1st_sun_of_nov(name)
            else:
                dates_obs = {
                    1927: (NOV, 6),
                    1929: (NOV, 3),
                }
                self._add_holiday(name, dates_obs.get(self._year, (NOV, 4)))

        # Established by Royal Decree 5342 of Oct 17, 1869.
        # Abolished by Royal Decree 1027 of Aug 4, 1913.
        # Restored by Royal Decree-Law 2859 of Dec 30, 1923.
        if self._year <= 1912 or self._year >= 1924:
            # Immaculate Conception.
            self._add_immaculate_conception_day(tr("Immacolata Concezione"))

        # Established by Royal Decree 5342 of Oct 17, 1869.

        # Christmas Day.
        self._add_christmas_day(tr("Natale"))

        # Established by Law 260 of May 27, 1949.
        if self._year >= 1949:
            # Saint Stephen's Day.
            self._add_christmas_day_two(tr("Santo Stefano"))

    def _populate_subdiv_holidays(self):
        # The municipal festivals of the patron saint (Festa del santo patrono).
        # Established by Royal Decree 5342 of Oct 17, 1869.
        # Abolished by Royal Decree 1027 of Aug 4, 1913.
        # Restored by Law 260 of May 27, 1949.
        if self._year <= 1913 or self._year >= 1950:
            super()._populate_subdiv_holidays()

    def _populate_subdiv_ag_public_holidays(self):
        # Saint Gerland's Day.
        self._add_holiday_feb_25(tr("San Gerlando"))

    def _populate_subdiv_al_public_holidays(self):
        # Saint Baudolino's Day.
        self._add_holiday_nov_10(tr("San Baudolino"))

    def _populate_subdiv_an_public_holidays(self):
        # Saint Cyriacus's Day.
        self._add_holiday_may_4(tr("San Ciriaco"))

    def _populate_subdiv_ao_public_holidays(self):
        # Saint Grat's Day.
        self._add_holiday_sep_7(tr("San Grato"))

    def _populate_subdiv_ap_public_holidays(self):
        # Saint Emidius's Day.
        self._add_holiday_aug_5(tr("Sant'Emidio"))

    def _populate_subdiv_aq_public_holidays(self):
        # Saint Maximus of Aveia's Day.
        self._add_holiday_jun_10(tr("San Massimo d'Aveia"))

    def _populate_subdiv_ar_public_holidays(self):
        # Saint Donatus of Arezzo's Day.
        self._add_holiday_aug_7(tr("San Donato d'Arezzo"))

    def _populate_subdiv_at_public_holidays(self):
        # Saint Secundus of Asti's Day.
        self._add_holiday_1st_tue_of_may(tr("San Secondo di Asti"))

    def _populate_subdiv_av_public_holidays(self):
        # Saint Modestinus's Day.
        self._add_holiday_feb_14(tr("San Modestino"))

    def _populate_subdiv_ba_public_holidays(self):
        # Saint Nicholas's Day.
        self._add_holiday_dec_6(tr("San Nicola"))

    def _populate_subdiv_bg_public_holidays(self):
        # Saint Alexander of Bergamo's Day.
        self._add_holiday_aug_26(tr("Sant'Alessandro di Bergamo"))

    def _populate_subdiv_bi_public_holidays(self):
        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Santo Stefano"))

    def _populate_subdiv_bl_public_holidays(self):
        # Saint Martin's Day.
        self._add_saint_martins_day(tr("San Martino"))

    def _populate_subdiv_bn_public_holidays(self):
        # Saint Bartholomew's Day.
        self._add_holiday_aug_24(tr("San Bartolomeo apostolo"))

    def _populate_subdiv_bo_public_holidays(self):
        # Saint Petronius's Day.
        self._add_holiday_oct_4(tr("San Petronio"))

    def _populate_subdiv_br_public_holidays(self):
        # Saint Theodore of Amasea and Saint Lawrence of Brindisi's Day.
        self._add_holiday_1st_sun_of_sep(tr("San Teodoro d'Amasea e San Lorenzo da Brindisi"))

    def _populate_subdiv_bs_public_holidays(self):
        # Saints Faustinus and Jovita's Day.
        self._add_holiday_feb_15(tr("Santi Faustino e Giovita"))

    def _populate_subdiv_bt_public_holidays(self):
        # Trani province holiday.

        # Saint Nicholas the Pilgrim's Day.
        self._add_holiday_may_3(tr("San Nicola Pellegrino"))

        # Andria province holiday.

        # Saint Richard of Andria's Day.
        self._add_holiday_3rd_sun_of_sep(tr("San Riccardo di Andria"))

        # Barletta province holiday.

        # Saint Roger's Day.
        self._add_holiday_dec_30(tr("San Ruggero"))

    def _populate_subdiv_bz_public_holidays(self):
        # Province Law 36 of Oct 16, 1992.
        if self._year >= 1993:
            # Whit Monday.
            self._add_whit_monday(tr("Lunedì di Pentecoste"))

    def _populate_subdiv_ca_public_holidays(self):
        # Saint Saturninus of Cagliari's Day.
        self._add_holiday_oct_30(tr("San Saturnino di Cagliari"))

    def _populate_subdiv_cb_public_holidays(self):
        # Saint George's Day.
        self._add_saint_georges_day(tr("San Giorgio"))

    def _populate_subdiv_ce_public_holidays(self):
        # Saint Sebastian's Day.
        self._add_holiday_jan_20(tr("San Sebastiano"))

    def _populate_subdiv_ch_public_holidays(self):
        # Saint Justin of Chieti's Day.
        self._add_holiday_may_11(tr("San Giustino di Chieti"))

    def _populate_subdiv_cl_public_holidays(self):
        # Saint Michael the Archangel's Day.
        self._add_holiday_sep_29(tr("San Michele Arcangelo"))

    def _populate_subdiv_cn_public_holidays(self):
        # Saint Michael the Archangel's Day.
        self._add_holiday_sep_29(tr("San Michele Arcangelo"))

    def _populate_subdiv_co_public_holidays(self):
        # Saint Abbondius's Day.
        self._add_holiday_aug_31(tr("Sant'Abbondio"))

    def _populate_subdiv_cr_public_holidays(self):
        # Saint Homobonus's Day.
        self._add_holiday_nov_13(tr("Sant'Omobono"))

    def _populate_subdiv_cs_public_holidays(self):
        # Our Lady of the Pilerio.
        self._add_holiday_feb_12(tr("Madonna del Pilerio"))

    def _populate_subdiv_ct_public_holidays(self):
        # Saint Agatha's Day.
        self._add_holiday_feb_5(tr("Sant'Agata"))

    def _populate_subdiv_cz_public_holidays(self):
        # Saint Vitalian's Day.
        self._add_holiday_jul_16(tr("San Vitaliano"))

    def _populate_subdiv_en_public_holidays(self):
        # Visitation of Mary Day.
        self._add_holiday_jul_2(tr("Madonna della Visitazione"))

    def _populate_subdiv_fc_public_holidays(self):
        # Forlì province holiday.

        # Our Lady of the Fire.
        self._add_holiday_feb_4(tr("Madonna del Fuoco"))

        # Cesena province holiday.

        # Saint John's Day.
        self._add_saint_johns_day(tr("San Giovanni Battista"))

    def _populate_subdiv_fe_public_holidays(self):
        # Saint George's Day.
        self._add_saint_georges_day(tr("San Giorgio"))

    def _populate_subdiv_fg_public_holidays(self):
        # Our Lady of the Seven Veils.
        self._add_holiday_mar_22(tr("Madonna dei Sette Veli"))

    def _populate_subdiv_fi_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("San Giovanni Battista"))

    def _populate_subdiv_fm_public_holidays(self):
        # Assumption Day.
        name = tr("Maria Santissima Assunta")
        self._add_assumption_of_mary_day(name)
        self._add_holiday_aug_16(name)

    def _populate_subdiv_fr_public_holidays(self):
        # Saint Silverius's Day.
        self._add_holiday_jun_20(tr("San Silverio"))

    def _populate_subdiv_ge_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("San Giovanni Battista"))

    def _populate_subdiv_go_public_holidays(self):
        # Saints Hilary and Tatian's Day.
        self._add_holiday_mar_16(tr("Santi Ilario e Taziano"))

    def _populate_subdiv_gr_public_holidays(self):
        # Saint Lawrence's Day.
        self._add_holiday_aug_10(tr("San Lorenzo"))

    def _populate_subdiv_im_public_holidays(self):
        # Saint Leonard of Porto Maurizio's Day.
        self._add_holiday_nov_26(tr("San Leonardo da Porto Maurizio"))

    def _populate_subdiv_is_public_holidays(self):
        # Saint Peter Celestine's Day.
        self._add_holiday_may_19(tr("San Pietro Celestino"))

    def _populate_subdiv_kr_public_holidays(self):
        # Saint Dionysius's Day.
        self._add_holiday_oct_9(tr("San Dionigi"))

    def _populate_subdiv_lc_public_holidays(self):
        # Saint Nicholas's Day.
        self._add_holiday_dec_6(tr("San Nicola"))

    def _populate_subdiv_le_public_holidays(self):
        # Saint Orontius's Day.
        self._add_holiday_aug_26(tr("Sant'Oronzo"))

    def _populate_subdiv_li_public_holidays(self):
        # Saint Julia's Day.
        self._add_holiday_may_22(tr("Santa Giulia"))

    def _populate_subdiv_lo_public_holidays(self):
        # Saint Bassianus's Day.
        self._add_holiday_jan_19(tr("San Bassiano"))

    def _populate_subdiv_lt_public_holidays(self):
        # Saint Mark's Day.
        self._add_holiday_apr_25(tr("San Marco Evangelista"))

        # Saint Maria Goretti's Day.
        self._add_holiday_jul_6(tr("Santa Maria Goretti"))

    def _populate_subdiv_lu_public_holidays(self):
        # Saint Paulinus of Lucca's Day.
        self._add_holiday_jul_12(tr("San Paolino di Lucca"))

    def _populate_subdiv_mb_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("San Giovanni Battista"))

    def _populate_subdiv_mc_public_holidays(self):
        # Saint Julian the Hospitaller's Day.
        self._add_holiday_aug_31(tr("San Giuliano l'ospitaliere"))

    def _populate_subdiv_me_public_holidays(self):
        # Our Lady of the Letter.
        self._add_holiday_jun_3(tr("Madonna della Lettera"))

    def _populate_subdiv_mi_public_holidays(self):
        # Saint Ambrose's Day.
        self._add_holiday_dec_7(tr("Sant'Ambrogio"))

    def _populate_subdiv_mn_public_holidays(self):
        # Saint Anselm of Baggio's Day.
        self._add_holiday_mar_18(tr("Sant'Anselmo da Baggio"))

    def _populate_subdiv_mo_public_holidays(self):
        # Saint Geminianus's Day.
        self._add_holiday_jan_31(tr("San Geminiano"))

    def _populate_subdiv_ms_public_holidays(self):
        # Saint Francis of Assisi's Day.
        self._add_holiday_oct_4(tr("San Francesco d'Assisi"))

    def _populate_subdiv_mt_public_holidays(self):
        # Our Lady of the Bruna.
        self._add_holiday_jul_2(tr("Madonna della Bruna"))

    def _populate_subdiv_na_public_holidays(self):
        # Saint Januarius's Day.
        self._add_holiday_sep_19(tr("San Gennaro"))

    def _populate_subdiv_no_public_holidays(self):
        # Saint Gaudentius's Day.
        self._add_holiday_jan_22(tr("San Gaudenzio"))

    def _populate_subdiv_nu_public_holidays(self):
        # Our Lady of the Snows.
        self._add_holiday_aug_5(tr("Nostra Signora della Neve"))

    def _populate_subdiv_or_public_holidays(self):
        # Saint Archelaus's Day.
        self._add_holiday_feb_13(tr("Sant'Archelao"))

    def _populate_subdiv_pa_public_holidays(self):
        # Saint Rosalia's Day.
        self._add_holiday_jul_15(tr("Santa Rosalia"))

    def _populate_subdiv_pc_public_holidays(self):
        # Saint Antoninus of Piacenza's Day.
        self._add_holiday_jul_4(tr("Sant'Antonino di Piacenza"))

    def _populate_subdiv_pd_public_holidays(self):
        # Saint Anthony of Padua's Day.
        self._add_saint_anthonys_day(tr("Sant'Antonio di Padova"))

    def _populate_subdiv_pe_public_holidays(self):
        # Saint Cetteus's Day.
        self._add_holiday_oct_10(tr("San Cetteo"))

    def _populate_subdiv_pg_public_holidays(self):
        # Saint Clare of Assisi's Day.
        self._add_holiday_aug_11(tr("Santa Chiara d'Assisi"))

        # Saint Francis of Assisi's Day.
        self._add_holiday_oct_4(tr("San Francesco d'Assisi"))

    def _populate_subdiv_pi_public_holidays(self):
        # Saint Ranieri's Day.
        self._add_holiday_jun_17(tr("San Ranieri"))

    def _populate_subdiv_pn_public_holidays(self):
        # Saint Mark's Day.
        self._add_holiday_apr_25(tr("San Marco Evangelista"))

        # Our Lady of Graces.
        self._add_nativity_of_mary_day(tr("Madonna delle Grazie"))

    def _populate_subdiv_po_public_holidays(self):
        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Santo Stefano"))

    def _populate_subdiv_pr_public_holidays(self):
        # Saint Hilary of Poitiers's Day.
        self._add_holiday_jan_13(tr("Sant'Ilario di Poitiers"))

    def _populate_subdiv_pt_public_holidays(self):
        # Saint James's Day.
        self._add_saint_james_day(tr("San Jacopo"))

    def _populate_subdiv_pu_public_holidays(self):
        # Urbino province holiday.

        # Saint Crescentinus's Day.
        self._add_holiday_jun_1(tr("San Crescentino"))

        # Pesaro province holiday.

        # Saint Terentius of Pesaro's Day.
        self._add_holiday_sep_24(tr("San Terenzio di Pesaro"))

    def _populate_subdiv_pv_public_holidays(self):
        # Saint Syrus's Day.
        self._add_holiday_dec_9(tr("San Siro"))

    def _populate_subdiv_pz_public_holidays(self):
        # Saint Gerard of Potenza's Day.
        self._add_holiday_may_30(tr("San Gerardo di Potenza"))

    def _populate_subdiv_ra_public_holidays(self):
        # Saint Apollinaris's Day.
        self._add_holiday_jul_23(tr("Sant'Apollinare"))

    def _populate_subdiv_rc_public_holidays(self):
        # Saint George's Day.
        self._add_saint_georges_day(tr("San Giorgio"))

    def _populate_subdiv_re_public_holidays(self):
        # Saint Prosper of Reggio's Day.
        self._add_holiday_nov_24(tr("San Prospero Vescovo"))

    def _populate_subdiv_rg_public_holidays(self):
        # Saint George's Day.
        self._add_saint_georges_day(tr("San Giorgio Martire"))

        # Saint John's Day.
        self._add_holiday_aug_29(tr("San Giovanni Battista"))

    def _populate_subdiv_ri_public_holidays(self):
        # Saint Barbara's Day.
        self._add_holiday_dec_4(tr("Santa Barbara"))

    def _populate_subdiv_rm_public_holidays(self):
        # Saints Peter and Paul's Day.
        self._add_saints_peter_and_paul_day(tr("Santi Pietro e Paolo"))

    def _populate_subdiv_rn_public_holidays(self):
        # Saint Gaudentius's Day.
        self._add_holiday_oct_14(tr("San Gaudenzio"))

    def _populate_subdiv_ro_public_holidays(self):
        # Saint Bellinus's Day.
        self._add_holiday_nov_26(tr("San Bellino"))

    def _populate_subdiv_sa_public_holidays(self):
        # Saint Matthew's Day.
        self._add_holiday_sep_21(tr("San Matteo Evangelista"))

    def _populate_subdiv_si_public_holidays(self):
        # Saint Ansanus's Day.
        self._add_holiday_dec_1(tr("Sant'Ansano"))

    def _populate_subdiv_so_public_holidays(self):
        # Saints Gervasius and Protasius's Day.
        self._add_holiday_jun_19(tr("San Gervasio e San Protasio"))

    def _populate_subdiv_sp_public_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("San Giuseppe"))

    def _populate_subdiv_sr_public_holidays(self):
        # Saint Lucy's Day.
        self._add_holiday_dec_13(tr("Santa Lucia"))

    def _populate_subdiv_ss_public_holidays(self):
        # Saint Nicholas's Day.
        self._add_holiday_dec_6(tr("San Nicola"))

    def _populate_subdiv_su_public_holidays(self):
        # Carbonia province holiday.

        # Saint Pontian's Day.
        self._add_holiday_4_days_past_2nd_sun_of_may(tr("San Ponziano"))

    def _populate_subdiv_sv_public_holidays(self):
        # Our Lady of Mercy.
        self._add_holiday_mar_18(tr("Nostra Signora della Misericordia"))

    def _populate_subdiv_ta_public_holidays(self):
        # Saint Catald's Day.
        self._add_holiday_may_10(tr("San Cataldo"))

    def _populate_subdiv_te_public_holidays(self):
        # Saint Berardo's Day.
        self._add_holiday_dec_19(tr("San Berardo da Pagliara"))

    def _populate_subdiv_tn_public_holidays(self):
        # Saint Vigilius's Day.
        self._add_holiday_jun_26(tr("San Vigilio"))

    def _populate_subdiv_to_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("San Giovanni Battista"))

    def _populate_subdiv_tp_public_holidays(self):
        # Saint Albert of Trapani's Day.
        self._add_holiday_aug_7(tr("Sant'Alberto degli Abati"))

    def _populate_subdiv_tr_public_holidays(self):
        # Saint Valentine's Day.
        self._add_holiday_feb_14(tr("San Valentino"))

    def _populate_subdiv_ts_public_holidays(self):
        # Saint Justus's Day.
        self._add_holiday_nov_3(tr("San Giusto"))

    def _populate_subdiv_tv_public_holidays(self):
        # Saint Liberal's Day.
        self._add_holiday_apr_27(tr("San Liberale"))

    def _populate_subdiv_ud_public_holidays(self):
        # Saints Hermagoras and Fortunatus's Day.
        self._add_holiday_jul_12(tr("Santi Ermacora e Fortunato"))

    def _populate_subdiv_va_public_holidays(self):
        # Saint Victor the Moor's Day.
        self._add_holiday_may_8(tr("San Vittore il Moro"))

    def _populate_subdiv_vb_public_holidays(self):
        # Verbania province holiday.

        # Saint Victor the Moor's Day.
        self._add_holiday_may_8(tr("San Vittore il Moro"))

    def _populate_subdiv_vc_public_holidays(self):
        # Saint Eusebius of Vercelli's Day.
        self._add_holiday_aug_1(tr("Sant'Eusebio di Vercelli"))

    def _populate_subdiv_ve_public_holidays(self):
        # Saint Mark's Day.
        self._add_holiday_apr_25(tr("San Marco Evangelista"))

        # Our Lady of Health.
        self._add_holiday_nov_21(tr("Madonna della Salute"))

    def _populate_subdiv_vi_public_holidays(self):
        # Our Lady of Monte Berico.
        self._add_nativity_of_mary_day(tr("Madonna di Monte Berico"))

    def _populate_subdiv_vr_public_holidays(self):
        # Saint Zeno's Day.
        self._add_holiday_may_21(tr("San Zeno"))

    def _populate_subdiv_vt_public_holidays(self):
        # Saint Rose of Viterbo's Day.
        self._add_holiday_sep_4(tr("Santa Rosa da Viterbo"))

    def _populate_subdiv_vv_public_holidays(self):
        # Saint Leoluca's Day.
        self._add_holiday_mar_1(tr("San Leoluca"))

    def _populate_subdiv_andria_public_holidays(self):
        # Saint Richard of Andria's Day.
        self._add_holiday_3rd_sun_of_sep(tr("San Riccardo di Andria"))

    def _populate_subdiv_barletta_public_holidays(self):
        # Saint Roger's Day.
        self._add_holiday_dec_30(tr("San Ruggero"))

    def _populate_subdiv_cesena_public_holidays(self):
        # Saint John's Day.
        self._add_saint_johns_day(tr("San Giovanni Battista"))

    def _populate_subdiv_forli_public_holidays(self):
        # Our Lady of the Fire.
        self._add_holiday_feb_4(tr("Madonna del Fuoco"))

    def _populate_subdiv_pesaro_public_holidays(self):
        # Saint Terentius of Pesaro's Day.
        self._add_holiday_sep_24(tr("San Terenzio di Pesaro"))

    def _populate_subdiv_trani_public_holidays(self):
        # Saint Nicholas the Pilgrim's Day.
        self._add_holiday_may_3(tr("San Nicola Pellegrino"))

    def _populate_subdiv_urbino_public_holidays(self):
        # Saint Crescentinus's Day.
        self._add_holiday_jun_1(tr("San Crescentino"))


class IT(Italy):
    pass


class ITA(Italy):
    pass


class ItalyStaticHolidays:
    """Italy special holidays.

    References:
        * [Law 366 of Jun 28, 1907](https://web.archive.org/web/20251124204309/https://www.gazzettaufficiale.it/eli/gu/1907/06/28/152/sg/pdf)
        * [Law 450 of Jul 7, 1910](https://web.archive.org/web/20251027060106/https://www.gazzettaufficiale.it/eli/gu/1910/07/18/167/sg/pdf)
        * [Royal Decree 269 of Mar 11, 1920](https://web.archive.org/web/20251027093344/https://www.gazzettaufficiale.it/eli/gu/1920/03/19/66/sg/pdf)
        * [Royal Decree 1208 of Aug 21, 1921](https://web.archive.org/web/20260117043612/https://www.gazzettaufficiale.it/eli/gu/1921/09/10/215/sg/pdf)
        * [Royal Decree 1207 of Jul 10, 1925](https://web.archive.org/web/20251027092027/https://www.gazzettaufficiale.it/eli/gu/1925/07/24/170/sg/pdf)
        * [Royal Decree-Law 376 of Apr 25, 1938](https://web.archive.org/web/20251027093701/https://www.gazzettaufficiale.it/eli/gu/1938/04/28/97/sg/pdf)
        * [Presidential Legislative Decree 2 of Jun 19, 1946](https://web.archive.org/web/20251213235623/https://www.normattiva.it/atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta=1946-06-20&atto.codiceRedazionale=046U0002&tipoDettaglio=originario&qId=0c1f85e7-d249-4987-966d-d9a4073ef48f)
    """

    # Anniversary of the Unification of Italy.
    anniversary_of_unification = tr("Anniversario dell'Unità d'Italia")

    special_public_holidays = {
        # 100th anniversary of the birth of General Giuseppe Garibaldi.
        1907: (JUL, 4, tr("Centenario della nascita del generale Giuseppe Garibaldi")),
        # 100th anniversary of the birth of Camillo Cavour.
        1910: (AUG, 10, tr("Centenario della nascita di Camillo Cavour")),
        1911: (MAR, 17, anniversary_of_unification),
        # 100th anniversary of the birth of Victor Emmanuel II.
        1920: (MAR, 14, tr("Centenario della nascita del Vittorio Emanuele II")),
        # Celebration of 600th anniversary of the death of Dante.
        1921: (SEP, 14, tr("Celebrazione del sesto centenario dantesco")),
        1926: (
            OCT,
            4,
            # 700th anniversary of the death of Saint Francis of Assisi.
            tr("Anniversario del VII centenario della morte di San Francesco di Assisi"),
        ),
        # National Holiday.
        1938: (MAY, 3, tr("Festa Nazionale")),
        # Public Holiday.
        1946: (JUN, 11, tr("Giorno Festivo")),
        1961: (MAR, 17, anniversary_of_unification),
        2011: (MAR, 17, anniversary_of_unification),
    }
