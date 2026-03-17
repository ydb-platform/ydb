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

from holidays.calendars.gregorian import APR, JUN, SEP, DEC
from holidays.constants import BANK, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_ONLY,
    MON_FRI_ONLY,
    TUE_TO_PREV_FRI,
    WED_TO_NEXT_FRI,
    FRI_ONLY,
    WORKDAY_TO_NEAREST_MON,
)


class Chile(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Chile holidays.

    References:
        * <https://web.archive.org/web/20250718012109/https://www.feriados.cl/>
        * [Excellent history of Chile holidays](https://web.archive.org/web/20250712031422/https://www.feriadoschilenos.cl/)
        * <https://es.wikipedia.org/wiki/Anexo:Días_feriados_en_Chile>
        * Law 2.977 (established official Chile holidays in its current form)
        * Law 20.983 (Day after New Year's Day, if it's a Sunday)
        * Law 19.668 (floating Monday holiday)
        * Law 19.668 (Corpus Christi)
        * Law 2.200, (Labour Day)
        * Law 18.018 (Labour Day renamed)
        * Law 16.840, Law 18.432 (Saint Peter and Saint Paul)
        * Law 20.148 (Day of Virgin of Carmen)
        * Law 18.026 (Day of National Liberation)
        * Law 19.588, Law 19.793 (Day of National Unity)
        * Law 20.983 (National Holiday Friday preceding Independence Day)
        * Law 20.215 (National Holiday Monday preceding Independence Day)
        * Law 20.215 (National Holiday Friday following Army Day)
        * Decree-law 636, Law 8.223
        * Law 3.810 (Columbus Day)
        * Law 20.299 (National Day of the Evangelical and Protestant Churches)
        * Law 20.663 (Región de Arica y Parinacota)
        * Law 20.678 (Región de Ñuble)
        * [Law 17.374 (holiday on census days)](https://web.archive.org/web/20250812022623/https://www.bcn.cl/leychile/navegar?idNorma=28960&idVersion=2002-02-01)
        * [Law 19.656 (Dec 31, 1999 holiday)](https://web.archive.org/web/20241228005823/https://www.bcn.cl/leychile/navegar?idNorma=149328&idVersion=1999-12-15)
        * [Law 12.051 (bank holidays Jun 30 and Dec 31)](https://web.archive.org/web/20241227190026/https://www.bcn.cl/leychile/navegar?idNorma=27013&idVersion=1956-07-12)
        * [Decree-law 1.171 (eliminate Jun 30)](https://web.archive.org/web/20241227191010/https://www.bcn.cl/leychile/navegar?idNorma=6507&idVersion=1975-09-05)
        * [Law 19.528 (eliminate Dec 31)](https://web.archive.org/web/20241227191452/https://www.bcn.cl/leychile/navegar?idNorma=76630&idVersion=1997-11-04)
        * [Law 19.559 (restore Dec 31)](https://web.archive.org/web/20241227195811/https://www.bcn.cl/leychile/navegar?idNorma=97758&idVersion=1998-04-16)
        * [Law 19.973 (Sep 17, 2004 holiday)](https://web.archive.org/web/20250812023003/https://www.bcn.cl/leychile/navegar?idLey=19973)
        * [Law 20.450 (Sep 17, 2010 and Sep 20, 2010 holidays)](https://web.archive.org/web/20250812023308/https://www.bcn.cl/leychile/navegar?idLey=20450)
        * [Law 21.521 (eliminate Dec 31 again, after the CMF publishes a specific regulation)](https://web.archive.org/web/20240214154900/https://www.bcn.cl/leychile/navegar?idNorma=1187323&idVersion=2023-02-03)
        * [Law 21.791 (restore bank holiday on Dec 31)](https://web.archive.org/web/20251219021727/https://www.bcn.cl/leychile/navegar?idNorma=1219578&idVersion=2025-12-17)
    """

    country = "CL"
    default_language = "es"
    start_year = 1915
    subdivisions = (
        "AI",  # Aisén del General Carlos Ibañez del Campo.
        "AN",  # Antofagasta.
        "AP",  # Arica y Parinacota.
        "AR",  # La Araucanía.
        "AT",  # Atacama.
        "BI",  # Biobío.
        "CO",  # Coquimbo.
        "LI",  # Libertador General Bernardo O'Higgins.
        "LL",  # Los Lagos.
        "LR",  # Los Ríos.
        "MA",  # Magallanes.
        "ML",  # Maule.
        "NB",  # Ñuble.
        "RM",  # Región Metropolitana de Santiago.
        "TA",  # Tarapacá.
        "VS",  # Valparaíso.
    )
    subdivisions_aliases = {
        "Aisén del General Carlos Ibañez del Campo": "AI",
        "Antofagasta": "AN",
        "Arica y Parinacota": "AP",
        "La Araucanía": "AR",
        "Atacama": "AT",
        "Biobío": "BI",
        "Coquimbo": "CO",
        "Libertador General Bernardo O'Higgins": "LI",
        "Los Lagos": "LL",
        "Los Ríos": "LR",
        "Magallanes": "MA",
        "Maule": "ML",
        "Ñuble": "NB",
        "Región Metropolitana de Santiago": "RM",
        "Tarapacá": "TA",
        "Valparaíso": "VS",
    }
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("en_US", "es", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, ChileStaticHolidays)
        kwargs.setdefault("observed_rule", WORKDAY_TO_NEAREST_MON)
        kwargs.setdefault("observed_since", 2000)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))
        if self._year >= 2017:
            # National Holiday.
            self._add_observed(self._add_new_years_day_two(tr("Feriado nacional")), rule=MON_ONLY)

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Holy Saturday.
        self._add_holy_saturday(tr("Sábado Santo"))

        if self._year <= 1967:
            # Ascension Day.
            self._add_ascension_thursday(tr("Ascensión del Señor"))

        if self._year <= 1967 or 1987 <= self._year <= 2006:
            # Corpus Christi.
            name = tr("Corpus Christi")
            if self._year <= 1999:
                self._add_corpus_christi_day(name)
            else:
                self._add_holiday_57_days_past_easter(name)

        if self._year >= 1932:
            # Labor Day.
            self._add_labor_day(tr("Día Nacional del Trabajo"))

        # Naval Glories Day.
        self._add_holiday_may_21(tr("Día de las Glorias Navales"))

        if self._year >= 2021:
            # National Day of Indigenous Peoples.
            name = tr("Día Nacional de los Pueblos Indígenas")
            if self._year == 2021:
                self._add_holiday_jun_21(name)
            else:
                self._add_holiday(name, self._summer_solstice_date)

        if self._year <= 1967 or self._year >= 1986:
            # Saint Peter and Saint Paul's Day.
            self._move_holiday(self._add_saints_peter_and_paul_day(tr("San Pedro y San Pablo")))

        if self._year >= 2007:
            # Day of Virgin of Carmen.
            self._add_holiday_jul_16(tr("Virgen del Carmen"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Asunción de la Virgen"))

        if 1981 <= self._year <= 1998:
            # Day of National Liberation.
            self._add_holiday_sep_11(tr("Día de la Liberación Nacional"))
        elif 1999 <= self._year <= 2001:
            # Day of National Unity.
            self._add_holiday_1st_mon_of_sep(tr("Día de la Unidad Nacional"))

        # National Holiday.
        name = tr("Fiestas Patrias")

        if self._year >= 2007:
            self._add_observed(
                self._add_holiday_sep_17(name),
                rule=MON_FRI_ONLY if self._year >= 2017 else MON_ONLY,
            )
            if self._year >= 2008:
                self._add_observed(self._add_holiday_sep_20(name), rule=FRI_ONLY)
        elif 1932 <= self._year <= 1944:
            self._add_holiday_sep_20(name)

        # Independence Day.
        self._add_holiday_sep_18(tr("Día de la Independencia"))

        # Army Day.
        self._add_holiday_sep_19(tr("Día de las Glorias del Ejército"))

        if self._year >= 1922 and self._year != 1973:
            self._move_holiday(
                self._add_columbus_day(
                    # Meeting of Two Worlds' Day.
                    tr("Día del Encuentro de dos Mundos")
                    if self._year >= 2000
                    # Columbus Day.
                    else tr("Día de la Raza")
                )
            )

        if self._year >= 2008:
            # This holiday is moved to the preceding Friday if it falls on a Tuesday,
            # or to the following Friday if it falls on a Wednesday.
            self._move_holiday(
                self._add_holiday_oct_31(
                    # National Day of the Evangelical and Protestant Churches.
                    tr("Día Nacional de las Iglesias Evangélicas y Protestantes")
                ),
                rule=TUE_TO_PREV_FRI + WED_TO_NEXT_FRI,
            )

        # All Saints' Day.
        self._add_all_saints_day(tr("Día de Todos los Santos"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("La Inmaculada Concepción"))

        if 1944 <= self._year <= 1988:
            # Christmas Eve.
            self._add_christmas_eve(tr("Víspera de Navidad"))

        # Christmas Day.
        self._add_christmas_day(tr("Navidad"))

    def _populate_subdiv_ap_public_holidays(self):
        if self._year >= 2013:
            # Assault and Capture of Cape Arica.
            self._add_holiday_jun_7(tr("Asalto y Toma del Morro de Arica"))

    def _populate_subdiv_nb_public_holidays(self):
        if self._year >= 2014:
            self._add_holiday_aug_20(
                # Nativity of Bernardo O'Higgins (Chillán and Chillán Viejo communes)
                tr("Nacimiento del Prócer de la Independencia (Chillán y Chillán Viejo)")
            )

    def _populate_bank_holidays(self):
        # Bank Holiday.
        name = tr("Feriado bancario")
        if 1957 <= self._year <= 1975:
            self._add_holiday_jun_30(name)

        if 1956 <= self._year and self._year != 1997:
            self._add_holiday_dec_31(name)

    @property
    def _summer_solstice_date(self) -> tuple[int, int]:
        day = 20
        if (self._year % 4 > 1 and self._year <= 2046) or (
            self._year % 4 > 2 and self._year <= 2075
        ):
            day = 21
        return JUN, day


class CL(Chile):
    pass


class CHL(Chile):
    pass


class ChileStaticHolidays:
    # National Holiday.
    national_holiday = tr("Feriado nacional")

    # National Population and Housing Census.
    national_census = tr("Censo Nacional de Población y Vivienda")

    special_public_holidays = {
        1999: (DEC, 31, national_holiday),
        2002: (APR, 24, national_census),
        2004: (SEP, 17, national_holiday),
        2010: (
            (SEP, 17, national_holiday),
            (SEP, 20, national_holiday),
        ),
        2017: (APR, 19, national_census),
        2022: (SEP, 16, national_holiday),
    }
