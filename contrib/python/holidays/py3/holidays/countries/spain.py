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
from holidays.calendars.gregorian import JAN, MAR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.groups import (
    ChristianHolidays,
    IslamicHolidays,
    InternationalHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NONE


class Spain(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Spain holidays.

    References:
        * <https://web.archive.org/web/20230307043804/https://administracion.gob.es/pag_Home/atencionCiudadana/calendarios.html>
        * [2008](https://web.archive.org/web/20251029052726/https://www.boe.es/diario_boe/txt.php?id=BOE-A-2007-18306)
        * [2009](https://web.archive.org/web/20250427104921/https://www.boe.es/buscar/doc.php?id=BOE-A-2008-18411)
        * [2010](https://web.archive.org/web/20250427181827/https://www.boe.es/buscar/doc.php?id=BOE-A-2009-18477)
        * [2011](https://web.archive.org/web/20231121065830/https://www.boe.es/buscar/doc.php?id=BOE-A-2010-15722)
        * [2012](https://web.archive.org/web/20250427181838/https://www.boe.es/buscar/doc.php?id=BOE-A-2011-16116)
        * [2013](https://web.archive.org/web/20220120080053/https://www.boe.es/buscar/doc.php?id=BOE-A-2012-13644)
        * [2014](https://web.archive.org/web/20201001232243/https://www.boe.es/buscar/doc.php?id=BOE-A-2013-12147)
        * [2015](https://web.archive.org/web/20240915041804/https://www.boe.es/buscar/doc.php?id=BOE-A-2014-10823)
        * [2016](https://web.archive.org/web/20240915044403/http://www.boe.es/buscar/doc.php?id=BOE-A-2015-11348)
        * [2017](https://web.archive.org/web/20170609094105/http://www.boe.es:80/buscar/doc.php?id=BOE-A-2016-9244)
        * [2018](https://web.archive.org/web/20241006073402/https://www.boe.es/buscar/doc.php?id=BOE-A-2017-11639)
        * [2019](https://web.archive.org/web/20240329020330/https://boe.es/buscar/doc.php?id=BOE-A-2018-14369)
        * [2020](https://web.archive.org/web/20240417060155/https://www.boe.es/buscar/doc.php?id=BOE-A-2019-14552)
        * [2021](https://web.archive.org/web/20241114022913/https://www.boe.es/buscar/doc.php?id=BOE-A-2020-13343)
        * [2022](https://web.archive.org/web/20240725121311/https://www.boe.es/buscar/doc.php?id=BOE-A-2021-17113)
        * [2023](https://web.archive.org/web/20240811035605/https://www.boe.es/buscar/doc.php?id=BOE-A-2022-16755)
        * [2024](https://web.archive.org/web/20240401192304/https://www.boe.es/buscar/doc.php?id=BOE-A-2023-22014)
        * [2025](https://web.archive.org/web/20241226214918/https://www.boe.es/buscar/doc.php?id=BOE-A-2024-21316)
        * [2026](https://web.archive.org/web/20251028115438/https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-21667)

    Subdivisions Holidays References:
        * Ceuta:
            * [2018](https://web.archive.org/web/20251029132255/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2018)
            * [2019](https://web.archive.org/web/20210506145356/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2019)
            * [2020](https://web.archive.org/web/20210413221634/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2020)
            * [2021](https://web.archive.org/web/20251029131434/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2021)
            * [2022](https://web.archive.org/web/20250115161923/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2022)
            * [2023](https://web.archive.org/web/20241211133010/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2023)
            * [2024](https://web.archive.org/web/20240419004114/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2024)
            * [2025](https://web.archive.org/web/20250315132943/https://sede.ceuta.es/controlador/controlador?modulo=info&cmd=calendario&year=2025)
        * Melilla:
            * [2017](https://web.archive.org/web/20251029145828/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=23611&nivel=1400&tipo=2)
            * [2018](https://web.archive.org/web/20251029144310/https://www.melilla.es/melillaPortal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=25713&nivel=1400&tipo=2)
            * [2019](https://web.archive.org/web/20251029132642/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=27481&nivel=1400&tipo=2)
            * [2020](https://web.archive.org/web/20251028181240/https://www.melilla.es/melillaPortal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=29323&nivel=1400&tipo=2)
            * [2021](https://web.archive.org/web/20251028181807/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=30529&nivel=1400&tipo=2)
            * [2022](https://web.archive.org/web/20251029030735/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=32051&nivel=1400&tipo=2)
            * [2023](https://web.archive.org/web/20251029030355/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=33685&nivel=1400&tipo=2)
            * [2024](https://web.archive.org/web/20241208104853/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=34997&nivel=1400&tipo=2)
            * [2025](https://web.archive.org/web/20250113015447/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=37491&nivel=1400&tipo=2)
            * [2026](https://web.archive.org/web/20251029045506/https://www.melilla.es/melillaportal/contenedor.jsp?seccion=s_fact_d4_v1.jsp&contenido=41767&nivel=1400&tipo=2)
        * Navarra:
            * [2008](https://web.archive.org/web/20100407160735/http://www.lexnavarra.navarra.es/detalle.asp?r=29465)
            * [2009](https://web.archive.org/web/20130615075729/http://www.lexnavarra.navarra.es/detalle.asp?r=29761)
            * [2010](https://web.archive.org/web/20250903095706/https://www.lexnavarra.navarra.es/detalle.asp?r=8402)
            * [2011](https://web.archive.org/web/20250903095217/https://www.lexnavarra.navarra.es/detalle.asp?r=8403)
            * [2012](https://web.archive.org/web/20250903095133/https://www.lexnavarra.navarra.es/detalle.asp?r=12993)
            * [2013](https://web.archive.org/web/20250903095136/https://www.lexnavarra.navarra.es/detalle.asp?r=26226)
            * [2014](https://web.archive.org/web/20250903095123/https://www.lexnavarra.navarra.es/detalle.asp?r=32382)
            * [2015](https://web.archive.org/web/20250903095257/https://www.lexnavarra.navarra.es/detalle.asp?r=34276)
            * [2016](https://web.archive.org/web/20250903095302/https://www.lexnavarra.navarra.es/detalle.asp?r=36141)
            * [2017](https://web.archive.org/web/20250903095306/https://www.lexnavarra.navarra.es/detalle.asp?r=37665)
            * [2018](https://web.archive.org/web/20170728130845/https://www.lexnavarra.navarra.es/detalle.asp?r=38904)
            * [2019](https://web.archive.org/web/20250903095819/https://www.lexnavarra.navarra.es/detalle.asp?r=50305)
            * [2020](https://web.archive.org/web/20250623005808/https://www.lexnavarra.navarra.es/detalle.asp?r=52229)
            * [2021](https://web.archive.org/web/20250623010750/https://www.lexnavarra.navarra.es/detalle.asp?r=52748)
            * [2022](https://web.archive.org/web/20250623000851/https://www.lexnavarra.navarra.es/detalle.asp?r=53763)
            * [2023](https://web.archive.org/web/20250623010106/https://www.lexnavarra.navarra.es/detalle.asp?r=55481)
            * [2024](https://web.archive.org/web/20250623001355/https://www.lexnavarra.navarra.es/detalle.asp?r=56116)
            * [2025](https://web.archive.org/web/20250622235218/https://www.lexnavarra.navarra.es/detalle.asp?r=57122)
            * [2026](https://web.archive.org/web/20251028120003/https://www.lexnavarra.navarra.es/detalle.asp?r=57937)

    Holidays checked with official sources for 2008-2026 only.
    """

    country = "ES"
    default_language = "es"
    # %s (estimated).
    estimated_label = tr("%s (estimado)")
    # Monday following %s (estimated).
    observed_estimated_label = tr("Lunes siguiente a %s (estimado)")
    # Monday following %s.
    observed_label = tr("Lunes siguiente a %s")
    # Earliest available online sources for all of Spain's subdivisions.
    start_year = 2008
    subdivisions = (
        "AN",  # Andalucía.
        "AR",  # Aragón.
        "AS",  # Asturias.
        "CB",  # Cantabria.
        "CE",  # Ceuta.
        "CL",  # Castilla y León.
        "CM",  # Castilla-La Mancha.
        "CN",  # Canarias.
        "CT",  # Cataluña (Catalunya).
        "EX",  # Extremadura.
        "GA",  # Galicia.
        "IB",  # Islas Baleares (Illes Balears).
        "MC",  # Murcia.
        "MD",  # Madrid.
        "ML",  # Melilla.
        "NC",  # Navarra.
        "PV",  # País Vasco.
        "RI",  # La Rioja.
        "VC",  # Valenciana.
    )
    subdivisions_aliases = {
        "Andalucía": "AN",
        "Aragón": "AR",
        "Asturias": "AS",
        "Cantabria": "CB",
        "Ceuta": "CE",
        "Castilla y León": "CL",
        "Castilla-La Mancha": "CM",
        "Canarias": "CN",
        "Cataluña": "CT",
        "Catalunya": "CT",
        "Extremadura": "EX",
        "Galicia": "GA",
        "Islas Baleares": "IB",
        "Illes Balears": "IB",
        "Murcia": "MC",
        "Madrid": "MD",
        "Melilla": "ML",
        "Navarra": "NC",
        "País Vasco": "PV",
        "La Rioja": "RI",
        "Valenciana": "VC",
    }
    supported_languages = ("ca", "en_US", "es", "th", "uk")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=SpainIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=SpainStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("Año Nuevo")), rule=SUN_TO_NONE)

        # Epiphany.
        self._add_observed(self._add_epiphany_day(tr("Epifanía del Señor")), rule=SUN_TO_NONE)

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Labor Day.
        self._add_observed(self._add_labor_day(tr("Fiesta del Trabajo")), rule=SUN_TO_NONE)

        self._add_observed(
            # Assumption Day.
            self._add_assumption_of_mary_day(tr("Asunción de la Virgen")),
            rule=SUN_TO_NONE,
        )

        self._add_observed(
            # National Day.
            self._add_holiday_oct_12(tr("Fiesta Nacional de España")),
            rule=SUN_TO_NONE,
        )

        # All Saints' Day.
        self._add_observed(self._add_all_saints_day(tr("Todos los Santos")), rule=SUN_TO_NONE)

        self._add_observed(
            # Constitution Day.
            self._add_holiday_dec_6(tr("Día de la Constitución Española")),
            rule=SUN_TO_NONE,
        )

        self._add_observed(
            # Immaculate Conception.
            self._add_immaculate_conception_day(tr("Inmaculada Concepción")),
            rule=SUN_TO_NONE,
        )

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Natividad del Señor")), rule=SUN_TO_NONE)

    def _populate_subdiv_an_public_holidays(self):
        # New Year's Day.
        self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        # Andalusia Day.
        self._move_holiday(self._add_holiday_feb_28(tr("Día de Andalucía")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Labor Day.
        self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        # Assumption Day.
        self._move_holiday(self._add_assumption_of_mary_day(tr("Asunción de la Virgen")))

        # National Day.
        self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        # All Saints' Day.
        self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        # Constitution Day.
        self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        # Immaculate Conception.
        self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_ar_public_holidays(self):
        # New Year's Day.
        self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Saint George's Day.
        self._move_holiday(self._add_saint_georges_day(tr("Día de San Jorge")))

        # Labor Day.
        self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        # Assumption Day.
        self._move_holiday(self._add_assumption_of_mary_day(tr("Asunción de la Virgen")))

        # National Day.
        self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        # All Saints' Day.
        self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        # Constitution Day.
        self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        # Immaculate Conception.
        self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_as_public_holidays(self):
        # New Year's Day.
        self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Labor Day.
        self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        # Assumption Day.
        self._move_holiday(self._add_assumption_of_mary_day(tr("Asunción de la Virgen")))

        # Asturias Day.
        self._move_holiday(self._add_holiday_sep_8(tr("Día de Asturias")))

        # National Day.
        self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        # All Saints' Day.
        self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        # Constitution Day.
        self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        # Immaculate Conception.
        self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_cb_public_holidays(self):
        if self._year == 2013:
            # Epiphany.
            self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        if self._year != 2018:
            # Maundy Thursday.
            self._add_holy_thursday(tr("Jueves Santo"))

        # Add when Cantabria Institutions Day is on Sunday.
        if self._is_sunday(JUL, 28) or self._year in {2015, 2020}:
            # Easter Monday.
            self._add_easter_monday(tr("Lunes de Pascua"))

        if self._year == 2011:
            # Labor Day.
            self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        # Add when Our Lady of Bien Aparecida is on Sunday.
        if self._is_sunday(SEP, 15) or self._year in {2012, 2014}:
            # Saint James' Day.
            self._add_saint_james_day(tr("Santiago Apóstol"))

        if self._year not in {2012, 2015}:
            self._add_observed(
                # Cantabria Institutions Day.
                self._add_holiday_jul_28(tr("Día de las Instituciones de Cantabria")),
                rule=SUN_TO_NONE,
            )

        # Our Lady of Bien Aparecida.
        self._add_observed(self._add_holiday_sep_15(tr("La Bien Aparecida")), rule=SUN_TO_NONE)

        if self._year == 2008:
            # National Day.
            self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        if self._year == 2015:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        if self._year == 2009 or self._year >= 2026:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year == 2019:
            # Immaculate Conception.
            self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        if self._year >= 2016:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_ce_public_holidays(self):
        if self._year == 2012:
            # New Year's Day.
            self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year == 2011:
            # Labor Day.
            self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        # Saint Anthony's Day.
        self._move_holiday(self._add_saint_anthonys_day(tr("San Antonio")))

        if self._year >= 2022:
            # Santa Maria of Africa.
            self._add_holiday_aug_5(tr("Nuestra Señora de África"))

        if self._year not in {2011, 2015, 2025}:
            # Ceuta Day.
            self._add_observed(self._add_holiday_sep_2(tr("Día de Ceuta")), rule=SUN_TO_NONE)

        if self._year <= 2014:
            # National Day.
            self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        if self._year <= 2015:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        if self._year <= 2020:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year == 2013:
            # Immaculate Conception.
            self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        if self._year <= 2016:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

        # Eid al-Adha.
        name = tr("Fiesta del Sacrificio-Eidul Adha")
        if self._year == 2011:
            self._add_eid_al_adha_day_two(name)
        elif self._year in {2012, 2014}:
            self._add_eid_al_adha_day_three(name)
        elif self._year >= 2010:
            self._add_eid_al_adha_day(name)

    def _populate_subdiv_cl_public_holidays(self):
        if self._year >= 2017:
            # New Year's Day.
            self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        if self._year in {2009, 2010, 2012}:
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("San José"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year != 2023:
            # Castile and León Day.
            self._move_holiday(self._add_holiday_apr_23(tr("Fiesta de Castilla y León")))

        if self._year >= 2016:
            # Labor Day.
            self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        if self._year in {2011, 2023}:
            # Saint James' Day.
            self._add_saint_james_day(tr("Santiago Apóstol"))

        if self._year >= 2021:
            # Assumption Day.
            self._move_holiday(self._add_assumption_of_mary_day(tr("Asunción de la Virgen")))

        # National Day.
        self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        # All Saints' Day.
        self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        if self._year >= 2015:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        # Immaculate Conception.
        self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_cm_public_holidays(self):
        if self._year <= 2013:
            # Epiphany.
            self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        if self._year in {2008, 2009, 2010, 2011, 2020}:
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("San José"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year in {2014, 2015, 2019, 2020, 2026}:
            # Easter Monday.
            self._add_easter_monday(tr("Lunes de Pascua"))

        if self._year not in {2009, 2010, 2018}:
            # Corpus Christi.
            self._add_corpus_christi_day(tr("Corpus Christi"))

        # Castilla-La Mancha Day.
        may_31 = self._add_holiday_may_31(tr("Día de Castilla-La Mancha"))
        if self._year <= 2009:
            self._move_holiday(may_31, show_observed_label=False)
        else:
            self._add_observed(may_31, rule=SUN_TO_NONE)

        if self._year >= 2026:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        if self._year <= 2015:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year >= 2016:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_cn_public_holidays(self):
        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year == 2016:
            # Labor Day.
            self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        if self._year != 2021:
            # Day of the Canary Islands.
            self._move_holiday(self._add_holiday_may_30(tr("Día de Canarias")))

        if self._year >= 2021:
            # Assumption Day.
            self._move_holiday(self._add_assumption_of_mary_day(tr("Asunción de la Virgen")))

        if self._year == 2015 or self._year >= 2026:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        if self._year in {2009, 2020}:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year == 2011 or self._year >= 2022:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_ct_public_holidays(self):
        # Easter Monday.
        self._add_easter_monday(tr("Lunes de Pascua"))

        # Add when Labor Day or Christmas Day falls on Sunday.
        if self._is_sunday(MAY, 1):
            # Whit Monday.
            self._add_whit_monday(tr("Día de la Pascua Granada"))

        # Saint John the Baptist.
        self._add_observed(self._add_saint_johns_day(tr("San Juan")), rule=SUN_TO_NONE)

        self._add_observed(
            # National Day of Catalonia.
            self._add_holiday_sep_11(tr("Fiesta Nacional de Cataluña")),
            rule=SUN_TO_NONE,
        )

        # Saint Stephen's Day.
        self._add_observed(self._add_christmas_day_two(tr("San Esteban")), rule=SUN_TO_NONE)

    def _populate_subdiv_ex_public_holidays(self):
        if self._year == 2012:
            # New Year's Day.
            self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        if self._year in {2023, 2024}:
            # Shrove Tuesday.
            self._add_carnival_tuesday(tr("Martes de Carnaval"))

        if self._year in {2010, 2017, 2021}:
            # Saint Joseph's Day.
            self._move_holiday(self._add_saint_josephs_day(tr("San José")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Labor Day.
        self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        if self._year != 2024:
            # Extremadura Day.
            self._move_holiday(self._add_holiday_sep_8(tr("Día de Extremadura")))

        # National Day.
        self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        # All Saints' Day.
        self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        # Constitution Day.
        self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        # Immaculate Conception.
        self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_ga_public_holidays(self):
        if self._year in {2008, 2009, 2010, 2011, 2019, 2020, 2021, 2026}:
            # Saint Joseph's Day.
            self._move_holiday(self._add_saint_josephs_day(tr("San José")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        self._add_observed(
            # Galician Literature Day.
            self._add_holiday_may_17(tr("Día de las Letras Gallegas")),
            rule=SUN_TO_NONE,
        )

        if self._year in {2013, 2016, 2020, 2022, 2026}:
            # Saint John the Baptist.
            self._add_saint_johns_day(tr("San Juan"))

        self._add_observed(
            # Galician National Day.
            self._add_holiday_jul_25(tr("Día Nacional de Galicia")),
            rule=SUN_TO_NONE,
        )

        if self._year == 2015:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        if self._year == 2009:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

    def _populate_subdiv_ib_public_holidays(self):
        # Day of the Balearic Islands.
        mar_1 = self._add_holiday_mar_1(tr("Día de las Islas Baleares"))
        if self._year >= 2026:
            self._move_holiday(mar_1)
        else:
            self._add_observed(mar_1, rule=SUN_TO_NONE)

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year not in {2014, 2025}:
            # Easter Monday.
            self._add_easter_monday(tr("Lunes de Pascua"))

        if self._year == 2008:
            # National Day.
            self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        if self._year == 2015:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        if self._year <= 2020:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

        if self._year in {2008, 2009, 2013, 2014, 2019, 2020, 2025, 2026}:
            # Saint Stephen's Day.
            self._add_christmas_day_two(tr("San Esteban"))

    def _populate_subdiv_mc_public_holidays(self):
        if self._year >= 2017:
            # New Year's Day.
            self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        if self._year >= 2013:
            # Epiphany.
            self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        if self._year != 2022:
            # Saint Joseph's Day.
            self._add_observed(self._add_saint_josephs_day(tr("San José")), rule=SUN_TO_NONE)

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year == 2011 or self._year >= 2022:
            # Labor Day.
            self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        if self._year not in {2013, 2024}:
            # Murcia Day.
            self._move_holiday(self._add_holiday_jun_9(tr("Día de la Región de Murcia")))

        if self._year == 2008:
            # National Day.
            self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        # Constitution Day.
        self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year == 2013 or self._year >= 2024:
            # Immaculate Conception.
            self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        if self._year >= 2016:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_md_public_holidays(self):
        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        # Add when New Year's Day, Assumption Day, or All Saints' Day (until 2015) falls on Sunday.
        if (
            self._is_sunday(JAN, 1)
            or self._is_sunday(AUG, 15)
            or (self._year <= 2015 and self._is_sunday(NOV, 1))
        ):
            # Saint Joseph's Day.
            self._move_holiday(self._add_saint_josephs_day(tr("San José")))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year != 2010:
            # Madrid Day.
            self._move_holiday(self._add_holiday_may_2(tr("Fiesta de la Comunidad de Madrid")))

        if self._year in {2009, 2010, 2011, 2014}:
            # Corpus Christi.
            self._add_corpus_christi_day(tr("Corpus Christi"))

        # Add when Labor Day falls on Sunday.
        if self._is_sunday(MAY, 1) or self._year in {2008, 2024, 2025}:
            # Saint James' Day.
            self._add_saint_james_day(tr("Santiago Apóstol"))

        if self._year >= 2020:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year == 2019:
            # Immaculate Conception.
            self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        if self._year >= 2016:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_ml_public_holidays(self):
        if self._year == 2017:
            # New Year's Day.
            self._move_holiday(self._add_new_years_day(tr("Año Nuevo")))

        # Epiphany.
        self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        if self._year in {2020, 2021, 2023}:
            # Statute of Autonomy of Melilla Day.
            self._add_holiday_mar_13(tr("Estatuto de Autonomía de la Ciudad de Melilla"))

        if self._year <= 2016:
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("San José"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year != 2024:
            self._move_holiday(
                self._add_nativity_of_mary_day(
                    # Day of Our Lady of Victory.
                    tr("Día de Nuestra Señora la Virgen de la Victoria")
                )
            )

        if self._year != 2023:
            # Melilla Day.
            self._move_holiday(self._add_holiday_sep_17(tr("Día de Melilla")))

        if self._year == 2008:
            # National Day.
            self._move_holiday(self._add_holiday_oct_12(tr("Fiesta Nacional de España")))

        if self._year == 2009:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        # Constitution Day.
        self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year >= 2019:
            # Immaculate Conception.
            self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

        if self._year >= 2022:
            # Eid al-Fitr.
            self._add_eid_al_fitr_day(tr("Fiesta del Eid Fitr"))

        # Eid al-Adha.
        name = tr("Fiesta del Sacrificio-Aid Al Adha")
        if self._year in {2011, 2012, 2021}:
            self._add_eid_al_adha_day_two(name)
        elif self._year == 2022:
            self._add_eid_al_adha_day_three(name)
        elif self._year >= 2010:
            self._add_eid_al_adha_day(name)

    def _populate_subdiv_nc_public_holidays(self):
        if self._year >= 2013:
            # Epiphany.
            self._move_holiday(self._add_epiphany_day(tr("Epifanía del Señor")))

        if self._year in {2008, 2009, 2010, 2012, 2014, 2015, 2019, 2020, 2021, 2026}:
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("San José"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Easter Monday.
        self._add_easter_monday(tr("Lunes de Pascua"))

        if self._year in {2008, 2009, 2011, 2013, 2015, 2016, 2017, 2022, 2023, 2024, 2025}:
            # Saint James' Day.
            self._add_saint_james_day(tr("Santiago Apóstol"))

        if self._year >= 2026:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Todos los Santos")))

        # Saint Francis Xavier's Day.
        self._move_holiday(self._add_holiday_dec_3(tr("San Francisco Javier")))

        if self._year == 2020:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_pv_public_holidays(self):
        # Add when Epiphany (non-País Vasco Day years), Assumption Day,
        # or All Saints' Day falls on Sunday.
        if (
            ((self._year <= 2010 or self._year >= 2015) and self._is_sunday(JAN, 6))
            or self._is_sunday(AUG, 15)
            or self._is_sunday(NOV, 1)
        ):
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("San José"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Easter Monday.
        self._add_easter_monday(tr("Lunes de Pascua"))

        if self._year not in {2012, 2014, 2018}:
            # Saint James' Day.
            self._add_observed(self._add_saint_james_day(tr("Santiago Apóstol")), rule=SUN_TO_NONE)

        if 2011 <= self._year <= 2014:
            # País Vasco Day.
            self._add_holiday_oct_25(tr("Día del País Vasco"))

    def _populate_subdiv_ri_public_holidays(self):
        if self._year in {2009, 2010, 2012}:
            # Saint Joseph's Day.
            self._add_saint_josephs_day(tr("San José"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        if self._year not in {2009, 2010, 2012, 2018}:
            # Easter Monday.
            self._add_easter_monday(tr("Lunes de Pascua"))

        # La Rioja Day.
        self._move_holiday(self._add_holiday_jun_9(tr("Día de La Rioja")))

        # Add when Christmas Day (until 2016) falls on Sunday.
        if (self._year <= 2016 and self._is_sunday(DEC, 25)) or self._year == 2008:
            # Saint James' Day.
            self._add_saint_james_day(tr("Santiago Apóstol"))

        # Constitution Day.
        self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year <= 2019:
            # Immaculate Conception.
            self._move_holiday(self._add_immaculate_conception_day(tr("Inmaculada Concepción")))

        if self._year >= 2022:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))

    def _populate_subdiv_vc_public_holidays(self):
        # Saint Joseph's Day.
        self._add_observed(self._add_saint_josephs_day(tr("San José")), rule=SUN_TO_NONE)

        # Add when Saint Joseph's Day (until 2017) or Valencian Community Day falls on Sunday.
        if (
            (self._year <= 2017 and self._is_sunday(MAR, 19))
            or self._is_sunday(OCT, 9)
            or self._year in {2008, 2009}
        ):
            # Maundy Thursday.
            self._add_holy_thursday(tr("Jueves Santo"))

        # Easter Monday.
        self._add_easter_monday(tr("Lunes de Pascua"))

        if self._year == 2011:
            # Labor Day.
            self._move_holiday(self._add_labor_day(tr("Fiesta del Trabajo")))

        if self._year >= 2019:
            # Saint John the Baptist.
            self._add_saint_johns_day(tr("San Juan"))

        self._add_observed(
            # Valencian Community Day.
            self._add_holiday_oct_9(tr("Día de la Comunidad Valenciana")),
            rule=SUN_TO_NONE,
        )

        if self._year == 2015:
            # Constitution Day.
            self._move_holiday(self._add_holiday_dec_6(tr("Día de la Constitución Española")))

        if self._year == 2016:
            # Christmas Day.
            self._move_holiday(self._add_christmas_day(tr("Natividad del Señor")))


class ES(Spain):
    pass


class ESP(Spain):
    pass


class SpainIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2010, 2026)
    EID_AL_ADHA_DATES = {
        2010: (NOV, 17),
        2012: (OCT, 25),
        2015: (SEP, 25),
        2016: (SEP, 12),
        2018: (AUG, 22),
        2019: (AUG, 12),
        2023: (JUN, 29),
        2024: (JUN, 17),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2022, 2026)
    EID_AL_FITR_DATES = {
        2022: (MAY, 3),
        2025: (MAR, 31),
    }


class SpainStaticHolidays:
    special_ga_public_holidays = {
        # Day following Saint Joseph's Day.
        2015: (MAR, 20, tr("Día siguiente a San José")),
    }

    special_md_public_holidays = {
        # Saint Joseph's Day Transfer.
        2013: (MAR, 18, tr("Traslado de San José")),
    }

    special_pv_public_holidays = {
        # 80th Anniversary of the first Basque Government.
        2016: (OCT, 7, tr("80 Aniversario del primer Gobierno Vasco")),
        # V Centennial of the Circumnavigation of the World.
        2022: (SEP, 6, tr("V Centenario Vuelta al Mundo")),
    }

    special_vc_public_holidays = {
        # The Fallas.
        2013: (MAR, 18, tr("Lunes de Fallas")),
    }
