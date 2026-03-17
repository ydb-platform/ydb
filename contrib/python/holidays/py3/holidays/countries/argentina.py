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
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.constants import ARMENIAN, BANK, GOVERNMENT, HEBREW, ISLAMIC, PUBLIC
from holidays.groups import (
    ChristianHolidays,
    HebrewCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    THU_TO_NEXT_MON,
    TUE_WED_TO_PREV_MON,
    THU_FRI_TO_NEXT_MON,
)


class Argentina(
    ObservedHolidayBase,
    ChristianHolidays,
    HebrewCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Argentina holidays.

    References:
        * [Spanish Wikipedia](https://es.wikipedia.org/wiki/Anexo:Días_festivos_en_Argentina)
        * [Decree-Law 2446](https://web.archive.org/web/20250421214226/https://www.argentina.gob.ar/normativa/nacional/decreto_ley-2446-1956-161122/texto)
        * [Law 21329](https://web.archive.org/web/20250415074830/https://www.argentina.gob.ar/normativa/nacional/ley-21329-65453/texto)
        * [Law 22655](https://web.archive.org/web/20240109181150/https://www.argentina.gob.ar/normativa/nacional/ley-22655-140416/texto)
        * [Law 22769](https://web.archive.org/web/20240114120630/https://www.argentina.gob.ar/normativa/nacional/ley-22769-65454/texto)
        * [Decree 901/1984](https://web.archive.org/web/20240115193220/https://www.argentina.gob.ar/normativa/nacional/decreto-901-1984-65455)
        * [Law 23555](https://web.archive.org/web/20250415201749/https://www.argentina.gob.ar/normativa/nacional/ley-23555-20986/texto)
        * [Law 24023](https://web.archive.org/web/20250421105240/https://www.argentina.gob.ar/normativa/nacional/ley-24023-422/texto)
        * [Law 24160](https://web.archive.org/web/20250419110215/https://www.argentina.gob.ar/normativa/nacional/ley-24160-558/texto)
        * [Law 24360](https://web.archive.org/web/20250415204304/https://www.argentina.gob.ar/normativa/nacional/ley-24360-756/texto)
        * [Law 24445](https://web.archive.org/web/20250416044658/https://www.argentina.gob.ar/normativa/nacional/ley-24445-782/texto)
        * [Law 24571](https://web.archive.org/web/20250414000940/https://www.argentina.gob.ar/normativa/nacional/ley-24571-29129/texto)
        * [Law 24757](https://web.archive.org/web/20250414003007/https://www.argentina.gob.ar/normativa/nacional/ley-24757-41168/texto)
        * [Law 25370](https://web.archive.org/web/20250415155957/https://www.argentina.gob.ar/normativa/nacional/ley-25370-65442/texto)
        * [Law 25442](https://web.archive.org/web/20250417162156/https://www.argentina.gob.ar/normativa/nacional/ley-25442-67888/texto)
        * [Decree 1932/2002](https://web.archive.org/web/20240114120622/https://www.argentina.gob.ar/normativa/nacional/decreto-1932-2002-78207/texto)
        * [Law 26085](https://web.archive.org/web/20250422065647/https://www.argentina.gob.ar/normativa/nacional/ley-26085-114811/texto)
        * [Law 26089](https://web.archive.org/web/20250414041748/https://www.argentina.gob.ar/normativa/nacional/ley-26089-115677/texto)
        * [Law 26110](https://web.archive.org/web/20250414182412/https://www.argentina.gob.ar/normativa/nacional/ley-26110-117507/texto)
        * [Law 26199](https://web.archive.org/web/20250425020808/https://www.argentina.gob.ar/normativa/nacional/ley-26199-124099/texto)
        * [Law 26416](https://web.archive.org/web/20250416115107/https://www.argentina.gob.ar/normativa/nacional/ley-26416-145231/texto)
        * [Decree 1584/2010](https://web.archive.org/web/20250415031046/https://www.argentina.gob.ar/normativa/nacional/decreto-1584-2010-174389/texto)
        * [Decree 521/2011](https://web.archive.org/web/20250416013752/https://www.argentina.gob.ar/normativa/nacional/decreto-521-2011-181754/texto)
        * [Decree 2226/2015](https://web.archive.org/web/20250415163728/https://www.argentina.gob.ar/normativa/nacional/decreto-2226-2015-253971/texto)
        * [Law 26876](https://web.archive.org/web/20250415210339/https://www.argentina.gob.ar/normativa/nacional/ley-26876-218152/texto)
        * [Law 27258](https://web.archive.org/web/20250416110147/https://www.argentina.gob.ar/normativa/nacional/ley-27258-262574/texto)
        * [Decree 52/2017](https://web.archive.org/web/20250415221116/https://www.argentina.gob.ar/normativa/nacional/decreto-52-2017-271094/texto)
        * [Decree 80/2017](https://web.archive.org/web/20231214090313/https://www.argentina.gob.ar/normativa/nacional/decreto-80-2017-271382/texto)
        * [Law 27399](https://web.archive.org/web/20250424202106/https://www.argentina.gob.ar/normativa/nacional/ley-27399-281835/texto)
        * [Decreto 297/2020](https://web.archive.org/web/20250416211712/https://www.argentina.gob.ar/normativa/nacional/decreto-297-2020-335741/texto)
        * [Collective Agreement 18/75](https://web.archive.org/web/20250415144525/https://labancaria.org/convenio-colectivo-18-75/)

    Subdivisions Holidays References:
        * Catamarca:
            * [Law 4553](https://web.archive.org/web/20250427173633/https://www.argentina.gob.ar/normativa/provincial/ley-4553-123456789-0abc-defg-355-4000kvorpyel)
            * [Law 5525](https://web.archive.org/web/20250427173109/https://digesto.catamarca.gob.ar/ley/ley_detail/1650)
        * Chubut:
            * [Law I-85](https://web.archive.org/web/20240926052315/https://digesto.legislaturadelchubut.gob.ar/public/rama/1/ley/85)
            * [Law I-547](https://web.archive.org/web/20250422154722/https://www.argentina.gob.ar/normativa/provincial/ley-547-123456789-0abc-defg-745-0001uvorpyel/actualizacion)
        * Corrientes:
            * [Law 5874](https://web.archive.org/web/20250415160018/https://www.argentina.gob.ar/normativa/provincial/ley-5874-123456789-0abc-defg-478-5000wvorpyel/actualizacion)
            * [Law 5884](https://web.archive.org/web/20250415204439/https://www.argentina.gob.ar/normativa/provincial/ley-5884-123456789-0abc-defg-488-5000wvorpyel/actualizacion)
        * Entre Ríos:
            * [Law 7285/84](https://web.archive.org/web/20241208122634/https://www.entrerios.gov.ar/dgrrhh/normativas/7285%20-%20Feriado%203%20de%20febrero.pdf)
            * [Decree 4359/93](https://web.archive.org/web/20240626093740/https://www.entrerios.gov.ar/dgrrhh/normativas/1993%20-%204359%20MGJE%20(San%20Miguel%20Arcangel).pdf)
            * [Decree 216/2003](https://web.archive.org/web/20220816033046/https://www.entrerios.gov.ar/dgrrhh/normativas/2003%20-%20216%20GOB%20(Dia%20del%20Empleado%20Publico).pdf)
            * [Law 10541](https://web.archive.org/web/20250417063744/https://www.argentina.gob.ar/normativa/provincial/ley-10541-123456789-0abc-defg-145-0100evorpyel/actualizacion)
        * Jujuy:
            * [Law 4059](https://web.archive.org/web/20250416043711/https://boletinoficial.jujuy.gob.ar/?p=44711)
            * [Law 4927](https://web.archive.org/web/20250415213548/https://boletinoficial.jujuy.gob.ar/?p=56587)
            * [Law 5005](https://web.archive.org/web/20250414192446/https://boletinoficial.jujuy.gob.ar/?p=51196)
            * [Law 6197](https://web.archive.org/web/20250422010737/https://boletinoficial.jujuy.gob.ar/?p=204052)
        * La Rioja:
            * [Law 6886](https://web.archive.org/web/20250415205303/https://www.argentina.gob.ar/normativa/provincial/ley-6886-123456789-0abc-defg-688-6000fvorpyel/actualizacion)
            * [Law 9844](https://web.archive.org/web/20250416124635/https://www.argentina.gob.ar/normativa/provincial/ley-9844-123456789-0abc-defg-448-9000fvorpyel/actualizacion)
            * [Law 9955](https://web.archive.org/web/20250414092438/https://www.argentina.gob.ar/normativa/provincial/ley-9955-123456789-0abc-defg-559-9000fvorpyel/actualizacion)
            * [Law 10242](https://web.archive.org/web/20250414195737/https://www.argentina.gob.ar/normativa/provincial/ley-10242-123456789-0abc-defg-242-0100fvorpyel/actualizacion)
            * [Law 10298](https://web.archive.org/web/20250416014331/https://www.argentina.gob.ar/normativa/provincial/ley-10298-123456789-0abc-defg-892-0100fvorpyel/actualizacion)
            * [Law 1035](https://web.archive.org/web/20250415042554/https://www.argentina.gob.ar/normativa/provincial/ley-1035-123456789-0abc-defg-153-0100fvorpyel/actualizacion)
        * Mendoza:
            * [Law 4081](https://web.archive.org/web/20250418220022/https://www.argentina.gob.ar/normativa/provincial/ley-4081-123456789-0abc-defg-534-0000mvorpyel/actualizacion)
        * Salta:
            * [Law 5032](https://web.archive.org/web/20250414055533/https://www.argentina.gob.ar/normativa/provincial/ley-5032-123456789-0abc-defg-230-5000avorpyel/actualizacion)
        * San Juan:
            * [Law 14-P](https://web.archive.org/web/20250427173119/https://www.argentina.gob.ar/normativa/provincial/ley-14-123456789-0abc-defg-410-0061jvorpyel/actualizacion)
        * San Luis:
            * [Law II-0046-2004](https://web.archive.org/web/20250427173701/https://www.argentina.gob.ar/normativa/provincial/ley-46-123456789-0abc-defg-640-0001dvorpyel/actualizacion)
        * Santa Cruz:
            * [Law 2882](https://web.archive.org/web/20250427173158/https://www.argentina.gob.ar/normativa/provincial/ley-2882-123456789-0abc-defg-288-2000zvorpyel/actualizacion)
            * [Law 3342](https://web.archive.org/web/20250427173122/https://www.argentina.gob.ar/normativa/provincial/ley-3342-123456789-0abc-defg-243-3000zvorpyel/actualizacion)
            * [Law 3419](https://web.archive.org/web/20250427173122/https://www.argentina.gob.ar/normativa/provincial/ley-3419-123456789-0abc-defg-914-3000zvorpyel/actualizacion)
            * [Law 3668](https://web.archive.org/web/20250427173133/https://www.argentina.gob.ar/normativa/provincial/ley-3668-123456789-0abc-defg-866-3000zvorpyel/actualizacion)
        * Tierra del Fuego:
            * [Law 7/92](https://web.archive.org/web/20250427173650/https://www.argentina.gob.ar/normativa/provincial/ley-7-123456789-0abc-defg-751-0000vvorpyel/actualizacion)
            * [Law 1389/2021](https://web.archive.org/web/20250427173101/https://www.argentina.gob.ar/normativa/provincial/ley-1389-123456789-0abc-defg-588-1000vvorpyel/actualizacion)
        * Tucumán:
            * [Law 1765](https://web.archive.org/web/20241213162228/https://fet.com.ar/nota/1054/feriado-en-tucuman-24-de-setiembre)

    Other sources:
        * <https://web.archive.org/web/20250402221607/https://biblioteca.afip.gob.ar/search/query/index.aspx>
        * <https://web.archive.org/web/20250219140544/https://www.argentina.gob.ar/interior/feriados>
        * <https://web.archive.org/web/20250403071214/https://servicios.lanacion.com.ar/feriados>
        * <https://web.archive.org/web/20250406124816/https://www.clarin.com/feriados>
        * <https://web.archive.org/web/20250426175526/http://www.saij.gob.ar/>

    Checked With:
        * <https://web.archive.org/web/20250501050306/https://www.lanacion.com.ar/sociedad/conoce-el-calendario-de-feriados-2015-nid1741224/>
        * <https://web.archive.org/web/20250501050042/https://www.lanacion.com.ar/sociedad/como-es-el-calendario-de-feriados-para-2016-nid1846640/>
        * <https://web.archive.org/web/20250501045504/https://www.lanacion.com.ar/sociedad/feriados-puente-2017-gobierno-calendario-nid1978221/>
        * <https://web.archive.org/web/20250501045044/https://www.lanacion.com.ar/sociedad/feriados-2018-asi-quedo-el-calendario-nid2081185/>
        * <https://web.archive.org/web/20201102185631/https://www.argentina.gob.ar/interior/feriados-nacionales-2019>
        * <https://web.archive.org/web/20220111194913/https://www.argentina.gob.ar/interior/feriados-nacionales-2020>
        * <https://web.archive.org/web/20220527150354/https://www.argentina.gob.ar/interior/feriados-nacionales-2021>
        * <https://web.archive.org/web/20230307074516/https://www.argentina.gob.ar/interior/feriados-nacionales-2022>
        * <https://web.archive.org/web/20231231171358/https://www.argentina.gob.ar/interior/feriados-nacionales-2023>
        * <https://web.archive.org/web/20241211171318/https://www.argentina.gob.ar/interior/feriados-nacionales-2024>
        * <https://web.archive.org/web/20250106182732/https://www.argentina.gob.ar/interior/feriados-nacionales-2025>
        * [Entre Ríos](https://web.archive.org/web/20241003004359/https://www.entrerios.gov.ar/dgrrhh/index.php?i=7)
        * [Tierra del Fuego](https://web.archive.org/web/20240716112344/https://www.justierradelfuego.gov.ar/dias-no-laborables/)
        * Jujuy:
            * <https://web.archive.org/web/20241202223127/http://agpjujuy.gob.ar/Archivos/Calendario2024.pdf>
            * <https://web.archive.org/web/20250427172904/https://agpjujuy.gob.ar/Archivos/Calendario2025.pdf>
    """

    country = "AR"
    default_language = "es"
    # %s (estimated).
    estimated_label = tr("%s (estimado)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observado, estimado)")
    # %s (observed).
    observed_label = tr("%s (observado)")
    # Decree-Law 2446.
    start_year = 1957
    subdivisions = (
        "A",  # Salta.
        "B",  # Buenos Aires.
        "C",  # Ciudad Autónoma de Buenos Aires.
        "D",  # San Luis.
        "E",  # Entre Ríos.
        "F",  # La Rioja.
        "G",  # Santiago del Estero.
        "H",  # Chaco.
        "J",  # San Juan.
        "K",  # Catamarca.
        "L",  # La Pampa.
        "M",  # Mendoza.
        "N",  # Misiones.
        "P",  # Formosa.
        "Q",  # Neuquén.
        "R",  # Río Negro.
        "S",  # Santa Fe.
        "T",  # Tucumán.
        "U",  # Chubut.
        "V",  # Tierra del Fuego.
        "W",  # Corrientes.
        "X",  # Córdoba.
        "Y",  # Jujuy.
        "Z",  # Santa Cruz.
    )
    subdivisions_aliases = {
        "Salta": "A",
        "Buenos Aires": "B",
        "Ciudad Autónoma de Buenos Aires": "C",
        "San Luis": "D",
        "Entre Ríos": "E",
        "La Rioja": "F",
        "Santiago del Estero": "G",
        "Chaco": "H",
        "San Juan": "J",
        "Catamarca": "K",
        "La Pampa": "L",
        "Mendoza": "M",
        "Misiones": "N",
        "Formosa": "P",
        "Neuquén": "Q",
        "Río Negro": "R",
        "Santa Fe": "S",
        "Tucumán": "T",
        "Chubut": "U",
        "Tierra del Fuego": "V",
        "Corrientes": "W",
        "Córdoba": "X",
        "Jujuy": "Y",
        "Santa Cruz": "Z",
    }
    supported_languages = ("en_US", "es", "uk")
    supported_categories = (ARMENIAN, BANK, GOVERNMENT, HEBREW, ISLAMIC, PUBLIC)

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        HebrewCalendarHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=ArgentinaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, ArgentinaStaticHolidays)
        # Law 23555.
        kwargs.setdefault("observed_rule", TUE_WED_TO_PREV_MON + THU_FRI_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 1988)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        # Established in 1957 by Decree-Law 2446.
        # Abolished in 1977 by Law 21329.

        if self._year <= 1976:
            # Epiphany.
            self._add_epiphany_day(tr("Día de Reyes"))

        # Established in 1956 by Decree-Law 2446.
        # Abolished in 1977 by Law 21329.
        # Restored in 2011 by Decree 1584/2010.

        if self._year <= 1976 or self._year >= 2011:
            # Carnival Monday.
            self._add_carnival_monday(tr("Lunes de Carnaval"))

            # Carnival Tuesday.
            self._add_carnival_tuesday(tr("Martes de Carnaval"))

        # Established in 2006 by Law 26085.

        if self._year >= 2006:
            self._add_holiday_mar_24(
                # National Day of Remembrance for Truth and Justice.
                tr("Día Nacional de la Memoria por la Verdad y la Justicia")
            )

        # Established in 1993 by Law 24160.
        # Merged in 2001 with "Day of Affirmation of Argentine Rights..." by Law 25370.
        # Made fixed in 2007 by Law 26110.
        # Got moved temporary in 2020 (Decree 297/2020).

        if self._year >= 1993:
            name = (
                # War Veteran's Day.
                tr("Día del Veterano de Guerra")
                if self._year <= 2000
                # Veteran's Day and the Fallen in the Malvinas War.
                else tr("Día del Veterano y de los Caidos en la Guerra de Malvinas")
            )
            if self._year == 2020:
                self._add_holiday_mar_31(name)
            else:
                apr_2 = self._add_holiday_apr_2(name)
                if self._year <= 2006:
                    self._move_holiday(apr_2, show_observed_label=False)

        # Established in 1957 by Decree-Law 2446.
        # Abolished in 1977 by Law 21329.
        # Restored in 2011 by Decree 1584/2010 (as non-working day).

        if self._year <= 1976 or self._year >= 2011:
            # Maundy Thursday.
            self._add_holy_thursday(tr("Jueves Santo"))

        # Established in 1977 by Law 21329.

        if self._year >= 1977:
            # Good Friday.
            self._add_good_friday(tr("Viernes Santo"))

        # Labor Day.
        self._add_labor_day(tr("Día del Trabajo"))

        # May Revolution Day.
        self._add_holiday_may_25(tr("Día de la Revolución de Mayo"))

        # Established in 1957 by Decree-Law 2446.
        # Abolished in 1976 by Law 21329.

        if self._year <= 1975:
            # Corpus Christi.
            self._add_corpus_christi_day(tr("Corpus Christi"))

        # Established in 1983 on April 2 by Law 22769.
        # Moved to June 10 by Decree 901/1984.
        # Made movable in 1988 by Law 23555.
        # Abandoned in 2001 (superseded by "Veterans Day and the Fallen in the Malvinas War").

        if 1983 <= self._year <= 2000:
            # Day of Affirmation of Argentine Rights over the Malvinas, Islands and
            # Antarctic Sector.
            name = tr(
                "Día de la Afirmación de los Derechos Argentinos sobre las Malvinas, "
                "Islas y Sector Antártico"
            )
            if self._year == 1983:
                self._add_holiday_apr_2(name)
            else:
                self._move_holiday(self._add_holiday_jun_10(name), show_observed_label=False)

        # Established in 2016 by Law 27258.

        if self._year >= 2016:
            # If Jun 17 is Friday, then it should move to Mon, Jun 20
            # but Jun 20 is Gen. Belgrano holiday.
            self._move_holiday(
                self._add_holiday_jun_17(
                    # Pass to the Immortality of General Don Martín Miguel de Güemes.
                    tr("Paso a la Inmortalidad del General Don Martín Miguel de Güemes")
                ),
                rule=TUE_WED_TO_PREV_MON + THU_TO_NEXT_MON,
                show_observed_label=False,
            )

        # Established in 1938 as fixed by Law 12361.
        # Made movable in 1988 by Law 23555.
        # Made fixed in 1992 by Law 24023.
        # Set as 3rd MON of JUN in 1995 by Law 24445.
        # Made fixed in 2011 by Decree 1584/2010.
        # Also called "National Flag Day" (Día de la Bandera Nacional).

        # Pass to the Immortality of General Don Manuel Belgrano.
        name = tr("Paso a la Inmortalidad del General Don Manuel Belgrano")

        if 1995 <= self._year <= 2010:
            self._add_holiday_3rd_mon_of_jun(name)
        else:
            jun_20 = self._add_holiday_jun_20(name)
            if self._year <= 1991:
                self._move_holiday(jun_20, show_observed_label=False)

        # Independence Day.
        self._add_holiday_jul_9(tr("Día de la Independencia"))

        # Established in 1957 by Decree-Law 2446.
        # Abolished in 1976 by Law 21329.

        # Established in 1938 as fixed by Law 12387.
        # Made movable in 1988 by Law 23555.
        # Set as 3rd MON of AUG in 1995 by Law 24445.
        # Moved to Aug 22 for 2011 (election interfere) by Decree 521/2011.
        # Made movable in 2017 by Decree 52/2017.

        # Pass to the Immortality of General Don José de San Martín.
        name = tr("Paso a la Inmortalidad del General Don José de San Martín")
        if self._year <= 1994 or self._year >= 2017:
            self._move_holiday(self._add_holiday_aug_17(name), show_observed_label=False)
        elif self._year == 2011:
            self._add_holiday_aug_22(name)
        else:
            self._add_holiday_3rd_mon_of_aug(name)

        # Established in 1957 by Decree-Law 2446.
        # Abolished in 1977 by Law 21329.

        if self._year <= 1975:
            # Assumption Day.
            self._add_assumption_of_mary_day(tr("Día de la Asunción"))

        # Established in 1917.
        # Abolished in 1976 by Law 21329.
        # Restored in 1982 by Law 22655.
        # Made movable in 1988 by Law 23555.
        # Observed on Oct 8 in 2001 by Law 25442.
        # Observed on Oct 14 in 2002 by Decree 1932/2002.
        # Changed moving rule in 2008 by Law 26146
        # 2008-2009 actual dates match the dates by the 2010 rule, so code is simplified.
        # Changed name and moving rule in 2010 by Decree 1584/2010.
        # Changed moving rule in 2017 by Decree 52/2017.

        if self._year <= 1975 or self._year >= 1982:
            name = (
                # Respect for Cultural Diversity Day.
                tr("Día del Respeto a la Diversidad Cultural")
                if self._year >= 2010
                # Columbus Day.
                else tr("Día de la Raza")
            )
            if self._year == 2001:
                self._add_holiday_oct_8(name)
            elif self._year == 2002:
                self._add_holiday_oct_14(name)
            elif 2008 <= self._year <= 2016:
                self._add_holiday_2nd_mon_of_oct(name)
            else:
                self._move_holiday(self._add_columbus_day(name), show_observed_label=False)

        # Established in 1957 by Decree-Law 2446.
        # Abolished in 1977 by Law 21329.

        if self._year <= 1975:
            # All Saints' Day.
            self._add_all_saints_day(tr("Todos Los Santos"))

        # First observed with no holiday in 1974 by Law 20770.
        # Established in 2010 as 4th MON of NOV by Decree 1584/2010.
        # Moved to Nov 27 in 2015 by Decree 2226/2015.
        # Moved to Nov 28 again for 2016.

        if self._year >= 2010:
            # National Sovereignty Day.
            name = tr("Día de la Soberanía Nacional")
            if self._year >= 2017:
                self._move_holiday(self._add_holiday_nov_20(name), show_observed_label=False)
            elif self._year == 2016:
                self._add_holiday_nov_28(name)
            elif self._year == 2015:
                self._add_holiday_nov_27(name)
            else:
                self._add_holiday_4th_mon_of_nov(name)

        # Established in 1957 by Decree-Law 2446.
        # Abolished in 1976 by Law 21329.
        # Restored in 1995 by Law 24445.

        if self._year <= 1975 or self._year >= 1995:
            # Immaculate Conception.
            self._add_immaculate_conception_day(tr("Inmaculada Concepción de María"))

        # Christmas Day.
        self._add_christmas_day(tr("Navidad"))

    def _populate_subdiv_a_public_holidays(self):
        """Salta holidays."""

        # Law 5032.
        if self._year >= 1977:
            # Anniversary of the Battle of Salta.
            self._add_holiday_feb_20(tr("Aniversario de la Batalla de Salta"))

            self._add_holiday_jun_17(
                # Day of Memory of General Don Martín Miguel de Güemes.
                tr(
                    "Dia de la memoria del Guerrero de la Independencia y Gobernador "
                    "de la Provincia de Salta General Don Martín Miguel de Güemes"
                )
            )

            # Feasts of the Lord and the Virgin of Miracle.
            name = tr("Festividades del Señor y de la Virgen del Milagro")
            self._add_holiday_sep_13(name)
            self._add_holiday_sep_14(name)
            self._add_holiday_sep_15(name)

    def _populate_subdiv_d_public_holidays(self):
        """San Luis holidays."""

        # Law II-0046-2004.
        if self._year >= 2004:
            # Exaltation of the Holy Cross Day.
            self._add_holiday_may_3(tr("Día de la Exaltación de la Santa Cruz"))

            # Saint Louis the King of France's Day.
            self._add_holiday_aug_25(tr("Día de San Luis Rey de Francia"))

    def _populate_subdiv_e_public_holidays(self):
        """Entre Ríos holidays."""

        # Law 10541.
        if self._year >= 2018:
            self._add_holiday_mar_24(
                # Provincial Day of Remembrance for Truth and Justice.
                tr("Día Provincial de la Memoria por la Verdad y la Justicia")
            )

        # Law 7285/84.
        if self._year >= 1984:
            # Commemoration of the Battle of Caseros.
            self._add_holiday_feb_3(tr("Conmemoración de la Batalla de Caseros"))

        # Decree 216/2003.
        if self._year >= 2004:
            # State Worker's Day.
            self._add_holiday_jun_27(tr("Día del Trabajador Estatal"))

        # Decree 4359/93.
        if self._year >= 1993:
            # Saint Michael the Archangel's Day.
            self._add_holiday_sep_29(tr("San Miguel Arcángel"))

    def _populate_subdiv_f_public_holidays(self):
        """La Rioja holidays."""

        # Law 1035.
        if self._year >= 2021:
            # Day of the Death of Juan Facundo Quiroga.
            self._add_holiday_feb_16(tr("Día del fallecimiento de Juan Facundo Quiroga"))

        # Law 10242.
        if self._year >= 2020:
            # Provincial Autonomy Day.
            self._add_holiday_mar_1(tr("Día de la Autonomía Provincial"))

        # Law 9955.
        if self._year >= 2017:
            # Day of Remembrance for Truth and Justice.
            self._add_holiday_mar_24(tr("Día de la Memoria por la Verdad y la Justicia"))

        # Established by Law 9955.
        # Abolished by Law 10298.
        if 2017 <= self._year <= 2020:
            # Malvinas Memorial Day.
            self._add_holiday_apr_2(tr("Día de los Caídos en Malvinas"))

        # Law 6886.
        if self._year >= 2000:
            # La Rioja Foundation Day.
            self._add_holiday_may_20(tr("Día de la fundación de La Rioja"))

        # Law 9844.
        if self._year >= 2016:
            self._add_holiday_aug_4(
                # Anniversary of the Death of Enrique Angelelli.
                tr("Día del Aniversario del Fallecimiento de Monseñor Enrique Angelelli")
            )

        # Law 10298.
        if self._year >= 2020:
            self._add_holiday_nov_12(
                # Anniversary of the Death of Ángel Vicente Peñaloza.
                tr("Día del Aniversario del Fallecimiento de Ángel Vicente Peñaloza")
            )

        # Law 6886.
        if self._year >= 2000:
            # Tinkunaco Festival.
            self._add_holiday_dec_31(tr("Día del Tinkunaco Riojano"))

    def _populate_subdiv_j_public_holidays(self):
        """San Juan holidays."""

        # Law 14-P.
        if self._year >= 2015:
            # Teacher's Day.
            self._add_holiday_sep_11(tr("Día del Maestro"))

    def _populate_subdiv_k_public_holidays(self):
        """Catamarca holidays."""

        # Law 4553.
        if self._year >= 1990:
            # Birthday of Mamerto Esquiú.
            self._add_holiday_may_11(tr("Natalicio de Fray Mamerto Esquiú"))

            # Catamarca Autonomy Day.
            self._add_holiday_aug_25(tr("Autonomía de Catamarca"))

        # Law 5525.
        if self._year >= 2018:
            # Miracle Day.
            self._add_holiday_sep_7(tr("Día del Milagro"))

        # Law 4553.
        if self._year >= 1989:
            # Immaculate Conception.
            self._add_immaculate_conception_day(tr("Inmaculada Concepción de María"))

    def _populate_subdiv_m_public_holidays(self):
        """Mendoza holidays."""

        # Law 4081.
        if self._year >= 1977:
            # Saint James' Day.
            self._add_saint_james_day(tr("Día del Apóstol Santiago"))

    def _populate_subdiv_t_public_holidays(self):
        """Tucumán holidays."""

        # Law 1765.

        # Anniversary of the Battle of Tucumán.
        self._add_holiday_sep_24(tr("Aniversario de la Batalla de Tucumán"))

    def _populate_subdiv_u_public_holidays(self):
        """Chubut holidays."""

        # Law I-85.
        if self._year >= 1984:
            # Plebiscite 1902 Trevelin.
            self._add_holiday_apr_30(tr("Plebiscito 1902 Trevelin"))

            self._add_holiday_jul_28(
                # Anniversary of the arrival of the first Welsh settlers.
                tr("Aniversario del arribo de los primeros colonizadores galeses")
            )

            # National Petroleum Day.
            self._add_holiday_dec_13(tr("Día del Petróleo Nacional"))

        # Law I-547.
        if self._year >= 2015:
            self._add_holiday_nov_3(
                # Tehuelches and Mapuches declare loyalty to the Argentine flag.
                tr("Tehuelches y Mapuches declaran lealtad a la bandera Argentina")
            )

    def _populate_subdiv_v_public_holidays(self):
        """Tierra del Fuego holidays."""

        # Law 7/92.
        if self._year >= 1992:
            self._add_holiday_jun_1(
                # Day of the Province of Tierra del Fuego, Antarctica
                # and the South Atlantic Islands.
                tr("Día de la Provincia de Tierra del Fuego, Antártida e Islas del Atlántico Sur")
            )

        # Law 1389/2021.
        if self._year >= 2021:
            # Selk'Nam Genocide Day.
            self._add_holiday_nov_25(tr("Día del Genocidio Selk'Nam"))

    def _populate_subdiv_w_public_holidays(self):
        """Corrientes holidays."""

        if self._year >= 2009:
            # Law 5884.
            self._add_holiday_jun_20(
                # Anniversary of the Death of General Manuel Belgrano.
                tr(
                    "Día del Aniversario del Fallecimiento del General Manuel José Joaquín "
                    "del Corazón de Jesús Belgrano"
                )
            )

            # Law 5874.
            self._add_holiday_aug_17(
                # Anniversary of the Death of General José Francisco de San Martín.
                tr(
                    "Día del Aniversario del Fallecimiento del General José Francisco "
                    "de San Martín"
                )
            )

    def _populate_subdiv_y_public_holidays(self):
        """Jujuy holidays."""

        # Law 4059.
        if self._year >= 1984:
            # Carnival Monday.
            self._add_carnival_monday(tr("Lunes de Carnaval"))

            # Carnival Tuesday.
            self._add_carnival_tuesday(tr("Martes de Carnaval"))

            # Jujuy Exodus Day.
            self._add_holiday_aug_23(tr("Día del Éxodo Jujeño"))

            # Jujuy Political Autonomy Day.
            self._add_holiday_nov_18(tr("Autonomía Política de Jujuy"))

        # Law 4927.
        if self._year >= 1996:
            # Pachamama Day.
            self._add_holiday_aug_1(tr("Día de la Pachamama"))

        # Law 5005.
        if self._year >= 1997:
            # Day of the Virgin of the Rosary of Río Blanco and Paypaya.
            self._add_holiday_oct_7(tr("Día de la Virgen del Rosario de Río Blanco y Paypaya"))

        # Law 6197.
        if self._year >= 2021:
            # Great Day of Jujuy.
            self._add_holiday_apr_27(tr("Día Grande de Jujuy"))

    def _populate_subdiv_z_public_holidays(self):
        """Santa Cruz holidays."""

        # Law 2882, Law 3419.
        if self._year >= 2007:
            # Saint John Bosco's Day.
            name = tr("Homenaje al Patrono de la Provincia San Juan Bosco")
            if self._year >= 2015:
                self._add_holiday_aug_16(name)
            else:
                self._add_holiday_jan_31(name)

        # Law 3342.
        if self._year >= 2014:
            self._add_holiday_oct_27(
                # Anniversary of the Death of Néstor Carlos Kirchner.
                tr(
                    "Día del Aniversario del Fallecimiento del ex Presidente de la Nación "
                    "Doctor Néstor Carlos Kirchner"
                )
            )

        # Law 3669.
        if self._year >= 2019:
            self._add_holiday_dec_7(
                # Commemoration of the workers shot in the Patagonian Strikes.
                tr("Conmemoración a los obreros fusilados en las Huelgas Patagónicas")
            )

    def _populate_armenian_holidays(self):
        # Established in 2007 by Law 26199.

        if self._year >= 2007:
            self._add_holiday_apr_24(
                # Day of Action for Tolerance and Respect among Peoples.
                tr("Día de acción por la tolerancia y el respeto entre los pueblos")
            )

    def _populate_bank_holidays(self):
        # Established in 1975 by Collective Agreement 18/75, Art. 50.

        if self._year >= 1975:
            # Bankers' Day.
            self._add_holiday_nov_6(tr("Día del Bancario"))

    def _populate_government_holidays(self):
        # Established in 2014 by Law 26876.

        if self._year >= 2014:
            # State Worker's Day.
            self._add_holiday_jun_27(tr("Día del Trabajador del Estado"))

    def _populate_hebrew_holidays(self):
        # Established in 1996 by Law 24571.

        if self._year >= 1996:
            # Rosh Hashanah.
            self._add_rosh_hashanah(tr("Año Nuevo Judío (Rosh Hashana)"), range(2))

            # Yom Kippur.
            self._add_yom_kippur(tr("Día del Perdón (Iom Kipur)"))

        # Established in 2007 by Law 26089.

        if self._year >= 2007:
            # Pesach.
            name = tr("Pascua Judía (Pésaj)")
            self._add_passover(name, range(2))
            self._add_passover(name, range(6, 8))

    def _populate_islamic_holidays(self):
        # Established in 1997 by Law 24757.

        if self._year >= 1997:
            # Islamic New Year.
            self._add_islamic_new_year_day(tr("Año Nuevo Musulmán (Hégira)"))

            # Eid al-Fitr.
            self._add_eid_al_fitr_day(tr("Día posterior a la culminación del ayuno (Id Al-Fitr)"))

            # Eid al-Adha.
            self._add_eid_al_adha_day(tr("Día de la Fiesta del Sacrificio (Id Al-Adha)"))


class AR(Argentina):
    pass


class ARG(Argentina):
    pass


class ArgentinaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2019, 2025)
    EID_AL_ADHA_DATES = {
        2025: (JUN, 10),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2019, 2025)
    EID_AL_FITR_DATES = {
        2025: (MAR, 31),
    }

    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2019, 2025)
    HIJRI_NEW_YEAR_DATES = {
        2021: (AUG, 8),
    }


class ArgentinaStaticHolidays:
    """Argentina special holidays.

    Special Holidays References:
        * [Decree 615/2010](https://web.archive.org/web/20240109181152/https://www.argentina.gob.ar/normativa/nacional/decreto-615-2010-166825/texto)
        * [Joint Resolution 1159/2010](https://web.archive.org/web/20231215002151/https://www.argentina.gob.ar/normativa/nacional/resolución-1159-2010-173748/texto)
        * [Law 26721](https://web.archive.org/web/20240109181220/https://www.argentina.gob.ar/normativa/nacional/ley-26721-191835/texto)
        * [Law 26763](https://web.archive.org/web/20240109181221/https://www.argentina.gob.ar/normativa/nacional/ley-26763-201915/texto)
        * [Law 26837](https://web.archive.org/web/20240109181217/https://www.argentina.gob.ar/normativa/nacional/ley-26837-207405/texto)
        * [Law 26840](https://web.archive.org/web/20240109181218/https://www.argentina.gob.ar/normativa/nacional/ley-26840-207258/texto)
        * [Decree 42/2022](https://web.archive.org/web/20241208143041/https://www.argentina.gob.ar/normativa/nacional/decreto-42-2022-360018/texto)
        * [Decree 842/2022](https://web.archive.org/web/20240114151710/https://www.argentina.gob.ar/normativa/nacional/decreto-842-2022-376857/texto)

    Special Bank Holidays References:
        * [Release P50962 (2019)](https://web.archive.org/web/20250219192339/https://www.bcra.gob.ar/Pdfs/comytexord/P50962.pdf)
        * [Release P50983 (2020)](https://web.archive.org/web/20241203102423/https://www.bcra.gob.ar/Pdfs/comytexord/p50983.pdf)
        * [Release P51020 (2021)](https://web.archive.org/web/20240616000609/http://www.bcra.gob.ar/Pdfs/comytexord/P51020.pdf)
        * [Release P51155 (2024)](https://web.archive.org/web/20250427172945/https://bcra.gob.ar/Pdfs/comytexord/P51155.pdf)
        * [Release P51199 (2025)](https://web.archive.org/web/20251221165343/https://www.bcra.gob.ar/archivos/Pdfs/comytexord/P51199.pdf)

    Special Subdivision-level Holidays References:
        * [2018 G20 Leader Summit Special Holidays for Buenos Aires](https://web.archive.org/web/20220305033755/https://www.perfil.com/noticias/sociedad/30-de-noviembre-feriado-ciudad-de-buenos-aires-cumbre-g20.phtml)

    Special Subdivision-level Holidays References:
        * [2018 G20 Leader Summit Special Holidays for Buenos Aires](https://web.archive.org/web/20220305033755/https://www.perfil.com/noticias/sociedad/30-de-noviembre-feriado-ciudad-de-buenos-aires-cumbre-g20.phtml)

    Special Bridge Holidays are given upto 3 days a year as long as it's declared
    50 days before calendar year's end.
    There's no Bridge Holidays declared in 2017.

    Bridge Holidays References:
        * [Decree 1585/2010 (2011-2013 Bridge Holidays)](https://web.archive.org/web/20240109181210/https://www.argentina.gob.ar/normativa/nacional/decreto-1585-2010-174391/texto)
        * [Decree 1768/2013 (2014-2016 Bridge Holidays)](https://web.archive.org/web/20240109181211/https://www.argentina.gob.ar/normativa/nacional/decreto-1768-2013-222021/texto)
        * [Decree 923/2017 (2018-2019 Bridge Holidays)](https://web.archive.org/web/20231215115842/https://www.argentina.gob.ar/normativa/nacional/decreto-923-2017-287145)
        * [Decree 717/2019 (2020 Bridge Holidays)](https://web.archive.org/web/20241208012315/https://www.argentina.gob.ar/normativa/nacional/decreto-717-2019-330204/texto)
        * [Decree 947/2020 (2021 Bridge Holidays)](https://web.archive.org/web/20250424202105/https://www.argentina.gob.ar/normativa/nacional/decreto-947-2020-344620/texto)
        * [Decree 789/2021 (2022 Bridge Holidays)](https://web.archive.org/web/20250114061352/https://www.argentina.gob.ar/normativa/nacional/decreto-789-2021-356678/texto)
        * [Decree 764/2022 (2023 Bridge Holidays)](https://web.archive.org/web/20250216064929/https://www.argentina.gob.ar/normativa/nacional/decreto-764-2022-375264/texto)
        * [Decree 106/2023 (2024 Bridge Holidays)](https://web.archive.org/web/20240930174820/https://www.argentina.gob.ar/normativa/nacional/decreto-106-2023-395689/texto)
        * [Decree 1017/2024 (2025 Bridge Holidays)](https://web.archive.org/web/20241227231911/https://www.argentina.gob.ar/normativa/nacional/decreto-1027-2024-406417/texto)
        * [Resolution 164/2025 (2026 Bridge Holidays)](https://web.archive.org/web/20260113123935/https://www.argentina.gob.ar/normativa/nacional/resolución-164-2025-421799/texto)
    """

    # Bridge Public Holiday.
    bridge_public_holiday = tr("Feriado con fines turísticos")

    # Bicentenary of the May Revolution.
    bicentennial_may_revolution = tr("Bicentenario de la Revolución de Mayo")

    # Bicentenary of the creation and first oath of the national flag.
    bicentennial_national_flag = tr(
        "Bicentenario de la creación y primera jura de la bandera nacional"
    )

    # Bicentenary of the Battle of Tucuman.
    bicentennial_battle_tucuman = tr("Bicentenario de la Batalla de Tucumán")

    # Bicentenary of the inaugural session of the National Constituent Assembly of the year 1813.
    bicentennial_assembly_1813 = tr(
        "Bicentenario de la sesión inaugural de la Asamblea Nacional Constituyente del año 1813"
    )

    # Bicentenary of the Battle of Salta.
    bicentennial_battle_salta = tr("Bicentenario de la Batalla de Salta")

    # National Census Day 2010.
    national_census_2010 = tr("Censo Nacional 2010")

    # G20 Leaders' Summit.
    g20_leaders_summit_holiday = tr("Cumbre de Líderes del Grupo de los 20 (G20)")

    # National Census Day 2022.
    national_census_2022 = tr("Censo Nacional 2022")

    # FIFA World Cup 2022 Victory Day.
    fifa_world_cup_2022_victory_day = tr("Día de la Victoria de la Copa Mundial de la FIFA 2022")

    # Bank Holiday.
    bank_holiday = tr("Asueto bancario")

    special_public_holidays = {
        2010: (
            # Decree 615/2010.
            (MAY, 24, bicentennial_may_revolution),
            # Joint Resolution 1159/2010.
            (OCT, 27, national_census_2010),
        ),
        2011: (
            (MAR, 25, bridge_public_holiday),
            (DEC, 9, bridge_public_holiday),
        ),
        2012: (
            # Law 26721.
            (FEB, 27, bicentennial_national_flag),
            (APR, 30, bridge_public_holiday),
            # Law 26763.
            (SEP, 24, bicentennial_battle_tucuman),
            (DEC, 24, bridge_public_holiday),
        ),
        2013: (
            # Law 26840.
            (JAN, 31, bicentennial_assembly_1813),
            # Law 26837.
            (FEB, 20, bicentennial_battle_salta),
            (APR, 1, bridge_public_holiday),
            (JUN, 21, bridge_public_holiday),
        ),
        2014: (
            (MAY, 2, bridge_public_holiday),
            (DEC, 26, bridge_public_holiday),
        ),
        2015: (
            (MAR, 23, bridge_public_holiday),
            (DEC, 7, bridge_public_holiday),
        ),
        2016: (
            (JUL, 8, bridge_public_holiday),
            (DEC, 9, bridge_public_holiday),
        ),
        2018: (
            (APR, 30, bridge_public_holiday),
            (DEC, 24, bridge_public_holiday),
            (DEC, 31, bridge_public_holiday),
        ),
        2019: (
            (JUL, 8, bridge_public_holiday),
            (AUG, 19, bridge_public_holiday),
            (OCT, 14, bridge_public_holiday),
        ),
        2020: (
            (MAR, 23, bridge_public_holiday),
            (JUL, 10, bridge_public_holiday),
            (DEC, 7, bridge_public_holiday),
        ),
        2021: (
            (MAY, 24, bridge_public_holiday),
            (OCT, 8, bridge_public_holiday),
            (NOV, 22, bridge_public_holiday),
        ),
        2022: (
            # Decree 42/2022.
            (MAY, 18, national_census_2022),
            (OCT, 7, bridge_public_holiday),
            (NOV, 21, bridge_public_holiday),
            (DEC, 9, bridge_public_holiday),
            # Decree 842/2022.
            (DEC, 20, fifa_world_cup_2022_victory_day),
        ),
        2023: (
            (MAY, 26, bridge_public_holiday),
            (JUN, 19, bridge_public_holiday),
            (OCT, 13, bridge_public_holiday),
        ),
        2024: (
            (APR, 1, bridge_public_holiday),
            (JUN, 21, bridge_public_holiday),
            (OCT, 11, bridge_public_holiday),
        ),
        2025: (
            (MAY, 2, bridge_public_holiday),
            (AUG, 15, bridge_public_holiday),
            (NOV, 21, bridge_public_holiday),
        ),
        2026: (
            (MAR, 23, bridge_public_holiday),
            (JUL, 10, bridge_public_holiday),
            (DEC, 7, bridge_public_holiday),
        ),
    }

    special_bank_holidays = {
        # Release P50962.
        2019: (
            (DEC, 24, bank_holiday),
            (DEC, 31, bank_holiday),
        ),
        # Release P50983.
        2020: (
            (DEC, 24, bank_holiday),
            (DEC, 31, bank_holiday),
        ),
        # Release P51020.
        2021: (
            (DEC, 24, bank_holiday),
            (DEC, 31, bank_holiday),
        ),
        # Release P51155.
        2024: (
            (DEC, 24, bank_holiday),
            (DEC, 31, bank_holiday),
        ),
        # Release P51199.
        2025: (
            (DEC, 24, bank_holiday),
            (DEC, 31, bank_holiday),
        ),
    }

    special_b_public_holidays = {
        2018: (NOV, 30, g20_leaders_summit_holiday),
    }
