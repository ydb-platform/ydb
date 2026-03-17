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

from holidays.calendars.gregorian import FEB, MAR, APR, MAY, AUG, OCT, DEC, SUN
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Venezuela(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Venezuela holidays.

    References:
        * <https://web.archive.org/web/20250317124608/https://dias-festivos.eu/dias-festivos/venezuela/>
        * <https://web.archive.org/web/20260104061304/https://ecoportal50a.tripod.com/directur-fiestas.htm>
        * [1909 Aug 5](https://web.archive.org/web/20241012151721/https://www.guao.org/sites/default/files/efemerides/69.Ley%20fiestas%20nacionales%201909.pdf)
        * [1918 May 19](https://web.archive.org/web/20250429081541/https://www.guao.org/sites/default/files/efemerides/70.%20Ley%20de%20fiestas%20nacionales%201918.pdf)
        * [1921 Jun 11](https://web.archive.org/web/20241012160001/https://www.guao.org/sites/default/files/efemerides/37.LEYES_Y_DECRETOS_1921_Día_de_la_raza.PDF)
        * [1945 Apr 30](https://web.archive.org/web/20250428122616/https://venezuelaenretrospectiva.wordpress.com/2016/05/01/1o-de-mayo-dia-del-trabajador-venezolano/)
        * [1971 Jun 22](https://web.archive.org/web/20240623070739/https://docs.venezuela.justia.com/federales/leyes/ley-de-fiestas-nacionales.pdf)
        * [2002 Oct 10](https://web.archive.org/web/20250214140239/https://www.acnur.org/fileadmin/Documentos/BDL/2008/6635.pdf)
        * [2012 May 7](https://web.archive.org/web/20250418204844/https://oig.cepal.org/sites/default/files/2012_leyorgtrabajo_ven.pdf)
        * [Family Day](https://web.archive.org/web/20240516041705/https://www.globovision.com/nacional/21451/ejecutivo-decreta-feriado-el-15-de-mayo-por-dia-de-la-familia)
    """

    country = "VE"
    default_language = "es"
    supported_languages = ("en_US", "es", "uk")
    # Ley de Fiestas Nacionales de 5 de agosto de 1909.
    start_year = 1910
    weekend = {SUN}

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, VenezuelaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        # Carnival Monday.
        self._add_carnival_monday(tr("Lunes de Carnaval"))

        # Carnival Tuesday.
        self._add_carnival_tuesday(tr("Martes de Carnaval"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Declaration of Independence.
        self._add_holiday_apr_19(tr("Declaración de la Independencia"))

        # Added via Decreto 113 de 30 de abril de 1945.
        if self._year >= 1945:
            # International Worker's Day.
            self._add_labor_day(tr("Dia Mundial del Trabajador"))

        # Added in 2024, confirmed to be observed in 2025.
        if self._year >= 2024:
            # Family Day.
            self._add_holiday_may_15(tr("Día de la Familia"))

        # Added via via Ley de Fiestas Nacionales de 5 de agosto de 1909.
        # Removed via Ley de Fiestas Nacionales de 19 de mayo de 1918.
        # Readded via Ley de Fiestas Nacionales de 22 de junio de 1971.
        if self._year <= 1917 or self._year >= 1971:
            # Battle of Carabobo.
            self._add_holiday_jun_24(tr("Batalla de Carabobo"))

        # Independence Day.
        self._add_holiday_jul_5(tr("Día de la Independencia"))

        # Added via Ley de Fiestas Nacionales de 19 de mayo de 1918.
        if self._year >= 1918:
            # Birthday of Simon Bolivar.
            self._add_holiday_jul_24(tr("Natalicio de Simón Bolívar"))

        # Added via Ley de Fiestas Nacionales de 11 de junio de 1921.
        # Name changed via Decreto N° 2.028, 10 de octubre de 2002.
        if self._year >= 1921:
            self._add_columbus_day(
                # Day of Indigenous Resistance.
                tr("Día de la Resistencia Indígena")
                if self._year >= 2002
                # Columbus Day.
                else tr("Día de la Raza")
            )

        # Added via Ley de Fiestas Nacionales de 5 de agosto de 1909.
        # Removed via Ley de Fiestas Nacionales de 19 de mayo de 1918.
        if self._year <= 1917:
            # Feast of Saint Simon the Zealot.
            self._add_holiday_oct_28(tr("Fiesta de San Simón Apóstol"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Nochebuena"))

        # Christmas Day.
        self._add_christmas_day(tr("Día de Navidad"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Fiesta de Fin de Año"))


class VE(Venezuela):
    pass


class VEN(Venezuela):
    pass


class VenezuelaStaticHolidays:
    """Venezuela special holidays.

    References:
        * [February 2nd, 2009](https://web.archive.org/web/20260104075141/https://venezuelanalysis.com/news/4164/)
        * [March 29th-31st, 2010](https://web.archive.org/web/20241013111835/https://accesoalajusticia.org/wp-content/uploads/2017/08/SPA-N%C2%BA-869-01-08-2017.pdf)
        * [December 8th, 2013](https://web.archive.org/web/20230129224800/https://www.bbc.com/mundo/ultimas_noticias/2013/11/131105_ultnot_venezuela_dia_lealtad_chavez)
        * [February 27th-28th, 2014](https://web.archive.org/web/20201111233441/https://www.economist.com/the-americas/2014/03/01/towards-the-brink)
        * [March 21st-23rd, 2016](https://web.archive.org/web/20250803061045/https://kpmg.com/ve/es/home/insights/2016/03/sintesis-legal--11-2016.html)
        * [All Fridays in April & May, 2016](https://web.archive.org/web/20260104055041/https://www.abs-cbn.com/world/v1/04/08/16/venezuela-decrees-fridays-a-holiday-to-ease-energy-crisis)
        * [April 18th, 2016](https://web.archive.org/web/20160422035408/https://www.arabnews.com/world/news/910856)
        * [April 10th-12th, 2017](https://web.archive.org/web/20260104080641/https://interjuris.com/dias-no-laborables-sector-publico/)
        * [August 20th, 2018](https://web.archive.org/web/20260104074101/https://www.afaca.com.ve/wp-content/uploads/2018/08/Gaceta-oficial-iva-a-16.pdf)
        * [February 28th-March 1st, 2019](https://web.archive.org/web/20260104073806/https://interjuris.com/non-working-days/)
        * [March 11th-12th, 2019](https://web.archive.org/web/20260104065639/https://www.aa.com.tr/en/energy/electricity/venezuela-extends-public-holiday-over-power-outage/23799)
        * [March 26th, 2019](https://web.archive.org/web/20260104065935/https://www.latimes.com/espanol/noticas-mas/articulo/2019-03-26/efe-3935909-15268311-20190326)
        * [October 19th-20th, 2025](https://web.archive.org/web/20260104073119/https://finanzasdigital.com/gaceta-oficial-6933-dias-no-laborables-19-20-octubre-2025/)
    """

    # National Holidays.
    national_holidays = tr("Fiesta Nacional")

    # Canonization of José Gregorio Hernández and Mother Carmen Rendiles.
    canonization = tr("Canonización de José Gregorio Hernández y la Madre Carmen Rendiles")

    special_public_holidays = {
        # 10th Anniversary of the Bolivarian Revolution.
        2009: (FEB, 2, tr("10.º aniversario de la Revolución Bolivariana")),
        2010: (
            (MAR, 29, national_holidays),
            (MAR, 30, national_holidays),
            (MAR, 31, national_holidays),
        ),
        2013: (
            DEC,
            8,
            # Day of Loyalty and Love for Supreme Commander Hugo Chávez and the Fatherland.
            tr("Día de la Lealtad y el Amor al Comandante Supremo Hugo Chávez y a la Patria"),
        ),
        2014: (
            (FEB, 27, national_holidays),
            (FEB, 28, national_holidays),
        ),
        2016: (
            (MAR, 21, national_holidays),
            (MAR, 22, national_holidays),
            (MAR, 23, national_holidays),
            (APR, 8, national_holidays),
            (APR, 15, national_holidays),
            (APR, 18, national_holidays),
            (APR, 22, national_holidays),
            (APR, 29, national_holidays),
            (MAY, 6, national_holidays),
            (MAY, 13, national_holidays),
            (MAY, 20, national_holidays),
            (MAY, 27, national_holidays),
        ),
        2017: (
            (APR, 10, national_holidays),
            (APR, 11, national_holidays),
            (APR, 12, national_holidays),
        ),
        2018: (AUG, 20, national_holidays),
        2019: (
            (FEB, 28, national_holidays),
            (MAR, 1, national_holidays),
            (MAR, 11, national_holidays),
            (MAR, 12, national_holidays),
            (MAR, 26, national_holidays),
        ),
        2025: (
            (OCT, 19, canonization),
            (OCT, 20, canonization),
        ),
    }
