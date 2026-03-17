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

from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, DEC, SUN
from holidays.constants import GOVERNMENT, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Paraguay(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Paraguay holidays.

    References:
        * [Ley 8/1990](https://web.archive.org/web/20250427173904/https://www.bacn.gov.py/leyes-paraguayas/2358/ley-n-8--por-la-cual-se-determinan-los-feriados-de-la-republica)
        * [Ley 715/1995](https://web.archive.org/web/20250427174040/https://www.bacn.gov.py/leyes-paraguayas/792/ley-n-715--amplia-la-ley-no-0890-)
        * [Ley 1.601/2000](https://web.archive.org/web/20250427173847/https://www.bacn.gov.py/leyes-paraguayas/1677/ley-n-1601--modifica-el-articulo-1-de-la-ley-no-71595-que-amplia-la-ley-n-0890-por-la-cual-se-determinan-los-feriados-de-la-republica)
        * [Ley 1.723/2001](https://web.archive.org/web/20250427173914/https://www.bacn.gov.py/leyes-paraguayas/634/ley-n-1723--autoriza-al-poder-ejecutivo-a-trasladar-los-feriados-nacionales-al-dia-lunes)
        * [Ley 4.531/2011](https://web.archive.org/web/20250420210317/https://www.bacn.gov.py/leyes-paraguayas/3831/ley-n-4531-restablece-el-dia-14-de-mayo-de-cada-ano-como-feriado-nacional)
        * <https://es.wikipedia.org/wiki/Anexo:Días_festivos_en_Paraguay>
        * <https://web.archive.org/web/20131212214628/https://www.abc.com.py/nacionales/feriados-de-2013-se-trasladaran-al-dia-lunes-494705.html>
        * <https://web.archive.org/web/20241207131557/https://www.ultimahora.com/establecen-traslados-feriados-el-2017-n1042098>
        * <https://web.archive.org/web/20250913135012/https://foco.lanacion.com.py/2025/01/01/feriados-2025-paraguay-tendra-6-fines-de-semana-largos/>
    """

    country = "PY"
    default_language = "es"
    supported_categories = (GOVERNMENT, PUBLIC)
    supported_languages = ("en_US", "es", "uk")
    # Ley 8/1990.
    start_year = 1991
    weekend = {SUN}

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, ParaguayStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        dates_obs = {
            2013: (MAR, 4),
            2016: (FEB, 29),
            2017: (FEB, 27),
            2018: (FEB, 26),
            2019: (MAR, 4),
            2022: (FEB, 28),
            2023: (FEB, 27),
            2025: (MAR, 3),
        }
        self._add_holiday(
            # Patriots Day.
            tr("Día de los Héroes de la Patria"),
            dates_obs.get(self._year, (MAR, 1)),
        )

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Domingo de Resurrección"))

        # Workers' Day.
        self._add_labor_day(tr("Día de los Trabajadores"))

        if self._year >= 2012:
            # National Holiday.
            self._add_holiday_may_14(tr("Feriado Nacional"))

        # Independence Day.
        self._add_holiday_may_15(tr("Día de la Independencia Nacional"))

        dates_obs = {
            2013: (JUN, 10),
            2014: (JUN, 16),
            2018: (JUN, 11),
            2019: (JUN, 17),
            2024: (JUN, 10),
            2025: (JUN, 16),
        }
        # Chaco Armistice Day.
        self._add_holiday(tr("Día de la Paz del Chaco"), dates_obs.get(self._year, (JUN, 12)))

        # Asuncion Foundation's Day.
        name = tr("Día de la Fundación de Asunción")
        # Decreto N° 6292.
        if self._year == 2017:
            self._add_holiday_aug_14(name)
        else:
            self._add_holiday_aug_15(name)

        # Ley 715/1995, 1.601/2000.
        if self._year >= 1995:
            dates_obs = {
                2015: (SEP, 28),
                2016: (OCT, 3),
                2017: (OCT, 2),
                2021: (SEP, 27),
                2022: (OCT, 3),
                2024: (SEP, 30),
            }
            self._add_holiday(
                # Boqueron Battle Day.
                tr("Día de la Batalla de Boquerón"),
                dates_obs.get(self._year, (SEP, 29)),
            )

        # Caacupe Virgin Day.
        self._add_holiday_dec_8(tr("Día de la Virgen de Caacupé"))

        # Christmas Day.
        self._add_christmas_day(tr("Día de la Navidad"))


class PY(Paraguay):
    pass


class PRY(Paraguay):
    pass


class ParaguayStaticHolidays:
    """Paraguay special holidays.

    References:
        * <https://web.archive.org/web/20250917075450/https://www.pgr.gov.py/wp-content/uploads/2013/12/DECRETO-991-ASUETO-EL-24-Y-EL-31-DE-DICIEMBRE.pdf>
        * <https://web.archive.org/web/20250917074759/https://www.ultimahora.com/ejecutivo-declara-asueto-el-23-y-30-diciembre-sector-publico-n3040041>
        * <https://web.archive.org/web/20231003173505/https://www.lanacion.com.py/politica/2023/03/24/decretan-asueto-de-funcionarios-publicos-el-miercoles-5-de-abril-por-semana-santa/>
        * <https://web.archive.org/web/20250917042124/https://informacionpublica.paraguay.gov.py/public/1046579-RESPUESTAAIPN79397pdf-RESPUESTAAIPN79397.pdf>
        * <https://web.archive.org/web/20250416092842/https://www.radionacional.gov.py/2024/03/22/ejecutivo-declara-asueto-el-miercoles-27-de-marzo-para-funcionarios-publicos/>
        * <https://web.archive.org/web/20241225185947/https://www.abc.com.py/politica/2024/12/20/declaran-asueto-para-funcionarios-publicos-el-24-y-31-de-diciembre/>
        * <https://web.archive.org/web/20250917075032/https://portal.ips.gov.py/sistemas/ipsportal/archivos/archivos/1744391513.pdf>
    """

    # Public holiday.
    public_holiday = tr("Asueto adicional")
    # Public sector holiday.
    public_sector_holiday = tr("Asueto de la Administración Pública")

    special_public_holidays = {
        # Public holiday for business purposes, in view of the recently increased risk
        # of Dengue fever.
        2007: (JAN, 29, public_holiday),
        # Public sector holiday to celebrate Paraguay's football team's qualification
        # for the 2010 World Cup.
        2009: (SEP, 10, public_holiday),
        # Public holiday to coincide with the Paraguay-Italy game of the current World Cup.
        2010: (JUN, 14, public_holiday),
        2011: (
            # Public holiday to coincide with the current anti-Dengue drive.
            (APR, 19, public_holiday),
            # Public holidays to commemorate the Bicentennial of Paraguay's independence.
            (MAY, 14, public_holiday),
            (MAY, 16, public_holiday),
        ),
        # Date of the inauguration of President-elect Horacio Cartes.
        2013: (AUG, 14, public_holiday),
        2015: (
            # Public holidays in Paraguay on account of the upcoming visit of Pope Francis
            # in Paraguay.
            (JUL, 10, public_holiday),
            (JUL, 11, public_holiday),
        ),
    }

    special_government_holidays = {
        2010: (
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        2011: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (APR, 20, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 23, public_sector_holiday),
            (DEC, 30, public_sector_holiday),
        ),
        2012: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (APR, 4, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        2013: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (MAR, 27, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        2014: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (APR, 16, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        2015: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (APR, 1, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        # Public sector holiday to let civil servants begin their Holy Week earlier.
        2016: (MAR, 23, public_sector_holiday),
        # Public sector holiday to let civil servants begin their Holy Week earlier.
        2017: (MAR, 28, public_sector_holiday),
        2018: (
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        2019: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (APR, 17, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        # Public sector holiday to let civil servants begin their Holy Week earlier.
        2020: (APR, 8, public_sector_holiday),
        2021: (
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        2022: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (APR, 13, public_sector_holiday),
            # Public sector holiday due to the annual May 1st public holiday falling on a Sunday.
            (MAY, 2, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 23, public_sector_holiday),
            (DEC, 30, public_sector_holiday),
        ),
        2023: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (APR, 5, public_sector_holiday),
            # Public sector holiday from resolution no. 1045.
            (DEC, 7, public_sector_holiday),
        ),
        2024: (
            # Public sector holiday to let civil servants begin their Holy Week earlier.
            (MAR, 27, public_sector_holiday),
            # 2 year-end public sector holidays.
            (DEC, 24, public_sector_holiday),
            (DEC, 31, public_sector_holiday),
        ),
        # Public sector holiday to let civil servants begin their Holy Week earlier.
        2025: (APR, 16, public_sector_holiday),
    }
