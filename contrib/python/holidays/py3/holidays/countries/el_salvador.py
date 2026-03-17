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
from holidays.holiday_base import HolidayBase


class ElSalvador(HolidayBase, ChristianHolidays, InternationalHolidays):
    """El Salvador holidays.

    References:
        * [Labor Code 1972](https://web.archive.org/web/20240918054537/http://www.transparencia.gob.sv/institutions/gd-usulutan/documents/192280/download)
        * <https://web.archive.org/web/20250308121509/https://www.timeanddate.com/holidays/el-salvador/>
        * <https://web.archive.org/web/20250122083353/https://www.officeholidays.com/countries/el-salvador>
    """

    country = "SV"
    default_language = "es"
    # Labor Code 1972.
    start_year = 1973
    subdivisions = (
        "AH",  # Ahuachapán.
        "CA",  # Cabañas.
        "CH",  # Chalatenango.
        "CU",  # Cuscatlán.
        "LI",  # La Libertad.
        "MO",  # Morazán.
        "PA",  # La Paz.
        "SA",  # Santa Ana.
        "SM",  # San Miguel.
        "SO",  # Sonsonate.
        "SS",  # San Salvador.
        "SV",  # San Vicente.
        "UN",  # La Unión.
        "US",  # Usulután.
    )
    subdivisions_aliases = {
        "Ahuachapán": "AH",
        "Cabañas": "CA",
        "Chalatenango": "CH",
        "Cuscatlán": "CU",
        "La Libertad": "LI",
        "Morazán": "MO",
        "La Paz": "PA",
        "Santa Ana": "SA",
        "San Miguel": "SM",
        "Sonsonate": "SO",
        "San Salvador": "SS",
        "San Vicente": "SV",
        "La Unión": "UN",
        "Usulután": "US",
    }
    supported_languages = ("en_US", "es", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Holy Saturday.
        self._add_holy_saturday(tr("Sábado Santo"))

        # Labor Day.
        self._add_labor_day(tr("Día del Trabajo"))

        # Legislative Decree #399 from Apr 14, 2016.
        if self._year >= 2016:
            # Mother's Day.
            self._add_holiday_may_10(tr("Día de la Madre"))

        # Legislative Decree #208 from Jun 17, 2012.
        if self._year >= 2013:
            # Father's Day.
            self._add_holiday_jun_17(tr("Día del Padre"))

        # Celebrations of San Salvador.
        self._add_holiday_aug_6(tr("Celebración del Divino Salvador del Mundo"))

        # Independence Day.
        self._add_holiday_sep_15(tr("Día de la Independencia"))

        # All Souls' Day.
        self._add_all_souls_day(tr("Día de los Difuntos"))

        # Christmas Day.
        self._add_christmas_day(tr("Navidad"))

    def _populate_subdiv_ss_public_holidays(self):
        # Feast of San Salvador.
        name = tr("Fiesta de San Salvador")
        self._add_holiday_aug_3(name)
        self._add_holiday_aug_5(name)


class SV(ElSalvador):
    pass


class SLV(ElSalvador):
    pass
