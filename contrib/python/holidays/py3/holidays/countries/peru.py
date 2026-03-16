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


class Peru(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Peru holidays.

    References:
        * <https://web.archive.org/web/20250414165243/https://www.gob.pe/feriados/>
        * <https://es.wikipedia.org/wiki/Anexo:Días_feriados_en_el_Perú>
        * [Ley N° 31788](https://web.archive.org/web/20250716164223/https://img.lpderecho.pe/wp-content/uploads/2023/06/Ley-31788-LPDerecho.pdf)
        * [Ley N° 31822](https://web.archive.org/web/20250716164455/https://img.lpderecho.pe/wp-content/uploads/2023/07/Ley-31822-LPDerecho.pdf)
    """

    country = "PE"
    default_language = "es"
    supported_languages = ("en_US", "es", "uk")

    def __init__(self, *args, **kwargs) -> None:
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

        # Easter Sunday.
        self._add_easter_sunday(tr("Domingo de Resurrección"))

        # Labor Day.
        self._add_labor_day(tr("Día del Trabajo"))

        # Added via Ley N° 31788 on June 15th, 2023.
        if self._year >= 2024:
            # Battle of Arica and Flag Day.
            self._add_holiday_jun_7(tr("Batalla de Arica y Día de la Bandera"))

        # Saint Peter and Saint Paul's Day.
        self._add_saints_peter_and_paul_day(tr("San Pedro y San Pablo"))

        # Added via Ley N° 31822 on July 8th, 2023.
        if self._year >= 2023:
            # Peruvian Air Force Day.
            self._add_holiday_jul_23(tr("Día de la Fuerza Aérea del Perú"))

        # Independence Day.
        self._add_holiday_jul_28(tr("Día de la Independencia"))

        # Great Military Parade Day.
        self._add_holiday_jul_29(tr("Día de la Gran Parada Militar"))

        if self._year >= 2022:
            # Battle of Junín.
            self._add_holiday_aug_6(tr("Batalla de Junín"))

        # Rose of Lima Day.
        self._add_holiday_aug_30(tr("Santa Rosa de Lima"))

        # Battle of Angamos.
        self._add_holiday_oct_8(tr("Combate de Angamos"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Todos Los Santos"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Inmaculada Concepción"))

        if self._year >= 2022:
            # Battle of Ayacucho.
            self._add_holiday_dec_9(tr("Batalla de Ayacucho"))

        # Christmas Day.
        self._add_christmas_day(tr("Navidad del Señor"))


class PE(Peru):
    pass


class PER(Peru):
    pass
