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
from holidays.observed_holiday_base import ObservedHolidayBase, ALL_TO_NEXT_MON


class Colombia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Colombia holidays.

    References:
        * [Ley 35 de 1939 (DEC 4)](https://web.archive.org/web/20250429071624/https://www.funcionpublica.gov.co/eva/gestornormativo/norma_pdf.php?i=86145)
        * [Decreto 2663 de 1950 (AUG 5)](https://web.archive.org/web/20241113003142/https://www.suin-juriscol.gov.co/viewDocument.asp?id=1874133)
        * [Decreto 3743 de 1950 (DEC 20)](https://web.archive.org/web/20240725032513/http://suin-juriscol.gov.co/viewDocument.asp?id=1535683)
        * [Ley 51 de 1983 (DEC 6)](https://web.archive.org/web/20250423030608/https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=4954)

    A few links below to calendars from the 1980s to demonstrate this law change.
    In 1984 some calendars still use the old rules, presumably because they were printed
    prior to the declaration of law change:
        * [1981](https://web.archive.org/web/20250427173739/https://cloud10.todocoleccion.online/calendarios-antiguos/tc/2018/07/02/19/126899607_96874586.jpg)
        * [1982](https://web.archive.org/web/20250427173704/https://cloud10.todocoleccion.online/calendarios-antiguos/tc/2016/08/19/12/58620712_34642074.jpg)
        * [1984](https://web.archive.org/web/20250427173707/https://cloud10.todocoleccion.online/calendarios-antiguos/tc/2017/07/12/15/92811790_62818054.jpg)
    """

    country = "CO"
    default_language = "es"
    # %s (observed).
    observed_label = tr("%s (observado)")
    supported_languages = ("en_US", "es", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", ALL_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 1984)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        if self._year >= 1951:
            # Epiphany.
            self._move_holiday(self._add_epiphany_day(tr("Día de los Reyes Magos")))

            # Saint Joseph's Day.
            self._move_holiday(self._add_saint_josephs_day(tr("Día de San José")))

            # Maundy Thursday.
            self._add_holy_thursday(tr("Jueves Santo"))

            # Good Friday.
            self._add_good_friday(tr("Viernes Santo"))

            # Ascension Day.
            self._move_holiday(self._add_ascension_thursday(tr("Ascensión del señor")))

            # Corpus Christi.
            self._move_holiday(self._add_corpus_christi_day(tr("Corpus Christi")))

        # Labor Day.
        self._add_labor_day(tr("Día del Trabajo"))

        if self._year >= 1984:
            self._move_holiday(
                # Sacred Heart.
                self._add_holiday_68_days_past_easter(tr("Sagrado Corazón"))
            )

        if self._year >= 1951:
            # Saint Peter and Saint Paul's Day.
            self._move_holiday(self._add_saints_peter_and_paul_day(tr("San Pedro y San Pablo")))

        # Independence Day.
        self._add_holiday_jul_20(tr("Día de la Independencia"))

        # Battle of Boyaca.
        self._add_holiday_aug_7(tr("Batalla de Boyacá"))

        if self._year >= 1951:
            # Assumption Day.
            self._move_holiday(self._add_assumption_of_mary_day(tr("La Asunción")))

        # Columbus Day.
        self._move_holiday(self._add_columbus_day(tr("Día de la Raza")))

        if self._year >= 1951:
            # All Saints' Day.
            self._move_holiday(self._add_all_saints_day(tr("Día de Todos los Santos")))

        self._move_holiday(
            # Independence of Cartagena.
            self._add_holiday_nov_11(tr("Independencia de Cartagena"))
        )

        if self._year >= 1951:
            # Immaculate Conception.
            self._add_immaculate_conception_day(tr("La Inmaculada Concepción"))

        # Christmas Day.
        self._add_christmas_day(tr("Navidad"))


class CO(Colombia):
    pass


class COL(Colombia):
    pass
