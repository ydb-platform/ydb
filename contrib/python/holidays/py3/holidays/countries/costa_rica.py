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
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    ALL_TO_NEAREST_MON_LATAM,
    ALL_TO_NEXT_SUN,
    WORKDAY_TO_NEXT_MON,
)


class CostaRica(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Costa Rica holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Costa_Rica>
        * <https://web.archive.org/web/20250328094837/http://www.pgrweb.go.cr/scij/avanzada_pgr.aspx>
        * Law #8442 from 19.04.2005
        * Law #8604 from 17.09.2007
        * Law #8753 from 25.07.2009
        * Law #8886 from 01.11.2010
        * Law #9803 from 19.05.2020
        * Law #10050 from 25.10.2021
    """

    country = "CR"
    default_language = "es"
    # %s (observed).
    observed_label = tr("%s (observado)")
    supported_categories = (OPTIONAL, PUBLIC)
    supported_languages = ("en_US", "es", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", ALL_TO_NEAREST_MON_LATAM)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Año Nuevo"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Juan Santamaría Day.
        apr_11 = self._add_holiday_apr_11(tr("Día de Juan Santamaría"))
        if 2006 <= self._year <= 2010:
            self._move_holiday(apr_11, rule=WORKDAY_TO_NEXT_MON)
        elif self._year in {2023, 2024}:
            self._move_holiday(apr_11)

        # International Labor Day.
        dt = self._add_labor_day(tr("Día Internacional del Trabajo"))
        if self._year == 2021:
            self._move_holiday(dt)

        # Annexation of the Party of Nicoya to Costa Rica.
        jul_25 = self._add_holiday_jul_25(tr("Anexión del Partido de Nicoya a Costa Rica"))
        if 2005 <= self._year <= 2008:
            self._move_holiday(jul_25, rule=WORKDAY_TO_NEXT_MON)
        elif 2020 <= self._year <= 2024:
            self._move_holiday(jul_25)

        # Mother's Day.
        dt = self._add_assumption_of_mary_day(tr("Día de la Madre"))
        if 2005 <= self._year <= 2007:
            self._move_holiday(dt, rule=WORKDAY_TO_NEXT_MON)
        elif self._year in {2020, 2023, 2024}:
            self._move_holiday(dt)

        # Independence Day.
        sep_15 = self._add_holiday_sep_15(tr("Día de la Independencia"))
        if self._year in {2020, 2021, 2022}:
            self._move_holiday(sep_15)

        if self._year <= 2019:
            self._move_holiday(
                # Cultures Day.
                self._add_columbus_day(tr("Día de las Culturas")),
                rule=WORKDAY_TO_NEXT_MON,
            )

        # Christmas Day.
        self._add_christmas_day(tr("Navidad"))

    def _populate_optional_holidays(self):
        # Feast of Our Lady of the Angels.
        self._add_holiday_aug_2(tr("Fiesta de Nuestra Señora de los Ángeles"))

        if self._year >= 2021:
            aug_31 = self._add_holiday_aug_31(
                # Day of the Black Person and Afro-Costa Rican Culture.
                tr("Día de la Persona Negra y la Cultura Afrocostarricense")
            )
            if self._year in {2021, 2022, 2023}:
                # Move to next Sunday.
                self._move_holiday(aug_31, rule=ALL_TO_NEXT_SUN)

        if self._year >= 2020:
            # Army Abolition Day.
            dec_1 = self._add_holiday_dec_1(tr("Día de la Abolición del Ejército"))
            if self._year in {2020, 2021, 2022}:
                self._move_holiday(dec_1)


class CR(CostaRica):
    pass


class CRI(CostaRica):
    pass
