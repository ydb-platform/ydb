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
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    TUE_TO_PREV_MON,
    WED_TO_NEXT_FRI,
    SAT_TO_PREV_FRI,
    SUN_TO_NEXT_MON,
    SUN_TO_NEXT_TUE,
    WED_THU_TO_NEXT_FRI,
)


class Ecuador(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Ecuador holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Ecuador>
        * [Código del Trabajo](https://web.archive.org/web/20250428092005/https://biblioteca.defensoria.gob.ec/bitstream/37000/3364/1/Código%20de%20Trabajo%20(04-11-2021).pdf)
        * [2018](https://web.archive.org/web/20181231193947/https://www.turismo.gob.ec/wp-content/uploads/2018/06/CALENDARIO-FERIADOS2018.pdf)
        * [2019](https://web.archive.org/web/20220707173738/https://www.turismo.gob.ec/wp-content/uploads/2019/01/FERIADOS-2019.pdf)
        * [2020-2021](https://web.archive.org/web/20210425014244/https://www.turismo.gob.ec/wp-content/uploads/2020/03/CALENDARIO-DE-FERIADOS.pdf)
        * [2022](https://web.archive.org/web/20220127234055/https://www.turismo.gob.ec/wp-content/uploads/2022/01/FERIADOS-NACIONALES_2022.pdf)
        * [2023-2025](https://web.archive.org/web/20250806025223/https://www.turismo.gob.ec/wp-content/uploads/2023/12/CALENDARIO-FERIADOS-2023-2025-06-12-2022-.pdf)
    """

    country = "EC"
    default_language = "es"
    # %s (observed).
    observed_label = tr("%s (observado)")
    supported_languages = ("en_US", "es", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        # Art. 1 of Law #0 from 20.12.2016
        # When holidays falls on Tuesday, the rest shall be transferred to
        # preceding Monday, and if they falls on Wednesday or Thursday,
        # the rest shall be transferred to Friday of the same week.
        # Exceptions to this provision are January 1, December 25 and
        # Shrove Tuesday.
        # When holidays falls on Saturday or Sunday, the rest shall be
        # transferred, respectively, to the preceding Friday or the
        # following Monday.
        kwargs.setdefault(
            "observed_rule",
            TUE_TO_PREV_MON + WED_THU_TO_NEXT_FRI + SAT_TO_PREV_FRI + SUN_TO_NEXT_MON,
        )
        kwargs.setdefault("observed_since", 2017)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = self.tr("Año Nuevo")
        self._add_observed(self._add_new_years_day(name), rule=SAT_TO_PREV_FRI + SUN_TO_NEXT_MON)
        self._add_observed(self._next_year_new_years_day, name=name, rule=SAT_TO_PREV_FRI)

        # Carnival.
        name = tr("Carnaval")
        self._add_carnival_monday(name)
        self._add_carnival_tuesday(name)

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # Labor Day.
        self._add_observed(self._add_labor_day(tr("Día del Trabajo")))

        # The Battle of Pichincha.
        self._add_observed(self._add_holiday_may_24(tr("Batalla de Pichincha")))

        # Declaration of Independence of Quito.
        self._add_observed(self._add_holiday_aug_10(tr("Primer Grito de Independencia")))

        # Independence of Guayaquil.
        self._add_observed(self._add_holiday_oct_9(tr("Independencia de Guayaquil")))

        self._add_observed(
            # All Souls' Day.
            self._add_all_souls_day(tr("Día de Difuntos")),
            # Not observed the next day.
            rule=TUE_TO_PREV_MON + WED_TO_NEXT_FRI + SAT_TO_PREV_FRI + SUN_TO_NEXT_TUE,
        )

        self._add_observed(
            # Independence of Cuenca.
            self._add_holiday_nov_3(tr("Independencia de Cuenca")),
            # Not observed the previous day.
            rule=SUN_TO_NEXT_MON,
        )

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Navidad")),
            rule=SAT_TO_PREV_FRI + SUN_TO_NEXT_MON,
        )


class EC(Ecuador):
    pass


class ECU(Ecuador):
    pass
