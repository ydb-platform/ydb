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
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Cuba(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Cuba holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Cuba>
        * [1984 (DEC 28)](https://web.archive.org/web/20240726190205/https://files.sld.cu/prevemi/files/2013/03/ley_49_codigo_trabajo_1984.pdf)
        * [2007 (NOV 19)](https://web.archive.org/web/20220705173115/https://www.gacetaoficial.gob.cu/sites/default/files/go_x_053_2007.pdf)
        * [2013 (DEC 20)](https://web.archive.org/web/20241125112840/http://www.gacetaoficial.gob.cu/es/ley-no-116-codigo-de-trabajo/)

    Certain holidays details:
        * Good Friday:
            Granted temporarily in 2012 and 2013, permanently granted in 2013 decree for 2014
            and onwards:
                * <https://web.archive.org/web/20250413192145/https://www.cnn.com/2012/03/31/world/americas/cuba-good-friday/index.html>
                * <https://web.archive.org/web/20241005041855/https://www.catholicnewsagency.com/news/29455/cuban-government-makes-good-friday-official-holiday>

        * Christmas Day:
            In 1969, Christmas was cancelled for the sugar harvest but then was cancelled for good:
                * <https://web.archive.org/web/20250413192307/https://time.com/vault/issue/1969-11-07/page/44/>
            In 1997, Christmas was temporarily back for the Pope's visit:
                * <https://web.archive.org/web/20250427131226/https://www.cnn.com/WORLD/9712/15/castro.christmas/>
            In 1998, Christmas returns for good:
                * <https://web.archive.org/web/20250413185514/https://www.independent.co.uk/news/cuba-ends-its-30year-ban-on-christmas-1193525.html>
                * <https://web.archive.org/web/20220508094546/https://www.ilo.org/dyn/travail/docs/1320/DECRETO-LEY%20No.%20189-1998.pdf>

    For holidays that can be moved to a Monday if they fall on a Sunday, between 1984
    and 2013, the State Committee of Work and Social Security would determine if they
    would be moved to the Monday, or if they would stay on the Sunday, presumably
    depending on quotas. After 2013, they always move to Monday. I could not find any
    records of this, so I implemented this making it always go to the next Monday.
    """

    country = "CU"
    default_language = "es"
    # %s (observed).
    observed_label = tr("%s (observado)")
    supported_languages = ("en_US", "es", "uk")
    # This calendar only works from 1959 onwards.
    start_year = 1959

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Liberation Day.
        jan_1 = self._add_holiday_jan_1(tr("Triunfo de la Revolución"))
        if self._year <= 2013:
            self._add_observed(jan_1)

        if self._year >= 2008:
            #  Victory Day.
            self._add_holiday_jan_2(tr("Día de la Victoria"))

        if self._year >= 2012:
            # Good Friday.
            self._add_good_friday(tr("Viernes Santo"))

        # International Workers' Day.
        self._add_observed(self._add_labor_day(tr("Día Internacional de los Trabajadores")))

        # Commemoration of the Assault of the Moncada garrison.
        name = tr("Conmemoración del asalto a Moncada")
        self._add_holiday_jul_25(name)
        self._add_holiday_jul_27(name)

        # Day of the National Rebellion.
        self._add_holiday_jul_26(tr("Día de la Rebeldía Nacional"))

        # Independence Day.
        self._add_observed(self._add_holiday_oct_10(tr("Inicio de las Guerras de Independencia")))

        if self._year <= 1968 or self._year >= 1997:
            # Christmas Day.
            self._add_christmas_day(tr("Día de Navidad"))

        if self._year >= 2007:
            # New Year's Eve.
            self._add_new_years_eve(tr("Fiesta de Fin de Año"))


class CU(Cuba):
    pass


class CUB(Cuba):
    pass
