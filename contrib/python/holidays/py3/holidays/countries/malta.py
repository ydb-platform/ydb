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


class Malta(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Malta holidays.

    References:
        * <https://web.archive.org/web/20240406224540/https://www.gov.mt/en/About%20Malta/Pages/Public%20Holidays.aspx>
        * [Att 10 tal-1980 (Oldest Maltese Holidays Law available online in full)](https://web.archive.org/web/20250427184411/https://legislation.mt/eli/act/1980/10/mlt)
        * [A.L. 40 tal-1987 (Additional Holidays added)](https://web.archive.org/web/20250427184605/https://legislation.mt/eli/ln/1987/8/mlt)
        * [Att 8 tal-1989 (Additional Holidays added)](https://web.archive.org/web/20250427184429/https://legislation.mt/eli/act/1989/8)
        * [Att 2 tal-2005 (If fall on weekends then not observed in terms of vacation leave)](https://web.archive.org/web/20250427184434/https://legislation.mt/eli/act/2005/2/eng)
        * [Att 4 tal-2021 (Revert Act II of 2005 changes for vacation leave)](https://web.archive.org/web/20240716035452/https://legislation.mt/eli/cap/252/20210212/mlt)
    """

    country = "MT"
    default_language = "mt"
    supported_languages = ("en_US", "mt")
    # Earliest available source is 1980.
    start_year = 1980

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # L-Ewwel tas-Sena
        # Status: In-Use.

        # New Year's Day.
        self._add_new_years_day(tr("L-Ewwel tas-Sena"))

        # Il-Festa tan-Nawfraġju ta' San Pawl
        # Status: In-Use.
        # Started in 1987 via Act LX of 1987.

        if self._year >= 1987:
            # Feast of Saint Paul's Shipwreck.
            self._add_holiday_feb_10(tr("Il-Festa tan-Nawfraġju ta' San Pawl"))

        # Il-Festa ta' San Ġużepp
        # Status: In-Use.
        # Started in 1987 via Act LX of 1987.

        if self._year >= 1987:
            # Feast of Saint Joseph.
            self._add_saint_josephs_day(tr("Il-Festa ta' San Ġużepp"))

        # Jum il-Ħelsien
        # Status: In-Use.
        # Started in 1980 Act X of 1980.
        # Not presented in 1987-1988

        if self._year <= 1986 or self._year >= 1989:
            # Freedom Day.
            self._add_holiday_mar_31(tr("Jum il-Ħelsien"))

        # Il-Ġimgħa l-Kbira
        # Status: In-Use.

        # Good Friday.
        self._add_good_friday(tr("Il-Ġimgħa l-Kbira"))

        # Jum il-Ħaddiem
        # Status: In-Use.

        # Worker's Day.
        self._add_labor_day(tr("Jum il-Ħaddiem"))

        # Sette Giugno
        # Status: In-Use.
        # Start in 1989 via Act VIII of 1989.

        if self._year >= 1989:
            # Sette Giugno.
            self._add_holiday_jun_7(tr("Sette Giugno"))

        # Il-Festa ta' San Pietru u San Pawl
        # Status: In-Use.
        # Started in 1987 via Act LX of 1987.

        if self._year >= 1987:
            # Feast of Saint Peter and Saint Paul.
            self._add_saints_peter_and_paul_day(tr("Il-Festa ta' San Pietru u San Pawl"))

        # Il-Festa ta' Santa Marija
        # Status: In-Use.

        # Feast of the Assumption.
        self._add_assumption_of_mary_day(tr("Il-Festa ta' Santa Marija"))

        # Jum il-Vitorja
        # Status: In-Use.
        # Started in 1987 via Act LX of 1987.
        # While this concides with Nativity Of Mary Day, the two are considered separate.

        if self._year >= 1987:
            # Feast of Our Lady of Victories.
            self._add_nativity_of_mary_day(tr("Jum il-Vitorja"))

        # Jum l-Indipendenza
        # Status: In-Use.
        # Started in 1987 via Act LX of 1987.

        if self._year >= 1987:
            # Independence Day.
            self._add_holiday_sep_21(tr("Jum l-Indipendenza"))

        # Il-Festa tal-Immakulata Kunċizzjoni
        # Status: In-Use.
        # Started in 1987 via Act LX of 1987.

        if self._year >= 1987:
            # Feast of the Immaculate Conception
            self._add_immaculate_conception_day(tr("Il-Festa tal-Immakulata Kunċizzjoni"))

        # Jum ir-Repubblika
        # Status: In-Use.

        # Republic Day.
        self._add_holiday_dec_13(tr("Jum ir-Repubblika"))

        # Il-Milied
        # Status: In-Use.

        # Christmas Day.
        self._add_christmas_day(tr("Il-Milied"))


class MT(Malta):
    pass


class MLT(Malta):
    pass
