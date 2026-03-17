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


class Croatia(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Croatia holidays.

    References:
        * <https://web.archive.org/web/20250206084954/https://narodne-novine.nn.hr/clanci/sluzbeni/2019_11_110_2212.html>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Croatia>
        * <https://hr.wikipedia.org/wiki/Blagdani_i_spomendani_u_Hrvatskoj>

    Updated with act 022-03 / 19-01 / 219 of 14 November 2019
    """

    country = "HR"
    default_language = "hr"
    supported_languages = ("en_US", "hr", "uk")
    start_year = 1992

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nova godina"))

        if self._year != 2002:
            # Epiphany.
            self._add_epiphany_day(tr("Bogojavljenje ili Sveta tri kralja"))

        if self._year >= 2009:
            # Easter Sunday.
            self._add_easter_sunday(tr("Uskrs"))

        # Easter Monday.
        self._add_easter_monday(tr("Uskrsni ponedjeljak"))

        if self._year >= 2002:
            # Corpus Christi.
            self._add_corpus_christi_day(tr("Tijelovo"))

        # Labor Day.
        self._add_labor_day(tr("Praznik rada"))

        if self._year >= 1996:
            # Statehood Day.
            name = tr("Dan državnosti")
            if 2002 <= self._year <= 2019:
                self._add_holiday_jun_25(name)
            else:
                self._add_holiday_may_30(name)

        # Anti-Fascist Struggle Day.
        self._add_holiday_jun_22(tr("Dan antifašističke borbe"))

        self._add_holiday_aug_5(
            # Victory and Homeland Thanksgiving Day and Croatian Veterans Day.
            tr("Dan pobjede i domovinske zahvalnosti i Dan hrvatskih branitelja")
            if self._year >= 2008
            # Victory and Homeland Thanksgiving Day.
            else tr("Dan pobjede i domovinske zahvalnosti")
        )

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Velika Gospa"))

        if 2002 <= self._year <= 2019:
            # Independence Day.
            self._add_holiday_oct_8(tr("Dan neovisnosti"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Svi sveti"))

        if self._year >= 2020:
            self._add_holiday_nov_18(
                # Remembrance Day.
                tr(
                    "Dan sjećanja na žrtve Domovinskog rata i "
                    "Dan sjećanja na žrtvu Vukovara i Škabrnje"
                )
            )

        # Christmas Day.
        self._add_christmas_day(tr("Božić"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Sveti Stjepan"))


class HR(Croatia):
    pass


class HRV(Croatia):
    pass
