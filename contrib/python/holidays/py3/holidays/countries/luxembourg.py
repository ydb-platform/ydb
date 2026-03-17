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

from holidays.constants import BANK, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Luxembourg(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Luxembourg holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Luxembourg>
        * <https://web.archive.org/web/20250625132138/https://www.bcl.lu/en/About/Opening-days/index.html>
    """

    country = "LU"
    default_language = "lb"
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("de", "en_US", "fr", "lb", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Neijoerschdag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ouschterméindeg"))

        # Labor Day.
        self._add_labor_day(tr("Dag vun der Aarbecht"))

        if self._year >= 2019:
            # Europe Day.
            self._add_europe_day(tr("Europadag"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Christi Himmelfaart"))

        # Whit Monday.
        self._add_whit_monday(tr("Péngschtméindeg"))

        # National Day.
        self._add_holiday_jun_23(tr("Nationalfeierdag"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Léiffrawëschdag"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerhellgen"))

        # Christmas Day.
        self._add_christmas_day(tr("Chrëschtdag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stiefesdag"))

    def _populate_bank_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreideg"))

        # Christmas Eve (afternoon).
        self._add_christmas_eve(tr("Hellegowend (nomëtteg)"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Silvester"))


class LU(Luxembourg):
    pass


class LUX(Luxembourg):
    pass
