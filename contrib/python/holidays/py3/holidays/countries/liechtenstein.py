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


class Liechtenstein(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Liechtenstein holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Liechtenstein>
        * <https://web.archive.org/web/20250122013118/https://llb.li/en/contact/bank-holidays>
    """

    country = "LI"
    default_language = "de"
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("de", "en_US", "uk")

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Neujahr"))

        # Epiphany.
        self._add_epiphany_day(tr("Heilige Drei Könige"))

        # Candlemas.
        self._add_candlemas(tr("Mariä Lichtmess"))

        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Josefstag"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Ostersonntag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Auffahrt"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Pfingstsonntag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # National Day.
        self._add_assumption_of_mary_day(tr("Staatsfeiertag"))

        # Nativity of Mary.
        self._add_nativity_of_mary_day(tr("Mariä Geburt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        # Christmas Day.
        self._add_christmas_day(tr("Weihnachten"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_bank_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Shrove Tuesday.
        self._add_carnival_tuesday(tr("Fasnachtsdienstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Heiligabend"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Silvester"))


class LI(Liechtenstein):
    pass


class LIE(Liechtenstein):
    pass
