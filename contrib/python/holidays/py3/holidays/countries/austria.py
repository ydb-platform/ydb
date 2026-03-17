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


class Austria(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Austria holidays."""

    country = "AT"
    default_language = "de"
    subdivisions = (
        "1",  # Burgenland.
        "2",  # Kärnten.
        "3",  # Niederösterreich.
        "4",  # Oberösterreich.
        "5",  # Salzburg.
        "6",  # Steiermark.
        "7",  # Tirol.
        "8",  # Vorarlberg.
        "9",  # Wien.
    )
    subdivisions_aliases = {
        "Burgenland": "1",
        "Bgld": "1",
        "B": "1",
        "Kärnten": "2",
        "Ktn": "2",
        "K": "2",
        "Niederösterreich": "3",
        "NÖ": "3",
        "N": "3",
        "Oberösterreich": "4",
        "OÖ": "4",
        "O": "4",
        "Salzburg": "5",
        "Sbg": "5",
        "S": "5",
        "Steiermark": "6",
        "Stmk": "6",
        "St": "6",
        "Tirol": "7",
        "T": "7",
        "Vorarlberg": "8",
        "Vbg": "8",
        "V": "8",
        "Wien": "9",
        "W": "9",
    }
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

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Staatsfeiertag"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Christi Himmelfahrt"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # National Day.
        national_day = tr("Nationalfeiertag")
        if 1919 <= self._year <= 1934:
            self._add_holiday_nov_12(national_day)
        if self._year >= 1967:
            self._add_holiday_oct_26(national_day)

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        # Christmas Day.
        self._add_christmas_day(tr("Christtag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stefanitag"))

    def _populate_bank_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Heiliger Abend"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Silvester"))

    def _populate_subdiv_1_bank_holidays(self):
        # Saint Martin's Day.
        self._add_saint_martins_day(tr("Hl. Martin"))

    def _populate_subdiv_2_bank_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Hl. Josef"))

        # 1920 Carinthian plebiscite.
        self._add_holiday_oct_10(tr("Tag der Volksabstimmung"))

    def _populate_subdiv_3_bank_holidays(self):
        # Saint Leopold's Day.
        self._add_holiday_nov_15(tr("Hl. Leopold"))

    def _populate_subdiv_4_bank_holidays(self):
        if self._year >= 2004:
            # Saint Florian's Day.
            self._add_holiday_may_4(tr("Hl. Florian"))

    def _populate_subdiv_5_bank_holidays(self):
        # Saint Rupert's Day.
        self._add_holiday_sep_24(tr("Hl. Rupert"))

    def _populate_subdiv_6_bank_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Hl. Josef"))

    def _populate_subdiv_7_bank_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Hl. Josef"))

    def _populate_subdiv_8_bank_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Hl. Josef"))

    def _populate_subdiv_9_bank_holidays(self):
        # Saint Leopold's Day.
        self._add_holiday_nov_15(tr("Hl. Leopold"))


class AT(Austria):
    pass


class AUT(Austria):
    pass
