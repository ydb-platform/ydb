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

from holidays.calendars.julian_revised import JULIAN_REVISED_CALENDAR
from holidays.constants import BANK, OPTIONAL, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Cyprus(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Cyprus holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Cyprus>
        * <https://web.archive.org/web/20241127121516/https://www.centralbank.cy/en//the-bank/working-hours-bank-holidays>
    """

    country = "CY"
    default_language = "el"
    supported_categories = (BANK, OPTIONAL, PUBLIC)
    supported_languages = ("el", "en_CY", "en_US", "uk")
    start_year = 1961

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_REVISED_CALENDAR)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Πρωτοχρονιά"))

        # Epiphany.
        self._add_epiphany_day(tr("Ημέρα των Θεοφανίων"))

        # Green Monday.
        self._add_ash_monday(tr("Καθαρά Δευτέρα"))

        # Greek Independence Day.
        self._add_holiday_mar_25(tr("Ημέρα της Ελληνικής Ανεξαρτησίας"))

        # Cyprus National Day.
        self._add_holiday_apr_1(tr("Εθνική Ημέρα Κύπρου"))

        # Good Friday.
        self._add_good_friday(tr("Μεγάλη Παρασκευή"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Κυριακή του Πάσχα"))

        # Easter Monday.
        self._add_easter_monday(tr("Δευτέρα της Διακαινησίμου"))

        # Labor Day.
        self._add_labor_day(tr("Πρωτομαγιά"))

        # Whit Monday.
        self._add_whit_monday(tr("Δευτέρα του Αγίου Πνεύματος"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Κοίμηση της Θεοτόκου"))

        if self._year >= 1979:
            # Cyprus Independence Day.
            self._add_holiday_oct_1(tr("Ημέρα της Κυπριακής Ανεξαρτησίας"))

        # Ochi Day.
        self._add_holiday_oct_28(tr("Ημέρα του Όχι"))

        # Christmas Day.
        self._add_christmas_day(tr("Χριστούγεννα"))

        # Day After Christmas.
        self._add_christmas_day_two(tr("Επομένη Χριστουγέννων"))

    def _populate_bank_holidays(self):
        # Easter Tuesday.
        self._add_easter_tuesday(tr("Τρίτη της Διακαινησίμου"))

    def _populate_optional_holidays(self):
        # Holy Saturday.
        self._add_holy_saturday(tr("Μεγάλο Σάββατο"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Παραμονή Χριστουγέννων"))


class CY(Cyprus):
    pass


class CYP(Cyprus):
    pass
