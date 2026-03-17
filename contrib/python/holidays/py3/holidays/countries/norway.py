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

from holidays.calendars.gregorian import _get_all_sundays
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Norway(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Norway holidays.

    References:
        * <https://web.archive.org/web/20250102052441/https://lovdata.no/dokument/NL/lov/1947-04-26-1>
        * <https://no.wikipedia.org/wiki/Helligdager_i_Norge>
        * <https://web.archive.org/web/20250416120557/https://www.timeanddate.no/merkedag/norge>

    Note that holidays falling on a sunday is "lost", it will not be moved
    to another day to make up for the collision.

    In Norway, ALL sundays are considered a holiday (https://web.archive.org/web/20250202193959/https://snl.no/helligdag).
    Initialize this class with `include_sundays=False` to not include sundays as a holiday.
    """

    country = "NO"
    default_language = "no"
    subdivisions: tuple[str, ...] = (
        "03",  # Oslo.
        "11",  # Rogaland.
        "15",  # Møre og Romsdal.
        "18",  # Nordland.
        "21",  # Svalbard.
        "22",  # Jan Mayen.
        "31",  # Østfold.
        "32",  # Akershus.
        "33",  # Buskerud.
        "34",  # Innlandet.
        "39",  # Vestfold.
        "40",  # Telemark.
        "42",  # Agder.
        "46",  # Vestland.
        "50",  # Trøndelag (Trööndelage).
        "55",  # Troms (Romssa, Tromssan).
        "56",  # Finnmark (Finnmárkku, Finmarkun).
    )
    subdivisions_aliases = {
        "Oslo": "03",
        "Rogaland": "11",
        "Møre og Romsdal": "15",
        "Nordland": "18",
        "Svalbard": "21",
        "Jan Mayen": "22",
        "Østfold": "31",
        "Akershus": "32",
        "Buskerud": "33",
        "Innlandet": "34",
        "Vestfold": "39",
        "Telemark": "40",
        "Agder": "42",
        "Vestland": "46",
        "Trööndelage": "50",
        "Trøndelag": "50",
        "Romssa": "55",
        "Troms": "55",
        "Tromssan": "55",
        "Finmarkun": "56",
        "Finnmark": "56",
        "Finnmárkku": "56",
    }
    supported_languages = ("en_US", "no", "th", "uk")
    _deprecated_subdivisions = (
        "30",
        "38",
        "54",
    )

    def __init__(self, *args, include_sundays: bool = False, **kwargs):
        """
        Args:
            include_sundays:
                Whether to consider sundays as a holiday (which they are in Norway)
        """
        self.include_sundays = include_sundays
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Første nyttårsdag"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Skjærtorsdag"))

        # Good Friday.
        self._add_good_friday(tr("Langfredag"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Første påskedag"))

        # Easter Monday.
        self._add_easter_monday(tr("Andre påskedag"))

        # Source: https://web.archive.org/web/20250102052441/https://lovdata.no/dokument/NL/lov/1947-04-26-1
        if self._year >= 1947:
            # Labor Day.
            self._add_labor_day(tr("Arbeidernes dag"))

            # Constitution Day.
            self._add_holiday_may_17(tr("Grunnlovsdag"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Kristi himmelfartsdag"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Første pinsedag"))

        # Whit Monday.
        self._add_whit_monday(tr("Andre pinsedag"))

        # According to https://no.wikipedia.org/wiki/Første_juledag,
        # these dates are only valid from year > 1700
        # Wikipedia has no source for the statement, so leaving this be for now

        # Christmas Day.
        self._add_christmas_day(tr("Første juledag"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Andre juledag"))

        if self.include_sundays:
            # Optionally add all Sundays of the year.
            for dt in _get_all_sundays(self._year):
                # Sunday.
                self._add_holiday(tr("Søndag"), dt)


class NO(Norway):
    pass


class NOR(Norway):
    pass
