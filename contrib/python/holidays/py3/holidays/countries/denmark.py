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
from holidays.holiday_base import HolidayBase


class Denmark(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Denmark holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Denmark>
        * <https://da.wikipedia.org/wiki/Danske_helligdage>
        * <https://da.wikipedia.org/wiki/Helligdagsreformen_af_1770>
        * <https://web.archive.org/web/20250422010005/https://www.norden.org/en/info-norden/public-holidays-denmark>
        * <https://web.archive.org/web/20231219155820/http://www.ft.dk/samling/20222/lovforslag/l13/index.htm>
        * <https://web.archive.org/web/20250119084025/https://www.retsinformation.dk/eli/lta/2023/214>
        * <https://web.archive.org/web/20251016080453/https://cphpost.dk/2025-05-01/life-in-denmark/what-happens-on-the-1st-of-may/>
        * <https://web.archive.org/web/20140202200655/https://bibliotek.kk.dk/biblioteker/blog/grundlovsdag-dens-traditioner>
    """

    country = "DK"
    default_language = "da"
    # The Helligdagsreformen af 1770 was in effect from October 16th, 1770 onwards.
    start_year = 1771
    supported_categories = (OPTIONAL, PUBLIC)
    supported_languages = ("da", "en_US", "th", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nytårsdag"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Skærtorsdag"))

        # Good Friday.
        self._add_good_friday(tr("Langfredag"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Påskedag"))

        # Easter Monday.
        self._add_easter_monday(tr("Anden påskedag"))

        # Removed via Act no. 214 of 06/03/2023, in effect January 1st, 2024.
        if self._year <= 2023:
            # Great Day of Prayers.
            self._add_holiday_26_days_past_easter(tr("Store bededag"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Kristi himmelfartsdag"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Pinsedag"))

        # Whit Monday.
        self._add_whit_monday(tr("Anden pinsedag"))

        # Christmas Day.
        self._add_christmas_day(tr("Juledag"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Anden juledag"))

    def _populate_optional_holidays(self):
        # First observance in 1890.
        if self._year >= 1890:
            # Workers' Day.
            self._add_labor_day(tr("Arbejdernes kampdag"))

        # Gained its statutory day-off status in 1891.
        if self._year >= 1891:
            # Constitution Day.
            self._add_holiday_jun_5(tr("Grundlovsdag"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Juleaftensdag"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Nytårsaften"))


class DK(Denmark):
    pass


class DNK(Denmark):
    pass
