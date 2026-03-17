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
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_PREV_SAT, SUN_TO_NEXT_MON


class Netherlands(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Netherlands holidays.

    Easter Sunday and Whit Sunday are always on Sunday and are therefore not mentioned separately
    in the Algemene termijnenwet (Atw).

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Netherlands>
        * <https://nl.wikipedia.org/wiki/Feestdagen_in_Nederland>
        * <https://nl.wikipedia.org/wiki/Algemene_termijnenwet>
        * <https://web.archive.org/web/20251015082530/https://business.gov.nl/regulation/holiday-entitlement/>
        * <https://web.archive.org/save/https://wetten.overheid.nl/BWBR0002448/2010-10-10>
        * <https://web.archive.org/web/20250123145153/https://repository.overheid.nl/frbr/sgd/19811982/0000151909/1/pdf/SGD_19811982_0002862.pdf>
        * <https://web.archive.org/web/20251015082056/https://www.facebook.com/photo.php?fbid=5357926707591435&id=190259871024837&set=a.342400825810740>
    """

    country = "NL"
    default_language = "nl"
    supported_categories = (OPTIONAL, PUBLIC)
    supported_languages = ("en_US", "fy", "nl", "th", "uk")
    # Algemene termijnenwet (Atw) entered into force on April 1st, 1965.
    start_year = 1966

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nieuwjaarsdag"))

        # Good Friday.
        self._add_good_friday(tr("Goede Vrijdag"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Eerste paasdag"))

        # Easter Monday.
        self._add_easter_monday(tr("Tweede paasdag"))

        self._move_holiday(
            # King's Day.
            self._add_holiday_apr_27(tr("Koningsdag"))
            if self._year >= 2014
            # Queen's Day.
            else self._add_holiday_apr_30(tr("Koninginnedag")),
            rule=SUN_TO_PREV_SAT if self._year >= 1980 else SUN_TO_NEXT_MON,
        )

        # Officially adopted as an annual national holiday in the Atw on January 5th, 1982.
        # In practice, many collective labor agreements (CAOs) stipulate that May 5th is
        # a day off only once every five years (e.g., 2030, 2035, etc.).
        if self._year % 5 == 0:
            # Liberation Day.
            self._add_holiday_may_5(tr("Bevrijdingsdag"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Hemelvaartsdag"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Eerste Pinksterdag"))

        # Whit Monday.
        self._add_whit_monday(tr("Tweede Pinksterdag"))

        # Christmas Day.
        self._add_christmas_day(tr("Eerste Kerstdag"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Tweede Kerstdag"))

    def _populate_optional_holidays(self):
        if self._year >= 1982:
            # Liberation Day.
            self._add_holiday_may_5(tr("Bevrijdingsdag"))


class NL(Netherlands):
    pass


class NLD(Netherlands):
    pass
