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

from holidays.calendars.gregorian import GREGORIAN_CALENDAR
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Moldova(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Moldova holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Moldova>
        * <https://archive.org/details/httpswww.legis.mdcautaregetresultsdoc_id133686langro>
    """

    country = "MD"
    default_language = "ro"
    supported_languages = ("en_US", "ro", "uk")
    start_year = 1991

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Anul Nou"))

        name = (
            # Christmas Day (by old style).
            tr("Nașterea lui Iisus Hristos (Crăciunul pe stil vechi)")
            if self._year >= 2014
            # Christmas Day.
            else tr("Nașterea lui Iisus Hristos (Crăciunul)")
        )
        self._add_christmas_day(name)
        self._add_christmas_day_two(name)

        # International Women's Day.
        self._add_womens_day(tr("Ziua internatională a femeii"))

        # Easter.
        name = tr("Paștele")
        self._add_easter_sunday(name)
        self._add_easter_monday(name)

        # Day of Rejoicing.
        self._add_holiday_8_days_past_easter(tr("Paștele blajinilor"))

        # International Workers' Solidarity Day.
        self._add_labor_day(tr("Ziua internaţională a solidarităţii oamenilor muncii"))

        self._add_world_war_two_victory_day(
            # Victory Day and Commemoration of the heroes fallen for
            # Independence of Fatherland.
            tr("Ziua Victoriei și a comemorării eroilor căzuţi pentru Independenţa Patriei"),
            is_western=False,
        )

        if self._year >= 2017:
            # Europe Day.
            self._add_europe_day(tr("Ziua Europei"))

        if self._year >= 2016:
            # International Children's Day.
            self._add_childrens_day(tr("Ziua Ocrotirii Copilului"))

        # Republic of Moldova Independence Day.
        self._add_holiday_aug_27(tr("Ziua independenţei Republicii Moldova"))

        # National Language Day.
        self._add_holiday_aug_31(tr("Limba noastră"))

        if self._year >= 2013:
            self._add_christmas_day(
                # Christmas Day (by new style).
                tr("Nașterea lui Iisus Hristos (Crăciunul pe stil nou)"),
                GREGORIAN_CALENDAR,
            )


class MD(Moldova):
    pass


class MDA(Moldova):
    pass
