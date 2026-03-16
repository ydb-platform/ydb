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

from holidays.calendars.gregorian import JUN
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class TurksAndCaicosIslands(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Turks and Caicos Islands holidays.

    References:
        * [Wikipedia](https://en.wikipedia.org/wiki/Public_holidays_in_the_Turks_and_Caicos_Islands)
        * [Public Holidays Ordinance, rev. 2014](https://web.archive.org/web/20250210082429/https://gov.tc/agc/component/edocman/21-02-public-holidays-ordinance-2/viewdocument/599?Itemid=)
        * [Ordinance 5 of 2020](https://web.archive.org/web/20250429025117/https://www.gov.tc/agc/component/edocman/05-of-2020-public-holidays-amendment-ordinance/viewdocument/1419?Itemid=)
        * [Public Holidays Ordinance, rev. 2021](https://web.archive.org/web/20250429025602/https://www.gov.tc/agc/component/edocman/21-02-public-holidays-ordinance-2/viewdocument/2027?Itemid=)
        * [2017](https://web.archive.org/web/20250608202444/https://www.facebook.com/photo/?fbid=1137860329642985&set=a.349345645161128)
        * [2018](https://web.archive.org/web/20180126185141/https://gov.tc/pressoffice/999-listing-of-special-days-public-holidays-for-2018)
        * [2019](https://web.archive.org/web/20250608202446/https://www.facebook.com/photo/?fbid=2514587681970236&set=a.349345645161128)
        * [2020](https://web.archive.org/web/20250608202458/https://www.facebook.com/pressofficetcig/photos/a.349345645161128/2572825079479829/?type=3)
        * [2021](https://www.facebook.com/pressofficetcig/photos/a.349345645161128/3511371698958491/?type=3)
        * [2022](https://www.facebook.com/pressofficetcig/photos/a.349345645161128/4515540038541647/?type=3)
        * [2023](https://web.archive.org/web/20250608202452/https://www.facebook.com/photo/?fbid=557741379734203&set=a.363028765872133)
        * [2025](https://web.archive.org/web/20250608202433/https://www.facebook.com/photo/?fbid=1019082573584944&set=a.353200576839817)
        * [Destination TCI](https://web.archive.org/web/20250429024708/https://destinationtci.tc/turks-and-caicos-islands-public-holidays/)
        * [Time and Date](https://web.archive.org/web/20250429024853/https://www.timeanddate.com/holidays/turks-and-caicos-islands/)
        * [Constitution Day](https://web.archive.org/web/20251230163946/https://www.visittci.com/news/turks-and-caicos-public-holiday-changes-2026)
    """

    country = "TC"
    default_language = "en_TC"
    # %s (observed).
    observed_label = "%s (observed)"
    # Separated from Jamaica in 1962.
    start_year = 1963
    supported_languages = ("en_TC", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        # Public Holidays Act 1980 established weekend-to-Monday rule.
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 1980)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Commonwealth Day.
        self._add_holiday_2nd_mon_of_mar(tr("Commonwealth Day"))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        self._add_holiday_last_mon_of_may(
            # JAGS McCartney Day.
            tr("JAGS McCartney Day")
            if self._year >= 2020
            # National Heroes Day.
            else tr("National Heroes Day")
        )

        name = (
            # King's Birthday.
            tr("King's Birthday")
            if self._year >= 2023
            # Queen's Birthday.
            else tr("Queen's Birthday")
        )
        dates_obs = {
            2022: (JUN, 3),
            2023: (JUN, 19),
            2024: (JUN, 17),
            2025: (JUN, 23),
        }
        if dt := dates_obs.get(self._year):
            self._add_holiday(name, dt)
        else:
            self._add_holiday_2nd_mon_of_jun(name)

        # Emancipation Day.
        self._add_observed(self._add_holiday_aug_1(tr("Emancipation Day")))

        if self._year >= 2014:
            # National Day of Thanksgiving.
            name = tr("National Day of Thanksgiving")
            if self._year >= 2026:
                self._add_holiday_1_day_prior_last_mon_of_aug(name)
            else:
                self._add_holiday_4th_fri_of_nov(name)

        if self._year >= 2026:
            # Constitution Day.
            self._add_holiday_last_mon_of_aug(tr("Constitution Day"))

        # National Youth Day.
        self._add_holiday_last_fri_of_sep(tr("National Youth Day"))

        self._add_holiday_2nd_mon_of_oct(
            # National Heritage Day.
            tr("National Heritage Day")
            if self._year >= 2014
            # Columbus Day.
            else tr("Columbus Day")
        )

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )


class TC(TurksAndCaicosIslands):
    pass


class TCA(TurksAndCaicosIslands):
    pass
