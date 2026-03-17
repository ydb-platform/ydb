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

from holidays.calendars.gregorian import JAN, FEB, MAR, SEP, OCT, NOV, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Namibia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Namibia holidays.

    References:
        * [Public Holidays Act, 1990](https://web.archive.org/web/20250322163344/https://namiblii.org/akn/na/act/1990/26/eng@2004-12-17)
        * [Public Holidays Amendment Act, 2004](https://web.archive.org/web/20221015121213/https://namiblii.org/akn/na/act/2004/16/eng@2004-12-17)
        * [Genocide Remembrance Day](https://web.archive.org/web/20250126055207/https://namiblii.org/akn/na/officialGazette/government-gazette/2024-05-28/8373/eng@2024-05-28)

    As of 1991/2/1, whenever a public holiday falls on a Sunday, it rolls over to the Monday,
    unless that Monday is already a public holiday.
    Since the interval from 1991/1/1 to 1991/2/1 includes only New Year's Day, and it's a Tuesday,
    we can assume that the beginning is 1991.
    """

    country = "NA"
    default_language = "en_NA"
    # %s (observed).
    observed_label = tr("%s (observed)")
    start_year = 1991
    supported_languages = ("en_NA", "en_US", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, NamibiaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Independence Day.
        self._add_observed(self._add_holiday_mar_21(tr("Independence Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Workers' Day.
        self._add_observed(self._add_labor_day(tr("Workers' Day")))

        # Cassinga Day.
        self._add_observed(self._add_holiday_may_4(tr("Cassinga Day")))

        # Africa Day.
        self._add_observed(self._add_africa_day(tr("Africa Day")))

        if self._year >= 2025:
            # Genocide Remembrance Day.
            self._add_holiday_may_28(tr("Genocide Remembrance Day"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension Day"))

        # Heroes' Day.
        self._add_observed(self._add_holiday_aug_26(tr("Heroes' Day")))

        self._add_observed(
            self._add_holiday_sep_10(
                # Day of the Namibian Women and International Human Rights Day.
                tr("Day of the Namibian Women and International Human Rights Day")
                if self._year >= 2005
                # International Human Rights Day.
                else tr("International Human Rights Day")
            )
        )

        # Christmas Day.
        self._add_christmas_day(tr("Christmas Day"))

        # Family Day.
        self._add_observed(self._add_christmas_day_two(tr("Family Day")))


class NA(Namibia):
    pass


class NAM(Namibia):
    pass


class NamibiaStaticHolidays:
    """Namibia special holidays.

    References:
        * [March 1, 1994](https://web.archive.org/web/20241210145818/https://namiblii.org/akn/na/officialGazette/government-gazette/1994-02-15/797/eng@1994-02-15)
        * [December 7, 1994](https://web.archive.org/web/20250613113650/https://namiblii.org/akn/na/officialGazette/government-gazette/1994-11-30/987/eng@1994-11-30)
        * [October 24, 1995](https://web.archive.org/web/20250613184422/https://namiblii.org/akn/na/officialGazette/government-gazette/1995-10-20/1178/eng@1995-10-20)
        * [September 26, 1997](https://web.archive.org/web/20250613172016/https://namiblii.org/akn/na/officialGazette/government-gazette/1997-09-25/1690/eng@1997-09-25)
        * [November 30, 1998](https://web.archive.org/web/20250613184549/https://namiblii.org/akn/na/officialGazette/government-gazette/1998-11-06/1988/eng@1998-11-06)
        * [November 30, 1999](https://web.archive.org/web/20241012184236/https://namiblii.org/akn/na/officialGazette/government-gazette/1999-10-08/2202/eng@1999-10-08)
        * [Y2K changeover](https://web.archive.org/web/20250613145912/https://namiblii.org/akn/na/officialGazette/government-gazette/1999-11-22/2234/eng@1999-11-22)
        * [November 28, 2014](https://web.archive.org/web/20250613184051/https://namiblii.org/akn/na/officialGazette/government-gazette/2014-11-07/5609/eng@2014-11-07)
        * [November 27, 2015](https://web.archive.org/web/20250613184258/https://namiblii.org/akn/na/officialGazette/government-gazette/2015-11-26/5885/eng@2015-11-26)
        * [November 27, 2019](https://web.archive.org/web/20241112233422/https://namiblii.org/akn/na/officialGazette/government-gazette/2019-09-30/7008/eng@2019-09-30)
        * [November 25, 2020](https://web.archive.org/web/20250613112403/https://namiblii.org/akn/na/officialGazette/government-gazette/2020-11-19/7394/eng@2020-11-19)
        * [February 25, 2024](https://web.archive.org/web/20241203060851/https://namiblii.org/akn/na/officialGazette/government-gazette/2024-02-20/8312/eng@2024-02-20)
        * [November 27, 2024](https://web.archive.org/web/20250514020246/https://namiblii.org/akn/na/officialGazette/government-gazette/2024-09-26/8454/eng@2024-09-26)
        * [March 1, 2025](https://web.archive.org/web/20250613172517/https://namiblii.org/akn/na/officialGazette/government-gazette/2025-02-14/8577/eng@2025-02-14)
    """

    # Y2K changeover.
    y2k_changeover = tr("Y2K changeover")

    # General Election Day.
    general_election_day = tr("General Election Day")

    # Regional Election Day.
    regional_election_day = tr("Regional Election Day")

    special_public_holidays = {
        1994: (
            # Walvis Bay Reintegration Day.
            (MAR, 1, tr("Walvis Bay Reintegration Day")),
            (DEC, 7, general_election_day),
        ),
        # Public Holiday.
        1995: (OCT, 24, tr("Public Holiday")),
        # Day of Mourning for Honourable Moses Garoeb.
        1997: (SEP, 26, tr("Day of Mourning for Honourable Moses Garoeb")),
        1998: (NOV, 30, regional_election_day),
        1999: (
            (NOV, 30, general_election_day),
            (DEC, 31, y2k_changeover),
        ),
        2000: (JAN, 3, y2k_changeover),
        2014: (NOV, 28, general_election_day),
        2015: (NOV, 27, regional_election_day),
        2019: (NOV, 27, general_election_day),
        2020: (NOV, 25, regional_election_day),
        2024: (
            # Burial ceremony of Dr. Hage Gottfried Geingob.
            (FEB, 25, tr("Burial ceremony of Dr. Hage Gottfried Geingob")),
            (NOV, 27, general_election_day),
        ),
        # Burial ceremony of Dr. Sam Shafiishuna Nujoma.
        2025: (MAR, 1, tr("Burial ceremony of Dr. Sam Shafiishuna Nujoma")),
    }
