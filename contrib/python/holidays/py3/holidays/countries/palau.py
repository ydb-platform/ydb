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

from holidays.calendars.gregorian import SEP, NOV
from holidays.constants import ARMED_FORCES, HALF_DAY, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_TO_PREV_FRI, SUN_TO_NEXT_MON


class Palau(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Palau holidays.

    References:
        * [Chapter 7, Holidays](https://web.archive.org/web/20250130144425/http://www.paclii.org/pw/legis/consol_act/gpt1262/)
        * <https://web.archive.org/web/20240627055859/https://www.palaugov.pw/wp-content/uploads/2017/11/RPPL-No.-10-15-re.-Family-Day-Holiday.pdf>
        * [EO336 Memorial Day repealed](https://web.archive.org/web/20250429131246/https://www.facebook.com/plugins/post.php?href=https://www.facebook.com/PalauPresident/posts/195883107230463)
        * [Earliest source for President's Day](https://web.archive.org/web/20250429075658/https://www.taiwanembassy.org/pal_en/post/792.html)

    If any of the holidays enumerated in section 701 of this chapter falls on Sunday, the
    following Monday shall be observed as a holiday. If any of the holidays enumerated in
    section 701 of this chapter falls on Saturday, the preceding Friday shall be observed
    as a holiday.

    As there's no record of President's Day (Jun 1) and Independence Day (Oct 1) being
    legal holiday before 2017, as seen in RPRL 10-15, they shall be assumed to start in 2018
    for our current implementation.
    """

    country = "PW"
    supported_categories = (ARMED_FORCES, HALF_DAY, PUBLIC)
    observed_label = "%s (observed)"
    # Republic of Palau Public Law No. 2-15.
    # The legislation was first adopted by the 2nd Olbiil Era Kelulau (1984-1988),
    # but since we cannot find any info on its actual adoption date, we may as
    # well use the formation date of the country as the placeholder cut-off date.
    start_year = 1981

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, PalauStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_TO_PREV_FRI + SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Fixed Date Public Holidays.

        # New Year's Day.
        name = "New Year's Day"
        self._add_observed(self._add_new_years_day(name))
        self._add_observed(self._next_year_new_years_day, name=name, rule=SAT_TO_PREV_FRI)

        # Youth Day.
        self._add_observed(self._add_holiday_mar_15("Youth Day"))

        # Senior Citizens Day.
        self._add_observed(self._add_holiday_may_5("Senior Citizens Day"))

        if self._year in {2011, 2012}:
            # Memorial Day.
            self._add_holiday_last_mon_of_may("Memorial Day")

        if self._year >= 2018:
            # President's Day.
            self._add_observed(self._add_holiday_jun_1("President's Day"))

        # Constitution Day.
        self._add_observed(self._add_holiday_jul_9("Constitution Day"))

        # Labor Day.
        self._add_holiday_1st_mon_of_sep("Labor Day")

        if self._year >= 2018:
            # Independence Day.
            self._add_observed(self._add_holiday_oct_1("Independence Day"))

        # United Nations Day.
        self._add_observed(self._add_united_nations_day("United Nations Day"))

        # Thanksgiving Day.
        self._add_holiday_4th_thu_of_nov("Thanksgiving Day")

        if self._year >= 2017:
            # Family Day.
            self._add_holiday_4th_fri_of_nov("Family Day")

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"))


class PW(Palau):
    pass


class PLW(Palau):
    pass


class PalauStaticHolidays:
    """Palau special holidays.

    References:
        * <https://web.archive.org/web/20250608202645/https://www.facebook.com/photo?fbid=1774513196034105&set=a.175933635892077>
        * <https://web.archive.org/web/20250608202650/https://www.facebook.com/photo/?fbid=1794692910682800&set=a.175933635892077>
        * <https://web.archive.org/web/20250608202639/https://www.facebook.com/photo/?fbid=1408133829338712&set=a.175933635892077>
    """

    special_armed_forces_holidays = {
        2020: (NOV, 11, "Veterans Day"),
    }

    special_half_day_holidays = {
        2019: (SEP, 30, "Preparation for the 25th Independence Day of the Republic of Palau"),
    }

    special_public_holidays = {
        2020: (NOV, 3, "National Day of Democracy"),
    }
