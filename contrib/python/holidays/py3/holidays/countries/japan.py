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

from datetime import date
from gettext import gettext as tr

from holidays.calendars.gregorian import (
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    _timedelta,
)
from holidays.constants import BANK, PUBLIC
from holidays.groups import InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_WORKDAY


class Japan(ObservedHolidayBase, InternationalHolidays, StaticHolidays):
    """Japan holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Japan>
        * <https://web.archive.org/web/20240913161809/https://www.boj.or.jp/en/about/outline/holi.htm>
    """

    country = "JP"
    default_language = "ja"
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("en_US", "ja", "th")
    start_year = 1949
    end_year = 2099

    def __init__(self, *args, **kwargs) -> None:
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=JapanStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _is_observed(self, dt: date) -> bool:
        return dt >= date(1973, APR, 12)

    def _populate_observed(self, dts: set[date]) -> None:
        # When a national holiday falls on Sunday, next working day
        # shall become a public holiday (振替休日) - substitute holiday.
        for dt in sorted(dts):
            # Substitute Holiday.
            self._add_observed(dt, name=tr("振替休日"), show_observed_label=False)

        # A weekday between national holidays becomes a holiday too (国民の休日) -
        # national holiday.
        # In 1986-2006 it was only May 4 (between Constitution Day and Children's Day).
        # Since 2006, it may be only the day between Respect for the Aged Day and
        # Autumnal Equinox Day (in September).
        if self._year <= 1985:
            return None
        if self._year <= 2006:
            may_4 = (MAY, 4)
            if not self._is_monday(may_4) and not self._is_sunday(may_4):
                # National Holiday.
                self._add_holiday(tr("国民の休日"), may_4)
        else:
            for dt in dts:
                if dt.month == SEP and _timedelta(dt, +2) in dts:
                    # National Holiday.
                    self._add_holiday(tr("国民の休日"), _timedelta(dt, +1))
                    break

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("元日")))

        # Coming of Age Day.
        name = tr("成人の日")
        dts_observed.add(
            self._add_holiday_jan_15(name)
            if self._year <= 1999
            else self._add_holiday_2nd_mon_of_jan(name)
        )

        if self._year >= 1967:
            # Foundation Day.
            dts_observed.add(self._add_holiday_feb_11(tr("建国記念の日")))

        if self._year != 2019:
            # Emperor's Birthday.
            name = tr("天皇誕生日")
            if self._year >= 2020:
                # Reiwa Emperor's Birthday.
                dt = self._add_holiday_feb_23(name)
            elif self._year >= 1989:
                # Heisei Emperor's Birthday.
                dt = self._add_holiday_dec_23(name)
            else:
                # Showa Emperor's Birthday.
                dt = self._add_holiday_apr_29(name)
            dts_observed.add(dt)

        # Vernal Equinox Day.
        dts_observed.add(self._add_holiday(tr("春分の日"), self._vernal_equinox_date))

        if self._year >= 2007:
            # Showa Day.
            dts_observed.add(self._add_holiday_apr_29(tr("昭和の日")))

        # Constitution Day.
        dts_observed.add(self._add_holiday_may_3(tr("憲法記念日")))

        if self._year >= 1989:
            # Greenery Day.
            name = tr("みどりの日")
            dts_observed.add(
                self._add_holiday_may_4(name)
                if self._year >= 2007
                else self._add_holiday_apr_29(name)
            )

        # Children's Day.
        dts_observed.add(self._add_holiday_may_5(tr("こどもの日")))

        if self._year >= 1996:
            # Marine Day.
            name = tr("海の日")
            if self._year <= 2002:
                dts_observed.add(self._add_holiday_jul_20(name))
            else:
                dates = {
                    2020: (JUL, 23),
                    2021: (JUL, 22),
                }
                dts_observed.add(self._add_holiday(name, dt)) if (
                    dt := dates.get(self._year)
                ) else dts_observed.add(self._add_holiday_3rd_mon_of_jul(name))

        if self._year >= 2016:
            dates = {
                2020: (AUG, 10),
                2021: (AUG, 8),
            }
            # Mountain Day.
            name = tr("山の日")
            dts_observed.add(self._add_holiday(name, dates.get(self._year, (AUG, 11))))

        if self._year >= 1966:
            # Respect for the Aged Day.
            name = tr("敬老の日")
            dts_observed.add(
                self._add_holiday_3rd_mon_of_sep(name)
                if self._year >= 2003
                else self._add_holiday_sep_15(name)
            )

        # Autumnal Equinox Day.
        dts_observed.add(self._add_holiday(tr("秋分の日"), self._autumnal_equinox_date))

        # Physical Education and Sports Day.
        if self._year >= 1966:
            name = (
                # Sports Day.
                tr("スポーツの日")
                if self._year >= 2020
                # Physical Education Day.
                else tr("体育の日")
            )
            if self._year >= 2000:
                dates = {
                    2020: (JUL, 24),
                    2021: (JUL, 23),
                }
                dts_observed.add(self._add_holiday(name, dt)) if (
                    dt := dates.get(self._year)
                ) else dts_observed.add(self._add_holiday_2nd_mon_of_oct(name))
            else:
                dts_observed.add(self._add_holiday_oct_10(name))

        # Culture Day.
        dts_observed.add(self._add_holiday_nov_3(tr("文化の日")))

        # Labor Thanksgiving Day.
        dts_observed.add(self._add_holiday_nov_23(tr("勤労感謝の日")))

        if self.observed:
            self._populate_observed(dts_observed)

    def _populate_bank_holidays(self):
        # Bank Holiday.
        name = tr("銀行休業日")
        self._add_new_years_day(name)
        self._add_new_years_day_two(name)
        self._add_new_years_day_three(name)
        self._add_new_years_eve(name)

    @property
    def _vernal_equinox_date(self) -> tuple[int, int]:
        day = 20
        if (
            (self._year % 4 == 0 and self._year <= 1956)
            or (self._year % 4 == 1 and self._year <= 1989)
            or (self._year % 4 == 2 and self._year <= 2022)
            or (self._year % 4 == 3 and self._year <= 2055)
        ):
            day = 21
        elif self._year % 4 == 0 and self._year >= 2092:
            day = 19
        return MAR, day

    @property
    def _autumnal_equinox_date(self) -> tuple[int, int]:
        day = 23
        if self._year % 4 == 3 and self._year <= 1979:
            day = 24
        elif (
            (self._year % 4 == 0 and self._year >= 2012)
            or (self._year % 4 == 1 and self._year >= 2045)
            or (self._year % 4 == 2 and self._year >= 2078)
        ):
            day = 22
        return SEP, day


class JP(Japan):
    pass


class JPN(Japan):
    pass


class JapanStaticHolidays:
    national_holiday = tr("国民の休日")

    special_public_holidays = {
        1959: (APR, 10, tr("結婚の儀")),  # The Crown Prince marriage ceremony.
        1989: (FEB, 24, tr("大喪の礼")),  # State Funeral of Emperor Shōwa.
        1990: (NOV, 12, tr("即位礼正殿の儀")),  # Enthronement ceremony.
        1993: (JUN, 9, tr("結婚の儀")),  # The Crown Prince marriage ceremony.
        2019: (
            (MAY, 1, tr("天皇の即位の日")),  # Enthronement day.
            (OCT, 22, tr("即位礼正殿の儀が行われる日")),  # Enthronement ceremony.
        ),
    }

    special_public_holidays_observed = {
        2019: (
            (APR, 30, national_holiday),
            (MAY, 2, national_holiday),
        ),
    }
