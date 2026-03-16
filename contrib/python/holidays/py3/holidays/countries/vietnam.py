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

from __future__ import annotations

from gettext import gettext as tr
from typing import TYPE_CHECKING

from holidays.calendars.chinese import VIETNAMESE_CALENDAR
from holidays.calendars.gregorian import (
    JAN,
    FEB,
    APR,
    MAY,
    SEP,
    DEC,
    MON,
    TUE,
    WED,
    THU,
    FRI,
    SAT,
    SUN,
    _timedelta,
)
from holidays.groups import ChineseCalendarHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    ObservedRule,
    SAT_TO_PREV_WORKDAY,
    SUN_TO_NEXT_WORKDAY,
    SAT_SUN_TO_NEXT_WORKDAY,
)

if TYPE_CHECKING:
    from datetime import date

NATIONAL_DAY_RULE = ObservedRule({MON: +1, TUE: -1, WED: -1, THU: +1, FRI: -1, SAT: -1, SUN: +1})


class Vietnam(ObservedHolidayBase, ChineseCalendarHolidays, InternationalHolidays, StaticHolidays):
    """Vietnam holidays.

    References:
        * [Labor Code 1994 (Art. 73) (en)](https://web.archive.org/web/20160630110506/http://vbpl.vn/TW/Pages/vbpqen-toanvan.aspx?ItemID=2835)
        * [Labor Code 2012 (Art. 115) (en)](https://web.archive.org/web/20240706232343/http://vbpl.vn/TW/Pages/vbpqen-toanvan.aspx?ItemID=11013)
        * [Labor Code 2012 (Art. 115) (vi)](https://web.archive.org/web/20240917113133/https://vbpl.vn/TW/Pages/vbpq-toanvan.aspx?ItemID=27615)
        * [Labor Code 2019 (Art. 112) (en)](https://web.archive.org/web/20250108181808/https://vbpl.vn/TW/pages/vbpqen-toanvan.aspx?ItemID=11135)
        * [Labor Code 2019 (Art. 112) (vi)](https://web.archive.org/web/20250221171552/https://vbpl.vn/TW/Pages/vbpq-van-ban-goc.aspx?ItemID=139264)
    """

    country = "VN"
    # %s (estimated).
    estimated_label = tr("%s (dự kiến)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (nghỉ bù, dự kiến)")
    # %s (observed).
    observed_label = tr("%s (nghỉ bù)")
    default_language = "vi"
    supported_languages = ("en_US", "th", "vi")

    def __init__(self, *args, **kwargs):
        ChineseCalendarHolidays.__init__(self, calendar=VIETNAMESE_CALENDAR)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, VietnamStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 1995)
        super().__init__(*args, **kwargs)

    def _add_lunar_new_year_observed(self, dt_lny: date) -> None:
        if self._year <= 1994:
            return None

        day_names = {
            # 29 of Lunar New Year.
            -2: tr("29 Tết"),
            # Fourth Day of Lunar New Year.
            3: tr("Mùng bốn Tết Nguyên Đán"),
            # Fifth Day of Lunar New Year.
            4: tr("Mùng năm Tết Nguyên Đán"),
            # Sixth Day of Lunar New Year.
            5: tr("Mùng sáu Tết Nguyên Đán"),
        }
        for delta in range(-1, 4 if self._year >= 2013 else 3):
            dt = _timedelta(dt_lny, delta)
            dt_observed = self._get_observed_date(
                dt,
                rule=(
                    SAT_TO_PREV_WORKDAY + SUN_TO_NEXT_WORKDAY
                    if self._year >= 2014
                    else SAT_SUN_TO_NEXT_WORKDAY
                ),
            )
            if dt_observed != dt:
                self._add_holiday(
                    day_names[(dt_observed - dt_lny).days],  # type: ignore[operator]
                    dt_observed,
                )

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("Tết Dương lịch")))

        # Lunar New Year's Eve.
        self._add_chinese_new_years_eve(tr("Giao thừa Tết Nguyên Đán"))

        # Lunar New Year.
        lny = self._add_chinese_new_years_day(tr("Tết Nguyên Đán"))

        # Second Day of Lunar New Year.
        self._add_chinese_new_years_day_two(tr("Mùng hai Tết Nguyên Đán"))

        # Third Day of Lunar New Year.
        self._add_chinese_new_years_day_three(tr("Mùng ba Tết Nguyên Đán"))

        if self._year >= 2013:
            # Fourth Day of Lunar New Year.
            self._add_chinese_new_years_day_four(tr("Mùng bốn Tết Nguyên Đán"))

        if self._year >= 2007:
            # Hung Kings' Commemoration Day.
            dts_observed.add(self._add_hung_kings_day(tr("Ngày Giỗ Tổ Hùng Vương")))

        # Liberation Day/Reunification Day.
        dts_observed.add(self._add_holiday_apr_30(tr("Ngày Chiến thắng")))

        # International Labor Day.
        dts_observed.add(self._add_labor_day(tr("Ngày Quốc tế Lao động")))

        # National Day.
        name = tr("Quốc khánh")
        dts_observed.add(sep_2 := self._add_holiday_sep_2(name))
        if self._year >= 2021:
            self._add_holiday(name, self._get_observed_date(sep_2, NATIONAL_DAY_RULE))

        if self.observed:
            self._add_lunar_new_year_observed(lny)
            self._populate_observed(dts_observed)


class VN(Vietnam):
    pass


class VNM(Vietnam):
    pass


class VietnamStaticHolidays:
    """Vietnam special holidays.

    References:
        * [2018-2019](https://web.archive.org/web/20250427182343/https://thuvienphapluat.vn/cong-van/EN/Lao-dong-Tien-luong/Official-Dispatch-6519-VPCP-KGVX-2018-national-holidays-for-public-sector-employees/387625/tieng-anh.aspx)
        * [2024](https://web.archive.org/web/20241002165957/https://thuvienphapluat.vn/cong-van/EN/Lao-dong-Tien-luong/Official-Dispatch-2450-VPCP-KGVX-2024-swap-of-working-days-during-the-Reunification-Day/606458/tieng-anh.aspx)
    """

    # Date format (see strftime() Format Codes).
    substituted_date_format = tr("%d/%m/%Y")
    # Day off (substituted from %s).
    substituted_label = tr("Ngày nghỉ (thay cho ngày %s)")

    special_public_holidays = {
        2010: (FEB, 19, FEB, 27),
        2012: (JAN, 27, FEB, 4),
        2013: (APR, 29, MAY, 4),
        2014: (
            (MAY, 2, APR, 26),
            (SEP, 1, SEP, 6),
        ),
        2015: (
            (JAN, 2, DEC, 27, 2014),
            (FEB, 16, FEB, 14),
            (APR, 29, APR, 25),
        ),
        2018: (DEC, 31, JAN, 5, 2019),
        2019: (APR, 29, MAY, 4),
        2024: (APR, 29, MAY, 4),
    }
