# -*- coding: utf-8 -*-

#  python-holidays
#  ---------------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Author:  ryanss <ryanssdev@icloud.com> (c) 2014-2017
#           dr-prodigy <maurizio.montel@gmail.com> (c) 2017-2020
#  Website: https://github.com/dr-prodigy/python-holidays
#  License: MIT (see LICENSE file)

from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta as rd, FR, SA, MO

from holidays.constants import JAN, APR, MAY, SEP
from holidays.constants import SAT, SUN
from holidays.holiday_base import HolidayBase

# Installation: pip install korean_lunar_calendar
# URL: https://github.com/usingsky/korean_lunar_calendar_py/
from korean_lunar_calendar import KoreanLunarCalendar


class Vietnam(HolidayBase):

    # https://publicholidays.vn/
    # http://vbpl.vn/TW/Pages/vbpqen-toanvan.aspx?ItemID=11013 Article.115
    # https://www.timeanddate.com/holidays/vietnam/

    def __init__(self, **kwargs):
        self.country = "VN"
        self.korean_cal = KoreanLunarCalendar()
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        # New Year's Day
        name = "International New Year's Day"
        first_date = date(year, JAN, 1)
        self[first_date] = name
        if self.observed:
            self[first_date] = name
            if first_date.weekday() == SAT:
                self[first_date + rd(days=+2)] = name + " observed"
            elif first_date.weekday() == SUN:
                self[first_date + rd(days=+1)] = name + " observed"

        # Lunar New Year
        name = ["Vietnamese New Year",           # index: 0
                "The second day of Tet Holiday",  # index: 1
                "The third day of Tet Holiday",  # index: 2
                "The forth day of Tet Holiday",  # index: 3
                "The fifth day of Tet Holiday",  # index: 4
                "Vietnamese New Year's Eve",     # index: -1
                ]
        dt = self.get_solar_date(year, 1, 1)
        new_year_date = date(dt.year, dt.month, dt.day)
        if self.observed:
            for i in range(-1, 5, 1):
                tet_day = new_year_date + rd(days=+i)
                self[tet_day] = name[i]

        # Vietnamese Kings' Commemoration Day
        # https://en.wikipedia.org/wiki/H%C3%B9ng_Kings%27_Festival
        if year >= 2007:
            name = "Hung Kings Commemoration Day"
            dt = self.get_solar_date(year, 3, 10)
            king_hung_date = date(dt.year, dt.month, dt.day)
            self[king_hung_date] = name
        else:
            pass

        # Liberation Day/Reunification Day
        name = "Liberation Day/Reunification Day"
        libration_date = date(year, APR, 30)
        self[libration_date] = name

        # International Labor Day
        name = "International Labor Day"
        labor_date = date(year, MAY, 1)
        self[labor_date] = name

        # Independence Day
        name = "Independence Day"
        independence_date = date(year, SEP, 2)
        self[independence_date] = name

    # convert lunar calendar date to solar
    def get_solar_date(self, year, month, day):
        self.korean_cal.setLunarDate(year, month, day, False)
        return date(self.korean_cal.solarYear, self.korean_cal.solarMonth,
                    self.korean_cal.solarDay)


class VN(Vietnam):
    pass


class VNM(Vietnam):
    pass
