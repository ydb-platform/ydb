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

from holidays.constants import JAN, MAR, APR, MAY, JUN, JUL, AUG, OCT, DEC
from holidays.constants import MON, TUE, WED, THU, FRI, SAT, SUN
from holidays.holiday_base import HolidayBase

# Installation: pip install korean_lunar_calendar
# URL: https://github.com/usingsky/korean_lunar_calendar_py/
from korean_lunar_calendar import KoreanLunarCalendar


class Korea(HolidayBase):

    # https://publicholidays.co.kr/ko/2020-dates/
    # https://en.wikipedia.org/wiki/Public_holidays_in_South_Korea
    # http://www.law.go.kr/%EB%B2%95%EB%A0%B9/%EA%B4%80%EA%B3%B5%EC%84%9C%EC%9D%98%20%EA%B3%B5%ED%9C%B4%EC%9D%BC%EC%97%90%20%EA%B4%80%ED%95%9C%20%EA%B7%9C%EC%A0%95

    def __init__(self, **kwargs):
        self.country = "KR"
        self.korean_cal = KoreanLunarCalendar()
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        alt_holiday = "Alternative holiday of "

        # New Year's Day
        name = "New Year's Day"
        first_date = date(year, JAN, 1)
        if self.observed:
            self[first_date] = name
            if first_date.weekday() == SUN:
                self[first_date + rd(days=+1)] = alt_holiday + \
                    self.first_lower(name)
                first_date = first_date + rd(days=+1)
            else:
                self[first_date] = name
        else:
            self[first_date] = name

        # Lunar New Year
        name = "Lunar New Year's Day"
        preceding_day_lunar = "The day preceding of " + name
        second_day_lunar = "The second day of " + name

        dt = self.get_solar_date(year, 1, 1)
        new_year_date = date(dt.year, dt.month, dt.day)
        if self.observed and year >= 2015:
            if new_year_date.weekday() in [TUE, WED, THU, FRI]:
                self[new_year_date + rd(days=-1)] = preceding_day_lunar
                self[new_year_date] = name
                self[new_year_date + rd(days=+1)] = second_day_lunar
            elif new_year_date.weekday() in [SAT, SUN, MON]:
                self[new_year_date + rd(days=-1)] = preceding_day_lunar
                self[new_year_date] = name
                self[new_year_date + rd(days=+1)] = second_day_lunar
                self[new_year_date + rd(days=+2)] = alt_holiday + name
        else:
            self[new_year_date + rd(days=-1)] = preceding_day_lunar
            self[new_year_date] = name
            self[new_year_date + rd(days=+1)] = second_day_lunar

        # Independence Movement Day
        name = "Independence Movement Day"
        independence_date = date(year, MAR, 1)
        self[independence_date] = name

        # Tree Planting Day
        name = "Tree Planting Day"
        planting_date = date(year, APR, 5)
        if self.observed and 1949 <= year <= 2007 and year != 1960:
            self[planting_date] = name
        else:
            # removed from holiday since 2007
            pass

        # Birthday of the Buddha
        name = "Birthday of the Buddha"
        dt = self.get_solar_date(year, 4, 8)
        buddha_date = date(dt.year, dt.month, dt.day)
        self[buddha_date] = name

        # Children's Day
        name = "Children's Day"
        childrens_date = date(year, MAY, 5)
        if year >= 1975:
            self[childrens_date] = name
            if self.observed and year >= 2015:
                if childrens_date.weekday() == SUN:
                    self[childrens_date + rd(days=+1)] = alt_holiday + name
                if childrens_date.weekday() == SAT:
                    self[childrens_date + rd(days=+2)] = alt_holiday + name

                # if holiday overlaps with other holidays, should be next day.
                # most likely: Birthday of the Buddah
                if self[childrens_date] != name:
                    self[childrens_date + rd(days=+1)] = alt_holiday + name
        else:
            # no children's day before 1975
            pass

        # Labour Day
        name = "Labour Day"
        labour_date = date(year, MAY, 1)
        self[labour_date] = name

        # Memorial Day
        name = "Memorial Day"
        memorial_date = date(year, JUN, 6)
        self[memorial_date] = name

        # Constitution Day
        name = "Constitution Day"
        constitution_date = date(year, JUL, 17)
        if self.observed and 1948 <= year <= 2007:
            self[constitution_date] = name
        else:
            # removed from holiday since 2008
            pass

        # Liberation Day
        name = "Liberation Day"
        libration_date = date(year, AUG, 15)
        if self.observed and year >= 1945:
            self[libration_date] = name
        else:
            pass

        # Korean Mid Autumn Day
        name = "Chuseok"
        preceding_day_chuseok = "The day preceding of " + name
        second_day_chuseok = "The second day of " + name
        dt = self.get_solar_date(year, 8, 15)
        new_year_date = date(dt.year, dt.month, dt.day)
        if self.observed and year >= 2014:
            if new_year_date.weekday() in [TUE, WED, THU, FRI]:
                self[new_year_date + rd(days=-1)] = preceding_day_chuseok
                self[new_year_date] = name
                self[new_year_date + rd(days=+1)] = second_day_chuseok
            elif new_year_date.weekday() in [SAT, SUN, MON]:
                self[new_year_date + rd(days=-1)] = preceding_day_chuseok
                self[new_year_date] = name
                self[new_year_date + rd(days=+1)] = second_day_chuseok
                self[new_year_date + rd(days=+2)] = alt_holiday + name
        else:
            self[new_year_date + rd(days=-1)] = preceding_day_chuseok
            self[new_year_date] = name
            self[new_year_date + rd(days=+1)] = second_day_chuseok

        # National Foundation Day
        name = "National Foundation Day"
        foundation_date = date(year, OCT, 3)
        self[foundation_date] = name

        # Hangul Day
        name = "Hangeul Day"
        hangeul_date = date(year, OCT, 9)
        self[hangeul_date] = name

        # Christmas Day
        name = "Christmas Day"
        christmas_date = date(year, DEC, 25)
        self[christmas_date] = name

        # Just for year 2020 - since 2020.08.15 is Sat, the government
        # decided to make 2020.08.17 holiday, yay
        name = "Alternative public holiday"
        alt_date = date(2020, OCT, 17)
        self[alt_date] = name

    # convert lunar calendar date to solar
    def get_solar_date(self, year, month, day):
        self.korean_cal.setLunarDate(year, month, day, False)
        return date(self.korean_cal.solarYear, self.korean_cal.solarMonth,
                    self.korean_cal.solarDay)

    def first_lower(self, s):
        return s[0].lower() + s[1:]


class KR(Korea):
    pass


class KOR(Korea):
    pass
