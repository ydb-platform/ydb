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

from datetime import date, datetime

from dateutil.easter import easter
from dateutil.relativedelta import relativedelta as rd

from holidays.constants import FRI, SUN
from holidays.constants import JAN, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, \
    DEC
from holidays.holiday_base import HolidayBase


class SouthAfrica(HolidayBase):
    def __init__(self, **kwargs):
        # http://www.gov.za/about-sa/public-holidays
        # https://en.wikipedia.org/wiki/Public_holidays_in_South_Africa
        self.country = "ZA"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Observed since 1910, with a few name changes
        if year > 1909:
            self[date(year, 1, 1)] = "New Year's Day"

            e = easter(year)
            good_friday = e - rd(days=2)
            easter_monday = e + rd(days=1)
            self[good_friday] = "Good Friday"
            if year > 1979:
                self[easter_monday] = "Family Day"
            else:
                self[easter_monday] = "Easter Monday"

            if 1909 < year < 1952:
                dec_16_name = "Dingaan's Day"
            elif 1951 < year < 1980:
                dec_16_name = "Day of the Covenant"
            elif 1979 < year < 1995:
                dec_16_name = "Day of the Vow"
            else:
                dec_16_name = "Day of Reconciliation"
            self[date(year, DEC, 16)] = dec_16_name

            self[date(year, DEC, 25)] = "Christmas Day"

            if year > 1979:
                dec_26_name = "Day of Goodwill"
            else:
                dec_26_name = "Boxing Day"
            self[date(year, 12, 26)] = dec_26_name

        # Observed since 1995/1/1
        if year > 1994:
            self[date(year, MAR, 21)] = "Human Rights Day"
            self[date(year, APR, 27)] = "Freedom Day"
            self[date(year, MAY, 1)] = "Workers' Day"
            self[date(year, JUN, 16)] = "Youth Day"
            self[date(year, AUG, 9)] = "National Women's Day"
            self[date(year, SEP, 24)] = "Heritage Day"

        # Once-off public holidays
        national_election = "National and provincial government elections"
        y2k = "Y2K changeover"
        local_election = "Local government elections"
        presidential = "By presidential decree"
        if year == 1999:
            self[date(1999, JUN, 2)] = national_election
            self[date(1999, DEC, 31)] = y2k
        if year == 2000:
            self[date(2000, JAN, 2)] = y2k
        if year == 2004:
            self[date(2004, APR, 14)] = national_election
        if year == 2006:
            self[date(2006, MAR, 1)] = local_election
        if year == 2008:
            self[date(2008, MAY, 2)] = presidential
        if year == 2009:
            self[date(2009, APR, 22)] = national_election
        if year == 2011:
            self[date(2011, MAY, 18)] = local_election
            self[date(2011, DEC, 27)] = presidential
        if year == 2014:
            self[date(2014, MAY, 7)] = national_election
        if year == 2016:
            self[date(2016, AUG, 3)] = local_election
        if year == 2019:
            self[date(2019, MAY, 8)] = national_election

        # As of 1995/1/1, whenever a public holiday falls on a Sunday,
        # it rolls over to the following Monday
        for k, v in list(self.items()):
            if self.observed and year > 1994 and k.weekday() == SUN:
                add_days = 1
                while self.get(k + rd(days=add_days)) is not None:
                    add_days += 1
                self[k + rd(days=add_days)] = v + " (Observed)"

        # Historic public holidays no longer observed
        if 1951 < year < 1974:
            self[date(year, APR, 6)] = "Van Riebeeck's Day"
        elif 1979 < year < 1995:
            self[date(year, APR, 6)] = "Founder's Day"

        if 1986 < year < 1990:
            historic_workers_day = datetime(year, MAY, 1)
            # observed on first Friday in May
            while historic_workers_day.weekday() != FRI:
                historic_workers_day += rd(days=1)

            self[historic_workers_day] = "Workers' Day"

        if 1909 < year < 1994:
            ascension_day = e + rd(days=40)
            self[ascension_day] = "Ascension Day"

        if 1909 < year < 1952:
            self[date(year, MAY, 24)] = "Empire Day"

        if 1909 < year < 1961:
            self[date(year, MAY, 31)] = "Union Day"
        elif 1960 < year < 1994:
            self[date(year, MAY, 31)] = "Republic Day"

        if 1951 < year < 1961:
            queens_birthday = datetime(year, JUN, 7)
            # observed on second Monday in June
            while queens_birthday.weekday() != 0:
                queens_birthday += rd(days=1)

            self[queens_birthday] = "Queen's Birthday"

        if 1960 < year < 1974:
            self[date(year, JUL, 10)] = "Family Day"

        if 1909 < year < 1952:
            kings_birthday = datetime(year, AUG, 1)
            # observed on first Monday in August
            while kings_birthday.weekday() != 0:
                kings_birthday += rd(days=1)

            self[kings_birthday] = "King's Birthday"

        if 1951 < year < 1980:
            settlers_day = datetime(year, SEP, 1)
            while settlers_day.weekday() != 0:
                settlers_day += rd(days=1)

            self[settlers_day] = "Settlers' Day"

        if 1951 < year < 1994:
            self[date(year, OCT, 10)] = "Kruger Day"


class ZA(SouthAfrica):
    pass


class ZAF(SouthAfrica):
    pass
