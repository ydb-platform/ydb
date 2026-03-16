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

from datetime import date

from dateutil.easter import easter
from dateutil.relativedelta import relativedelta as rd, FR, SA

from holidays.constants import JAN, MAR, MAY, JUN, OCT, \
    DEC
from holidays.constants import MON, THU, FRI, SAT, SUN
from holidays.holiday_base import HolidayBase


class Sweden(HolidayBase):
    """
    Swedish holidays.
    Note that holidays falling on a sunday are "lost",
    it will not be moved to another day to make up for the collision.
    In Sweden, ALL sundays are considered a holiday
    (https://sv.wikipedia.org/wiki/Helgdagar_i_Sverige).
    Initialize this class with include_sundays=False
    to not include sundays as a holiday.
    Primary sources:
    https://sv.wikipedia.org/wiki/Helgdagar_i_Sverige and
    http://www.riksdagen.se/sv/dokument-lagar/dokument/svensk-forfattningssamling/lag-1989253-om-allmanna-helgdagar_sfs-1989-253
    """

    def __init__(self, include_sundays=True, **kwargs):
        """
        :param include_sundays: Whether to consider sundays as a holiday
        (which they are in Sweden)
        :param kwargs:
        """
        self.country = "SE"
        self.include_sundays = include_sundays
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # Add all the sundays of the year before adding the "real" holidays
        if self.include_sundays:
            first_day_of_year = date(year, JAN, 1)
            first_sunday_of_year = \
                first_day_of_year + \
                rd(days=SUN - first_day_of_year.weekday())
            cur_date = first_sunday_of_year

            while cur_date < date(year + 1, 1, 1):
                assert cur_date.weekday() == SUN

                self[cur_date] = "Söndag"
                cur_date += rd(days=7)

        # ========= Static holidays =========
        self[date(year, JAN, 1)] = "Nyårsdagen"

        self[date(year, JAN, 6)] = "Trettondedag jul"

        # Source: https://sv.wikipedia.org/wiki/F%C3%B6rsta_maj
        if year >= 1939:
            self[date(year, MAY, 1)] = "Första maj"

        # Source: https://sv.wikipedia.org/wiki/Sveriges_nationaldag
        if year >= 2005:
            self[date(year, JUN, 6)] = "Sveriges nationaldag"

        self[date(year, DEC, 24)] = "Julafton"
        self[date(year, DEC, 25)] = "Juldagen"
        self[date(year, DEC, 26)] = "Annandag jul"
        self[date(year, DEC, 31)] = "Nyårsafton"

        # ========= Moving holidays =========
        e = easter(year)
        maundy_thursday = e - rd(days=3)
        good_friday = e - rd(days=2)
        easter_saturday = e - rd(days=1)
        resurrection_sunday = e
        easter_monday = e + rd(days=1)
        ascension_thursday = e + rd(days=39)
        pentecost = e + rd(days=49)
        pentecost_day_two = e + rd(days=50)

        assert maundy_thursday.weekday() == THU
        assert good_friday.weekday() == FRI
        assert easter_saturday.weekday() == SAT
        assert resurrection_sunday.weekday() == SUN
        assert easter_monday.weekday() == MON
        assert ascension_thursday.weekday() == THU
        assert pentecost.weekday() == SUN
        assert pentecost_day_two.weekday() == MON

        self[good_friday] = "Långfredagen"
        self[resurrection_sunday] = "Påskdagen"
        self[easter_monday] = "Annandag påsk"
        self[ascension_thursday] = "Kristi himmelsfärdsdag"
        self[pentecost] = "Pingstdagen"
        if year <= 2004:
            self[pentecost_day_two] = "Annandag pingst"

        # Midsummer evening. Friday between June 19th and June 25th
        self[date(year, JUN, 19) + rd(weekday=FR)] = "Midsommarafton"

        # Midsummer day. Saturday between June 20th and June 26th
        if year >= 1953:
            self[date(year, JUN, 20) + rd(weekday=SA)] = "Midsommardagen"
        else:
            self[date(year, JUN, 24)] = "Midsommardagen"
            # All saints day. Friday between October 31th and November 6th
        self[date(year, OCT, 31) + rd(weekday=SA)] = "Alla helgons dag"

        if year <= 1953:
            self[date(year, MAR, 25)] = "Jungfru Marie bebådelsedag"


class SE(Sweden):
    pass


class SWE(Sweden):
    pass
