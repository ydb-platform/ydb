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

from dateutil.easter import easter, EASTER_ORTHODOX
from dateutil.relativedelta import relativedelta as rd, WE

from holidays.constants import JAN, MAR, MAY, AUG, OCT, DEC
from holidays.holiday_base import HolidayBase


class Greece(HolidayBase):
    # https://en.wikipedia.org/wiki/Public_holidays_in_Greece

    def __init__(self, **kwargs):
        self.country = 'GR'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        eday = easter(year, method=EASTER_ORTHODOX)

        # New Years
        self[date(year, JAN, 1)] = "Πρωτοχρονιά [New Year's Day]"
        # Epiphany
        self[date(year, JAN, 6)] = "Θεοφάνεια [Epiphany]"

        # Clean Monday
        self[eday - rd(days=48)] = "Καθαρά Δευτέρα [Clean Monday]"

        # Independence Day
        self[date(year, MAR, 25)] = "Εικοστή Πέμπτη Μαρτίου [Independence Day]"

        # Easter Monday
        self[eday + rd(days=1)] = "Δευτέρα του Πάσχα [Easter Monday]"

        # Labour Day
        self[date(year, MAY, 1)] = "Εργατική Πρωτομαγιά [Labour day]"

        # Monday of the Holy Spirit
        self[eday + rd(days=50)] = \
            "Δευτέρα του Αγίου Πνεύματος [Monday of the Holy Spirit]"

        # Assumption of Mary
        self[date(year, AUG, 15)] = "Κοίμηση της Θεοτόκου [Assumption of Mary]"

        # Ochi Day
        self[date(year, OCT, 28)] = "Ημέρα του Όχι [Ochi Day]"

        # Christmas
        self[date(year, DEC, 25)] = "Χριστούγεννα [Christmas]"

        # Day after Christmas
        self[date(year, DEC, 26)] = \
            "Επόμενη ημέρα των Χριστουγέννων [Day after Christmas]"


class GR(Greece):
    pass


class GRC(Greece):
    pass
