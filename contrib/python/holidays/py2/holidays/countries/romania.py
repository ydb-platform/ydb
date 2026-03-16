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
from dateutil.relativedelta import relativedelta as rd

from holidays.constants import JAN, MAY, JUN, AUG, NOV, DEC
from holidays.holiday_base import HolidayBase


class Romania(HolidayBase):
    # https://en.wikipedia.org/wiki/Public_holidays_in_Romania

    def __init__(self, **kwargs):
        self.country = 'RO'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        eday = easter(year, method=EASTER_ORTHODOX)

        # New Year
        for day in [1, 2]:
            self[date(year, JAN, day)] = "Anul Nou"

        # Anniversary of the formation of the United Principalities
        self[date(year, JAN, 24)] = "Unirea Principatelor Române"

        # Easter (Friday, Sunday and Monday)
        for day_after_easter in [-2, 0, 1]:
            self[eday + rd(days=day_after_easter)] = "Paștele"

        # Labour Day
        self[date(year, MAY, 1)] = "Ziua Muncii"

        # Children's Day (since 2017)
        if year >= 2017:
            self[date(year, JUN, 1)] = "Ziua Copilului"

        # Whit Monday
        for day_after_easter in [49, 50]:
            self[eday + rd(days=day_after_easter)] = "Rusaliile"

        # Assumption of Mary
        self[date(year, AUG, 15)] = "Adormirea Maicii Domnului"

        # St. Andrew's Day
        self[date(year, NOV, 30)] = "Sfântul Andrei"

        # National Day
        self[date(year, DEC, 1)] = "Ziua Națională a României"

        # Christmas Day
        self[date(year, DEC, 25)] = "Crăciunul"
        self[date(year, DEC, 26)] = "Crăciunul"


class RO(Romania):
    pass


class ROU(Romania):
    pass
