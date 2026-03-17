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
from dateutil.relativedelta import relativedelta as rd
from holidays.constants import FRI, SAT
from holidays.constants import JAN, APR, MAY, JUN, JUL, OCT
from holidays.holiday_base import HolidayBase
from holidays.utils import get_gre_date

WEEKEND = (FRI, SAT)


class Egypt(HolidayBase):

    # Holidays here are estimates, it is common for the day to be pushed
    # if falls in a weekend, although not a rule that can be implemented.
    # Holidays after 2020: the following four moving date holidays whose exact
    # date is announced yearly are estimated (and so denoted):
    # - Eid El Fetr*
    # - Eid El Adha*
    # - Arafat Day*
    # - Moulad El Naby*
    # *only if hijri-converter library is installed, otherwise a warning is
    #  raised that this holiday is missing. hijri-converter requires
    #  Python >= 3.6
    # is_weekend function is there, however not activated for accuracy.

    def __init__(self, **kwargs):
        self.country = 'EG'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        """
        # Function to store the holiday name in the appropriate
        # date and to shift the Public holiday in case it happens
        # on a Saturday(Weekend)
        # (NOT USED)
        def is_weekend(self, hol_date, hol_name):
            if hol_date.weekday() == FRI:
                self[hol_date] = hol_name + " [Friday]"
                self[hol_date + rd(days=+2)] = "Sunday following " + hol_name
            else:
                self[hol_date] = hol_name
        """

        # New Year's Day
        self[date(year, JAN, 1)] = "New Year's Day - Bank Holiday"

        # Coptic Christmas
        self[date(year, JAN, 7)] = "Coptic Christmas"

        # 25th of Jan
        if year >= 2012:
            self[date(year, JAN, 25)] = "Revolution Day - January 25"
        elif year >= 2009:
            self[date(year, JAN, 25)] = "Police Day"
        else:
            pass

        # Coptic Easter - Orthodox Easter
        self[easter(year, 2)] = "Coptic Easter Sunday"

        # Sham El Nessim - Spring Festival
        self[easter(year, 2) + rd(days=1)] = "Sham El Nessim"

        # Sinai Libration Day
        if year > 1982:
            self[date(year, APR, 25)] = "Sinai Liberation Day"

        # Labour Day
        self[date(year, MAY, 1)] = "Labour Day"

        # Armed Forces Day
        self[date(year, OCT, 6)] = "Armed Forces Day"

        # 30 June Revolution Day
        if year >= 2014:
            self[date(year, JUN, 30)] = "30 June Revolution Day"

        # Revolution Day
        if year > 1952:
            self[date(year, JUL, 23)] = "Revolution Day"

        # Eid al-Fitr - Feast Festive
        # date of observance is announced yearly, This is an estimate since
        # having the Holiday on Weekend does change the number of days,
        # deceided to leave it since marking a Weekend as a holiday
        # wouldn't do much harm.
        for date_obs in get_gre_date(year, 10, 1):
            hol_date = date_obs
            self[hol_date] = "Eid al-Fitr"
            self[hol_date + rd(days=1)] = "Eid al-Fitr Holiday"
            self[hol_date + rd(days=2)] = "Eid al-Fitr Holiday"

        # Arafat Day & Eid al-Adha - Scarfice Festive
        # date of observance is announced yearly
        for date_obs in get_gre_date(year, 12, 9):
            hol_date = date_obs
            self[hol_date] = "Arafat Day"
            self[hol_date + rd(days=1)] = "Eid al-Adha"
            self[hol_date + rd(days=2)] = "Eid al-Adha Holiday"
            self[hol_date + rd(days=3)] = "Eid al-Adha Holiday"

        # Islamic New Year - (hijari_year, 1, 1)
        for date_obs in get_gre_date(year, 1, 1):
            hol_date = date_obs
            self[hol_date] = "Islamic New Year"

        # Prophet Muhammad's Birthday - (hijari_year, 3, 12)
        for date_obs in get_gre_date(year, 3, 12):
            hol_date = date_obs
            self[hol_date] = "Prophet Muhammad's Birthday"


class EG(Egypt):
    pass


class EGY(Egypt):
    pass
