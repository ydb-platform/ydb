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
from holidays.constants import JAN, MAY, JUN
from holidays.holiday_base import HolidayBase
from holidays.utils import get_gre_date

WEEKEND = (FRI, SAT)

# Since Djibouti share most of it's holidays with other muslim countries,
# this class is just a copy of Egypt's.


class Djibouti(HolidayBase):

    # Holidays here are estimates, it is common for the day to be pushed
    # if falls in a weekend, although not a rule that can be implemented.
    # Holidays after 2020: the following four moving date holidays whose exact
    # date is announced yearly are estimated (and so denoted):
    # - Eid El Fetr*
    # - Eid El Adha*
    # - Isra wal Miraj*
    # - Moulad El Naby*
    # - Arafat*
    # *only if hijri-converter library is installed, otherwise a warning is
    #  raised that this holiday is missing. hijri-converter requires
    #  Python >= 3.6
    # is_weekend function is there, however not activated for accuracy.

    def __init__(self, **kwargs):
        self.country = 'DJ'
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
        self[date(year, JAN, 1)] = "Nouvel an"

        # Labour Day
        self[date(year, MAY, 1)] = "Fête du travail"

        # Fête de l'indépendance
        self[date(year, JUN, 27)] = "Fête de l'indépendance"
        self[date(year, JUN, 28)] = "Fête de l'indépendance"

        # Isra wal Miraj
        # The night journey of the prophet Muhammad
        for date_obs in get_gre_date(year, 7, 27):
            hol_date = date_obs
            self[hol_date] = "Isra wal Miraj"

        # Eid al-Fitr - Feast Festive
        # date of observance is announced yearly, This is an estimate since
        # having the Holiday on Weekend does change the number of days,
        # deceided to leave it since marking a Weekend as a holiday
        # wouldn't do much harm.
        for date_obs in get_gre_date(year, 10, 1):
            hol_date = date_obs
            self[hol_date] = "Eid al-Fitr"
            self[hol_date + rd(days=1)] = "Eid al-Fitr deuxième jour"

        # Arafat & Eid al-Adha - Scarfice Festive
        # date of observance is announced yearly
        for date_obs in get_gre_date(year, 12, 9):
            hol_date = date_obs
            self[hol_date] = "Arafat"
            self[hol_date + rd(days=1)] = "Eid al-Adha"
            self[hol_date + rd(days=2)] = "Eid al-Adha deuxième jour"

        # Islamic New Year - (hijari_year, 1, 1)
        for date_obs in get_gre_date(year, 1, 1):
            hol_date = date_obs
            self[hol_date] = "Nouvel an musulman"

        # Prophet Muhammad's Birthday - (hijari_year, 3, 12)
        for date_obs in get_gre_date(year, 3, 12):
            hol_date = date_obs
            self[hol_date] = "Naissance du prophet Muhammad"


class DJ(Djibouti):
    pass


class DJI(Djibouti):
    pass
