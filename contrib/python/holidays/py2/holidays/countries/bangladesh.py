# -*- coding: utf-8 -*-

#  python-holidays
#  ---------------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Author:  tasnim<tasnimislam1999@gmail.com>
#  ISO code = BD

from datetime import date
from holidays.constants import FEB, MAR, APR, MAY, AUG, DEC
from holidays.holiday_base import HolidayBase


class Bangladesh(HolidayBase):
    # https://mopa.gov.bd/sites/default/files/files/mopa.gov.bd/public_holiday/61c35b73_e335_462a_9bcf_4695b23b6d82/reg4-2019-212.PDF
    # https://en.wikipedia.org/wiki/Public_holidays_in_Bangladesh

    def __init__(self, **kwargs):
        self.country = 'TR'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        # 21st Feb
        self[date(year, FEB, 21)] = "International Mother's language Day"

        # 17th March
        self[date(year, MAR, 17)] = "Sheikh Mujibur Rahman's Birthday " \
                                    "and Children's Day"

        # 26th March
        self[date(year, MAR, 26)] = "Independence Day"

        # 14th April
        self[date(year, APR, 14)] = "Bengali New Year's Day"

        # 1st May
        self[date(year, MAY, 1)] = "May Day"

        # 15th AUG
        self[date(year, AUG, 15)] = "National Mourning Day"

        # 16th Dec
        self[date(year, DEC, 16)] = "Victory Day"


class BD(Bangladesh):
    pass


class BGD(Bangladesh):
    pass
