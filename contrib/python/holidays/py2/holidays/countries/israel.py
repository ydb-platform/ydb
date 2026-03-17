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


from convertdate import hebrew
from datetime import date
from dateutil.relativedelta import relativedelta as rd
from holidays.holiday_base import HolidayBase


class Israel(HolidayBase):
    def __init__(self, **kwargs):
        self.country = 'IL'

        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        is_leap_year = hebrew.leap(year + hebrew.HEBREW_YEAR_OFFSET)

        # Passover
        name = "Passover I"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.NISAN, 14)
        passover_start_dt = date(year, month, day)
        self[passover_start_dt] = name + ' - Eve'
        self[passover_start_dt + rd(days=1)] = name

        name = 'Passover'
        for offset in range(2, 6):
            self[passover_start_dt + rd(days=offset)] = \
                name + ' - Chol HaMoed'

        name = "Passover VII"
        self[passover_start_dt + rd(days=6)] = name + ' - Eve'
        self[passover_start_dt + rd(days=7)] = name

        # Memorial Day
        name = "Memorial Day"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.IYYAR, 3)
        self[date(year, month, day) + rd(days=1)] = name

        observed_delta = 0
        if self.observed:
            day_in_week = date(year, month, day).weekday()
            if day_in_week in (2, 3):
                observed_delta = - (day_in_week - 1)
            elif 2004 <= year and day_in_week == 5:
                observed_delta = 1

            if observed_delta != 0:
                self[date(year, month, day) + rd(days=observed_delta + 1)] = \
                    name + " (Observed)"

        # Independence Day
        name = "Independence Day"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.IYYAR, 4)
        self[date(year, month, day) + rd(days=1)] = name

        if self.observed and observed_delta != 0:
            self[date(year, month, day) + rd(days=observed_delta + 1)] = \
                name + " (Observed)"

        # Lag Baomer
        name = "Lag B'Omer"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.IYYAR, 18)
        self[date(year, month, day)] = name

        # Shavuot
        name = "Shavuot"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.SIVAN, 5)
        self[date(year, month, day)] = name + " - Eve"
        self[date(year, month, day) + rd(days=1)] = name

        # Rosh Hashana
        name = "Rosh Hashanah"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.ELUL, 29)
        self[date(year, month, day)] = name + " - Eve"
        self[date(year, month, day) + rd(days=1)] = name
        self[date(year, month, day) + rd(days=2)] = name

        # Yom Kippur
        name = "Yom Kippur"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.TISHRI, 9)
        self[date(year, month, day)] = name + ' - Eve'
        self[date(year, month, day) + rd(days=1)] = name

        # Sukkot
        name = "Sukkot I"
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.TISHRI, 14)
        sukkot_start_dt = date(year, month, day)
        self[sukkot_start_dt] = name + ' - Eve'
        self[sukkot_start_dt + rd(days=1)] = name

        name = 'Sukkot'
        for offset in range(2, 7):
            self[sukkot_start_dt + rd(days=offset)] = name + ' - Chol HaMoed'

        name = "Sukkot VII"
        self[sukkot_start_dt + rd(days=7)] = name + ' - Eve'
        self[sukkot_start_dt + rd(days=8)] = name

        # Hanukkah
        name = 'Hanukkah'
        year, month, day = hebrew.to_jd_gregorianyear(year, hebrew.KISLEV, 25)
        for offset in range(8):
            self[date(year, month, day) + rd(days=offset)] = name

        # Purim
        name = 'Purim'
        heb_month = hebrew.VEADAR if is_leap_year else hebrew.ADAR
        year, month, day = hebrew.to_jd_gregorianyear(year, heb_month, 14)
        self[date(year, month, day)] = name

        self[date(year, month, day) - rd(days=1)] = name + ' - Eve'

        name = 'Shushan Purim'
        self[date(year, month, day) + rd(days=1)] = name


class IL(Israel):
    pass


class ISR(Israel):
    pass
