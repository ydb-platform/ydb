# -*- coding: utf-8 -*-

#  python-holidays
#  ---------------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Author:      ryanss <ryanssdev@icloud.com> (c) 2014-2017
#               dr-prodigy <maurizio.montel@gmail.com> (c) 2017-2020
#  Contributor: rolandinsh <rolands@mediabox.lv> (c) 2020
#  Website:     https://github.com/rolandinsh/python-holidays
#  License:     MIT (see LICENSE file)

from datetime import date

from dateutil.easter import easter
from dateutil.relativedelta import relativedelta as rd, SU

from holidays.holiday_base import HolidayBase


class Latvia(HolidayBase):

    # https://en.wikipedia.org/wiki/Public_holidays_in_Latvia
    # https://information.lv/

    def __init__(self, **kwargs):
        self.country = "LV"
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):
        # New Year's Day
        self[date(year, 1, 1)] = "Jaunais gads"

        # Good Friday
        easter_date = easter(year)
        self[easter_date - rd(days=2)] = "Lielā Piektdiena"

        # Easter
        easter_date = easter(year)
        self[easter_date] = "Lieldienas"

        # Easter 2nd day
        self[easter_date + rd(days=1)] = "Otrās Lieldienas"

        # International Workers' Day
        self[date(year, 5, 1)] = "Darba svētki"

        # Restoration of Independence Day (1990).
        # Latvia proclaimed its independence from the USSR,
        #   and restoration of the Republic of Latvia.
        if year >= 1990:
            self[date(year, 5, 4)] = "Latvijas Republikas \
                Neatkarības atjaunošanas diena"

        # by law
        # https://likumi.lv/ta/id/72608-par-svetku-atceres-un-atzimejamam-dienam
        # Midsummer's Eve
        if year >= 1990:
            self[date(year, 6, 23)] = "Līgo diena"

        # Midsummer's Day
        # Also so called "St. John's Day"
        if year >= 1990:
            self[date(year, 6, 24)] = "Jāņu dienu"

        # Proclamation Day of the Republic of Latvia
        if year >= 1918:
            self[date(year, 11, 18)] = \
                "Latvijas Republikas proklamēšanas diena"

        # Christmas Eve
        self[date(year, 12, 24)] = "Ziemassvētku vakars"

        # Christmas 1st day
        self[date(year, 12, 25)] = "Ziemassvētki"

        # Christmas 2nd day
        self[date(year, 12, 26)] = "Otrie Ziemassvētki"

        # New Year's Eve
        self[date(year, 12, 31)] = "Vecgada vakars"


class LV(Latvia):
    pass


class LVA(Latvia):
    pass
