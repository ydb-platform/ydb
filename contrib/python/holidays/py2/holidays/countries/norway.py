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

from holidays.constants import JAN, MAY, DEC
from holidays.constants import MON, THU, FRI, SUN
from holidays.holiday_base import HolidayBase


class Norway(HolidayBase):
    """
    Norwegian holidays.
    Note that holidays falling on a sunday is "lost",
    it will not be moved to another day to make up for the collision.

    In Norway, ALL sundays are considered a holiday (https://snl.no/helligdag).
    Initialize this class with include_sundays=False
    to not include sundays as a holiday.

    Primary sources:
    https://lovdata.no/dokument/NL/lov/1947-04-26-1
    https://no.wikipedia.org/wiki/Helligdager_i_Norge
    https://www.timeanddate.no/merkedag/norge/
    """

    def __init__(self, include_sundays=True, **kwargs):
        """

        :param include_sundays: Whether to consider sundays as a holiday
        (which they are in Norway)
        :param kwargs:
        """
        self.country = "NO"
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

                self[cur_date] = "Søndag"
                cur_date += rd(days=7)

        # ========= Static holidays =========
        self[date(year, JAN, 1)] = "Første nyttårsdag"

        # Source: https://lovdata.no/dokument/NL/lov/1947-04-26-1
        if year >= 1947:
            self[date(year, MAY, 1)] = "Arbeidernes dag"
            self[date(year, MAY, 17)] = "Grunnlovsdag"

        # According to https://no.wikipedia.org/wiki/F%C3%B8rste_juledag,
        # these dates are only valid from year > 1700
        # Wikipedia has no source for the statement, so leaving this be for now
        self[date(year, DEC, 25)] = "Første juledag"
        self[date(year, DEC, 26)] = "Andre juledag"

        # ========= Moving holidays =========
        # NOTE: These are probably subject to the same > 1700
        # restriction as the above dates. The only source I could find for how
        # long Easter has been celebrated in Norway was
        # https://www.hf.uio.no/ikos/tjenester/kunnskap/samlinger/norsk-folkeminnesamling/livs-og-arshoytider/paske.html
        # which says
        # "(...) has been celebrated for over 1000 years (...)" (in Norway)
        e = easter(year)
        maundy_thursday = e - rd(days=3)
        good_friday = e - rd(days=2)
        resurrection_sunday = e
        easter_monday = e + rd(days=1)
        ascension_thursday = e + rd(days=39)
        pentecost = e + rd(days=49)
        pentecost_day_two = e + rd(days=50)

        assert maundy_thursday.weekday() == THU
        assert good_friday.weekday() == FRI
        assert resurrection_sunday.weekday() == SUN
        assert easter_monday.weekday() == MON
        assert ascension_thursday.weekday() == THU
        assert pentecost.weekday() == SUN
        assert pentecost_day_two.weekday() == MON

        self[maundy_thursday] = "Skjærtorsdag"
        self[good_friday] = "Langfredag"
        self[resurrection_sunday] = "Første påskedag"
        self[easter_monday] = "Andre påskedag"
        self[ascension_thursday] = "Kristi himmelfartsdag"
        self[pentecost] = "Første pinsedag"
        self[pentecost_day_two] = "Andre pinsedag"


class NO(Norway):
    pass


class NOR(Norway):
    pass
