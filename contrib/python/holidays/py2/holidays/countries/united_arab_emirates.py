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

from dateutil.relativedelta import relativedelta as rd
from holidays.constants import FRI, SAT
from holidays.constants import JAN, APR, MAY, JUN, JUL, AUG, SEP, NOV, DEC
from holidays.holiday_base import HolidayBase
from holidays.utils import get_gre_date

WEEKEND = (FRI, SAT)


class UnitedArabEmirates(HolidayBase):

    # Holidays are regulated by the Article 74
    # of Federal Law No. 08 for the year 1980:
    # https://www.ilo.org/dyn/natlex/docs/ELECTRONIC/11956/69376/F417089305/ARE11956.pdf
    # However the law is not applied literally,
    # and was amended often in the past few years.
    # Sources:
    # 2017: https://www.khaleejtimes.com/nation/uae-official-public-holidays-list-2017   # noqa: E501
    # 2018: https://www.thenational.ae/uae/government/uae-public-holidays-2018-announced-by-abu-dhabi-government-1.691393  # noqa: E501
    # 2019: https://www.thenational.ae/uae/government/uae-public-holidays-for-2019-and-2020-announced-by-cabinet-1.833425  # noqa: E501
    # 2020: https://u.ae/en/information-and-services/public-holidays-and-religious-affairs/public-holidays  # noqa: E501

    # Holidays based on the Islamic Calendar are estimated (and so denoted),
    # as are announced each year and based on moon sightings:
    # - Eid al-Fitr*
    # - Eid al-Adha*
    # - Arafat (Hajj) Day*
    # - Al-Hijra (Islamic New Year)*
    # - Mawlud al-Nabi (Prophet Mohammad's Birthday)*
    # - Leilat al-Miraj (Ascension of the Prophet)*, suspended after 2018.
    # *only if hijri-converter library is installed, otherwise a warning is
    #  raised that this holiday is missing. hijri-converter requires
    #  Python >= 3.6

    def __init__(self, **kwargs):
        self.country = 'AE'
        HolidayBase.__init__(self, **kwargs)

    def _populate(self, year):

        # New Year's Day
        self[date(year, JAN, 1)] = "New Year's Day"

        # Commemoration Day, since 2015.
        if (year >= 2015 and year < 2019):
            self[date(year, NOV, 30)] = "Commemoration Day"
        elif year >= 2019:
            self[date(year, DEC, 1)] = "Commemoration Day"
        else:
            pass

        # National Day
        self[date(year, DEC, 2)] = "National Day"
        self[date(year, DEC, 3)] = "National Day Holiday"

        # Eid al-Fitr
        # Date is announced each year. Usually stretches along 3 or 4 days,
        # in some instances prepending/appending a day or two
        # before/after the official holiday.
        dates_obs = {2017: [(JUN, 25)], 2018: [(JUN, 14)], 2019: [(JUN, 3)],
                     2020: [(MAY, 24)]}
        fitr = "Eid al-Fitr"
        if year in dates_obs:
            for date_obs in dates_obs[year]:
                hol_date = date(year, *date_obs)
                self[hol_date] = fitr
                self[hol_date + rd(days=1)] = '{} Holiday'.format(fitr)
                self[hol_date + rd(days=2)] = '{} Holiday'.format(fitr)
        else:
            for date_obs in get_gre_date(year, 10, 1):
                hol_date = date_obs
                self[hol_date] = '{}* (*estimated)'.format(fitr)
                self[hol_date + rd(days=1)] =\
                    '{} Holiday* (*estimated)'.format(fitr)
                self[hol_date + rd(days=2)] =\
                    '{} Holiday* (*estimated)'.format(fitr)

        # Arafat Day & Eid al-Adha
        dates_obs = {2017: [(AUG, 31)], 2018: [(AUG, 20)], 2019: [(AUG, 10)],
                     2020: [(JUL, 30)]}
        hajj = "Arafat (Hajj) Day"
        adha = "Eid al-Adha"
        if year in dates_obs:
            for date_obs in dates_obs[year]:
                hol_date = date(year, *date_obs)
                self[hol_date] = hajj
                self[hol_date + rd(days=1)] = adha
                self[hol_date + rd(days=2)] = '{} Holiday'.format(adha)
                self[hol_date + rd(days=3)] = '{} Holiday'.format(adha)
        else:
            for date_obs in get_gre_date(year, 12, 9):
                hol_date = date_obs
                self[hol_date] = '{}* (*estimated)'.format(hajj)
                self[hol_date + rd(days=1)] = '{}* (*estimated)'.format(adha)
                self[hol_date + rd(days=2)] =\
                    '{}* Holiday* (*estimated)'.format(adha)
                self[hol_date + rd(days=3)] =\
                    '{} Holiday* (*estimated)'.format(adha)

        # Islamic New Year - (hijari_year, 1, 1)
        dates_obs = {2017: [(SEP, 22)], 2018: [(SEP, 11)], 2019: [(AUG, 31)],
                     2020: [(AUG, 23)]}
        new_hijri_year = "Al Hijra - Islamic New Year"
        if year in dates_obs:
            for date_obs in dates_obs[year]:
                hol_date = date(year, *date_obs)
                self[hol_date] = new_hijri_year
        else:
            for date_obs in get_gre_date(year, 1, 1):
                hol_date = date_obs
                self[hol_date] = '{}* (*estimated)'.format(new_hijri_year)

        # Leilat al-Miraj - The Prophet's ascension (hijari_year, 7, 27)
        if year <= 2018:  # starting from 2019 the UAE government removed this
            dates_obs = {2017: [(APR, 23)], 2018: [(APR, 13)]}
            ascension = "Leilat al-Miraj - The Prophet's ascension"
            if year in dates_obs:
                for date_obs in dates_obs[year]:
                    hol_date = date(year, *date_obs)
                    self[hol_date] = ascension
            else:
                for date_obs in get_gre_date(year, 7, 27):
                    hol_date = date_obs
                    self[hol_date] = '{}* (*estimated)'.format(ascension)

        # Prophet Muhammad's Birthday - (hijari_year, 3, 12)
        if year <= 2019:  # starting from 2020 the UAE government removed this
            dates_obs = {2017: [(NOV, 30)], 2018: [(NOV, 19)],
                         2019: [(NOV, 9)]}
            mawlud = "Mawlud al-Nabi - Prophet Mohammad's Birthday"
            if year in dates_obs:
                for date_obs in dates_obs[year]:
                    hol_date = date(year, *date_obs)
                    self[hol_date] = mawlud
            else:
                for date_obs in get_gre_date(year, 3, 12):
                    hol_date = date_obs
                    self[hol_date] = '{}* (*estimated)'.format(mawlud)


class AE(UnitedArabEmirates):
    pass


class ARE(UnitedArabEmirates):
    pass
