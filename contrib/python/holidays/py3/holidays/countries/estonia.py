#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

from gettext import gettext as tr

from holidays.constants import HALF_DAY, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Estonia(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Estonia holidays.

    References:
        * <https://et.wikipedia.org/wiki/Eesti_riigipühad>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Estonia>
        * [Decree on State Holidays and on Amending the Labour Code of the Estonian SSR (09.10.1990)](https://web.archive.org/web/20260117043713/https://www.riigiteataja.ee/akt/28395)
        * [Holidays and Anniversaries Act (08.02.1994)](https://web.archive.org/web/20250122175021/https://www.riigiteataja.ee/akt/29913)
        * [Public Holidays and Days of National Importance Act (27.01.1998, in force since 23.02.1998)](https://web.archive.org/web/20260108041502/https://www.riigiteataja.ee/akt/13276841)
        * [Act amending § 2 of the Holidays and Anniversaries Act
            and § 25 of the Working and Rest Hours Act](https://web.archive.org/web/20250815032316/https://www.riigiteataja.ee/akt/895511)
        * [Working and Rest Hours Act (24.01.2001)](https://web.archive.org/web/20250429084533/https://www.riigiteataja.ee/akt/72738)
        * [Employment Contracts Act (17.12.2008, in force since 01.07.2009)](https://web.archive.org/web/20250520105700/https://www.riigiteataja.ee/akt/122122012029)
    """

    country = "EE"
    default_language = "et"
    # Decree on State Holidays and on Amending the Labour Code of the Estonian SSR (09.10.1990).
    start_year = 1991
    supported_categories = (HALF_DAY, PUBLIC)
    supported_languages = ("en_US", "et", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("uusaasta"))

        # Independence Day.
        self._add_holiday_feb_24(tr("iseseisvuspäev"))

        if self._year >= 1994:
            # Good Friday.
            self._add_good_friday(tr("suur reede"))

            # Easter Sunday.
            self._add_easter_sunday(tr("ülestõusmispühade 1. püha"))

            # Whit Sunday.
            self._add_whit_sunday(tr("nelipühade 1. püha"))

        self._add_holiday_may_1(
            # May Day.
            tr("kevadpüha")
            if self._year >= 1994
            # International Workers' Solidarity Day.
            else tr("töörahva rahvusvahelise solidaarsuse päev")
        )

        # Victory Day.
        self._add_holiday_jun_23(tr("võidupüha"))

        # Midsummer Day.
        self._add_saint_johns_day(tr("jaanipäev"))

        if self._year >= 1998:
            # Independence Restoration Day.
            self._add_holiday_aug_20(tr("taasiseseisvumispäev"))

        if self._year <= 1993:
            # Day of Declaration of Sovereignty.
            self._add_holiday_nov_16(tr("taassünnipäev"))

        # Act amending § 2 of the Holidays and Anniversaries Act
        # and § 25 of the Working and Rest Hours Act.
        if self._year >= 2005:
            # Christmas Eve.
            self._add_christmas_eve(tr("jõululaupäev"))

        # Christmas Day.
        self._add_christmas_day(tr("esimene jõulupüha"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("teine jõulupüha"))

    def _populate_half_day_holidays(self):
        # Working and Rest Hours Act (24.01.2001), § 25.
        # Employment Contracts Act (17.12.2008, in force since 01.07.2009), § 53.

        if self._year <= 2000:
            return None

        # Pre-holiday day (workday shortened by 3 hours).
        name = tr("pühade-eelne päev (tööpäev lüheneb 3 tunni võrra)")
        self._add_holiday_feb_23(name)
        self._add_holiday_jun_22(name)
        self._add_holiday_dec_31(name)

        if self._year >= 2005:
            self._add_holiday_dec_23(name)
        else:
            self._add_holiday_dec_24(name)


class EE(Estonia):
    pass


class EST(Estonia):
    pass
