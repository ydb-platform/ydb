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

from datetime import date

from holidays.calendars.gregorian import JAN, APR, MAY, JUN, JUL, SEP, OCT, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_WORKDAY,
    SUN_TO_NEXT_WORKDAY,
)


class Guernsey(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Guernsey holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Guernsey>
        * <https://web.archive.org/web/20230930101458/https://guernseylegalresources.gg/ordinances/guernsey-bailiwick/p/public-holidays/>
        * <https://web.archive.org/web/20250407162243/http://www.thegazette.co.uk/all-notices>

    Checked with:
        * <https://web.archive.org/web/20250118084112/https://gov.gg/holidaydates>

    His/Her Majesty's Birthday pre-1946 is cross-checked with The London Gazette's Record,
    Specifically as "Home Station" entry under King's Birthday declaration lists.

    Since 1955, if a bank holiday is on a sunday, a substitute weekday becomes a bank holiday,
    normally the following Monday. From 2009 onwards this also applies to saturday as well.
    """

    country = "GG"
    observed_label = "%s (substitute day)"
    # Ordonnance relative aux Jours Fériés, 1909.
    start_year = 1909

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, GuernseyStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 1955)
        ObservedHolidayBase.__init__(self, *args, **kwargs)

    def _add_observed(self, dt: date, **kwargs) -> tuple[bool, date | None]:
        # Prior to 2009, in-lieu are only given for Sundays.
        # https://web.archive.org/web/20230930101652/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55179
        kwargs.setdefault(
            "rule", SUN_TO_NEXT_WORKDAY if dt < date(2009, DEC, 15) else self._observed_rule
        )
        return super()._add_observed(dt, **kwargs)

    def _populate_public_holidays(self) -> None:
        # New Year's Day
        # Le jour de l'An

        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Good Friday
        # Vendredi Saint

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday
        # Le Lundi de Pâques

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # May Day
        # (No official Guernésiais translation available)
        # Started in 1980 for Guernsey.
        # Moved to May 8 specifically for 1995 and 2020.

        if self._year >= 1980:
            # May Day Bank Holiday
            name = "May Day Bank Holiday"
            if self._year in {1995, 2020}:
                self._add_holiday_may_8(name)
            else:
                self._add_holiday_1st_mon_of_may(name)

        # Spring bank holiday
        # (No official Guernésiais translation available)
        # Replaced Whit Monday in 1967, 1970, 1973, 1974, 1975, 1976.
        # No observance in 1977-1978. Properly established from 1979 onwards.
        # Moved for Queen Elizabeth II's Jubilee Dates in 2002, 2012, and 2022.

        if self._year in {1967, 1970} or 1973 <= self._year <= 1976 or self._year >= 1979:
            spring_bank_dates = {
                2002: (JUN, 4),
                2012: (JUN, 4),
                2022: (JUN, 2),
            }
            # Spring Bank Holiday
            name = "Spring Bank Holiday"
            if dt := spring_bank_dates.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_holiday_last_mon_of_may(name)

        # Whit Monday
        # Le lundi de Pentecôte
        # Replaced by Spring Bank Holiday in 1967, 1970, 1973, 1974, 1975, 1976.
        # Fully Removed in 1977.

        if self._year <= 1966 or self._year in {1968, 1969, 1971, 1972}:
            # Whit Monday.
            self._add_whit_monday("Whit Monday")

        # His/Her Majesty's Birthday
        # La jour fixé pour la célébration du jour de naissanee de Sa Majesté
        # Special Non-Observance Decree was issued in 1940.
        # This was fully replaced by Liberation Day in 1947.

        if self._year <= 1946:
            his_majesty_birth_dates = {
                # Edward VII (MAY/JUN observance).
                1909: (JUN, 25),  # (NOV, 9) for overseas.
                # George V.
                1910: (JUN, 24),  # (NOV, 9) for overseas; No decree cancelled Edward VII ones.
                1911: (MAY, 27),  # (JUN, 3) for overseas.
                1912: (JUN, 14),  # (JUN, 3) for overseas.
                1913: (JUN, 3),
                1914: (JUN, 24),  # Specifically held on the 24th on Guernsey, is 22nd elsewhere.
                # Not observed in 1915-1918 due to WW1.
                1919: (JUN, 3),
                1920: (JUN, 5),  # (JUN, 3) for overseas.
                1921: (JUN, 4),  # (JUN, 3) for overseas.
                1922: (JUN, 3),
                1923: (JUN, 2),
                1924: (JUN, 3),
                1925: (JUN, 3),
                1926: (JUN, 5),
                1927: (JUN, 3),
                1928: (JUN, 4),
                1929: (JUN, 3),
                1930: (JUN, 3),
                1931: (JUN, 3),
                1932: (JUN, 3),
                1933: (JUN, 3),
                1934: (JUN, 4),
                1935: (JUN, 3),
                # Edward VIII.
                1936: (JUN, 23),
                # George VI.
                1937: (JUN, 9),
                1938: (JUN, 9),
                1939: (JUN, 8),
                # Not observed in 1940-1941 due to WW2.
                # Although this was only de jure cancelled in 1940 for Guernsey.
                1942: (JUN, 11),
                1943: (JUN, 2),
                1944: (JUN, 8),
                1945: (JUN, 14),
                1946: (JUN, 13),
            }
            if dt := his_majesty_birth_dates.get(self._year):
                # His Majesty's Birthday.
                self._add_holiday("His Majesty's Birthday", dt)

        # Summer Bank Holiday
        # Le premier Lundi du mois d'Août
        # Current Pattern started in 1965. Was initially on first Monday of August for Guernsey.
        # Briefly as first Monday of September between 1968-1969 and no observance for 1977-1978.

        # Summer Bank Holiday.
        summer_bank_holiday = "Summer Bank Holiday"
        if self._year <= 1976 or self._year >= 1979:
            if 1968 <= self._year <= 1969:
                self._add_holiday_1st_mon_of_sep(summer_bank_holiday)
            elif self._year >= 1965:
                self._add_holiday_last_mon_of_aug(summer_bank_holiday)
            else:
                self._add_holiday_1st_mon_of_aug(summer_bank_holiday)

        # Christmas Day
        # Le jour de Noël

        # Christmas Day
        christmas_day = self._add_christmas_day("Christmas Day")

        # Boxing Day
        # Le premier jour ouvrier après le jour de Noël

        # Boxing Day
        boxing_day = self._add_christmas_day_two("Boxing Day")

        self._add_observed(christmas_day)
        self._add_observed(boxing_day)

        # Guernsey exclusive holidays

        # Liberation Day
        # Le jour fixé par les Etats pour la célébration de la Libération de cette Ile
        # de l'occupation allemande par les Forces Britannique
        # Started in 1946 for Guernsey. Fully replaced HM Birthdays in 1947. This has no in-lieus.
        # Specifically moved to May 10 in 2010.
        # Unlike Jersey this wasn't moved in 2021.

        if self._year >= 1946:
            liberation_dates = {
                2010: (MAY, 10),
            }
            # Liberation Day.
            name = "Liberation Day"
            if dt := liberation_dates.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_holiday_may_9(name)


class GG(Guernsey):
    pass


class GGY(Guernsey):
    pass


class GuernseyStaticHolidays:
    """Guernsey special holidays.

    References:
        * <https://web.archive.org/web/20250118084112/https://gov.gg/holidaydates>
        * <https://web.archive.org/web/20250427182440/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=52182>
        * <https://web.archive.org/web/20250427182440/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=52183>
        * <https://web.archive.org/web/20250427182440/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=52184>
        * <https://web.archive.org/web/20250427182949/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=52185>
        * <https://web.archive.org/web/20250427183011/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=52188>
        * <https://web.archive.org/web/20250427183015/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=52189>
        * <https://web.archive.org/web/20250427183051/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=52631>
        * <https://web.archive.org/web/20230930101410/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55175>
        * <https://web.archive.org/web/20250427183222/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55176>
        * <https://web.archive.org/web/20250427183057/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55177>
        * <https://web.archive.org/web/20230930101652/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55179>
        * <https://web.archive.org/web/20250427183103/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55180>
        * <https://web.archive.org/web/20250427183110/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55183>
        * <https://web.archive.org/web/20250427183127/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55185>
        * <https://web.archive.org/web/20250427183127/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=55505>
        * <https://web.archive.org/web/20250427183144/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=57425>
        * <https://web.archive.org/web/20250427183649/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=60605>
        * <https://web.archive.org/web/20250427183650/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=60606>
        * <https://web.archive.org/web/20250427183657/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=60607>
        * <https://web.archive.org/web/20250427183657/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=60608>
        * <https://web.archive.org/web/20250427183826/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=60613>
        * <https://web.archive.org/web/20250427183705/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=60614>
        * <https://web.archive.org/web/20230930101647/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=79939>
        * <https://web.archive.org/web/20250427183713/https://guernseylegalresources.gg/CHttpHandler.ashx?documentid=83147>
        * <https://web.archive.org/web/20221117224227/https://www.bbc.com/news/world-europe-guernsey-62864318>
        * <https://web.archive.org/web/20240718021636/https://www.bbc.co.uk/news/articles/c1441ddn87po>

    All "jour de relâche" entries are translated as special day off instead.

    While there's no source for Queen Elizabeth II's Silver Jubilee being observed
    by Guernsey - it's safe to assume such holiday was declared.
    """

    # Special Day Off.
    special_day_off = "Special Day Off"

    # The visit of Her Majesty Queen Elizabeth II.
    elizabeth_2_royal_visit = "The visit of Her Majesty Queen Elizabeth II"

    # Millenium Celebrations.
    millenium_celebrations = "Millennium Celebrations"

    # New Year's Day.
    new_years_day_in_lieu = "New Year's Day"

    # Christmas Day.
    christmas_day_in_lieu = "Christmas Day"

    # Boxing Day.
    boxing_day_in_lieu = "Boxing Day"

    special_public_holidays = {
        1930: (OCT, 10, special_day_off),
        1935: (MAY, 6, "Silver Jubilee of King George V"),
        1937: (MAY, 12, "The Coronation of King George VI and Queen Elizabeth"),
        1939: (SEP, 4, special_day_off),
        1949: (SEP, 19, special_day_off),
        1953: (JUN, 2, "The Coronation of Queen Elizabeth II"),
        1957: (JUL, 26, elizabeth_2_royal_visit),
        1977: (JUN, 7, "Silver Jubilee of Elizabeth II"),
        1978: (JUN, 28, elizabeth_2_royal_visit),
        1981: (JUL, 29, "Wedding of Charles and Diana"),
        1989: (MAY, 23, elizabeth_2_royal_visit),
        1999: (
            (DEC, 28, millenium_celebrations),
            (DEC, 31, millenium_celebrations),
        ),
        2000: (JAN, 3, millenium_celebrations),
        2001: (JUL, 12, elizabeth_2_royal_visit),
        2002: (JUN, 3, "Golden Jubilee of Elizabeth II"),
        2011: (APR, 29, "Wedding of William and Catherine"),
        2012: (JUN, 5, "Diamond Jubilee of Elizabeth II"),
        2022: (
            (JUN, 3, "Queen's Platinum Jubilee Bank Holiday"),
            (SEP, 19, "State Funeral of Queen Elizabeth II"),
        ),
        2023: (MAY, 8, "Extra Public Holiday for the Coronation of King Charles III"),
        2024: (JUL, 16, "The visit of His Majesty King Charles III and Queen Camilla"),
    }
    special_public_holidays_observed = {
        1932: (DEC, 27, christmas_day_in_lieu),
        1933: (JAN, 2, new_years_day_in_lieu),
        1938: (DEC, 27, christmas_day_in_lieu),
        1939: (JAN, 2, new_years_day_in_lieu),
        1981: (DEC, 28, boxing_day_in_lieu),
        1982: (DEC, 28, christmas_day_in_lieu),
        1983: (JAN, 3, new_years_day_in_lieu),
        1987: (DEC, 28, boxing_day_in_lieu),
        1993: (DEC, 28, christmas_day_in_lieu),
        1994: (JAN, 3, new_years_day_in_lieu),
        2004: (DEC, 28, christmas_day_in_lieu),
        2005: (JAN, 3, new_years_day_in_lieu),
    }
