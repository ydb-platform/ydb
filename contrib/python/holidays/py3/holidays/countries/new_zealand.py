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

from holidays.calendars.gregorian import MAR, JUN, JUL, SEP, _timedelta
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    ALL_TO_NEAREST_MON,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class NewZealand(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """New Zealand holidays."""

    country = "NZ"
    # %s (observed).
    observed_label = "%s (observed)"
    start_year = 1894
    subdivisions = (
        # ISO 3166-2: Regions and Special Island Authorities.
        # https://en.wikipedia.org/wiki/ISO_3166-2:NZ
        "AUK",  # Auckland (Tāmaki-Makaurau).
        "BOP",  # Bay of Plenty (Toi Moana).
        "CAN",  # Canterbury (Waitaha).
        "CIT",  # Chatham Islands Territory (Wharekauri).
        "GIS",  # Gisborne (Te Tairāwhiti).
        "HKB",  # Hawke's Bay (Te Matau-a-Māui).
        "MBH",  # Marlborough (Manawatū-Whanganui).
        "MWT",  # Manawatū-Whanganui (Manawatū Whanganui).
        "NSN",  # Nelson (Whakatū).
        "NTL",  # Northland (Te Taitokerau).
        "OTA",  # Otago (Ō Tākou).
        "STL",  # Southland (Te Taiao Tonga).
        "TAS",  # Tasman (Te tai o Aorere).
        "TKI",  # Taranaki (Taranaki).
        "WGN",  # Greater Wellington (Te Pane Matua Taiao).
        "WKO",  # Waikato (Waikato).
        "WTC",  # West Coast (Te Tai o Poutini).
        # Subregions:
        # https://web.archive.org/web/20250415230521/https://www.govt.nz/browse/work/public-holidays-and-work/public-holidays-and-anniversary-dates/
        "South Canterbury",
    )
    subdivisions_aliases = {
        # Fullnames in English and Maori, as well as HASC.
        "Auckland": "AUK",
        "Tāmaki-Makaurau": "AUK",
        "AU": "AUK",
        "Bay of Plenty": "BOP",
        "Toi Moana": "BOP",
        "BP": "BOP",
        "Canterbury": "CAN",
        "Waitaha": "CAN",
        "CA": "CAN",
        "Chatham Islands Territory": "CIT",
        "Chatham Islands": "CIT",  # 1901-1994, as County
        "Wharekauri": "CIT",
        "CI": "CIT",
        "Gisborne": "GIS",
        "Te Tairāwhiti": "GIS",
        "GI": "GIS",
        "Hawke's Bay": "HKB",
        "Te Matau-a-Māui": "HKB",
        "HB": "HKB",
        "Marlborough": "MBH",
        "MA": "MBH",
        "Manawatū Whanganui": "MWT",
        "Manawatū-Whanganui": "MWT",
        "MW": "MWT",
        "Nelson": "NSN",
        "Whakatū": "NSN",
        "NE": "NSN",
        "Northland": "NTL",
        "Te Taitokerau": "NTL",
        "NO": "NTL",
        "Otago": "OTA",
        "Ō Tākou": "OTA",
        "OT": "OTA",
        "Southland": "STL",
        "Te Taiao Tonga": "STL",
        "SO": "STL",
        "Tasman": "TAS",
        "Te tai o Aorere": "TAS",
        "TS": "TAS",
        "Taranaki": "TKI",
        "TK": "TKI",
        "Greater Wellington": "WGN",
        "Te Pane Matua Taiao": "WGN",
        "Wellington": "WGN",  # Prev. ISO code from 2010-2015.
        "Te Whanganui-a-Tara": "WGN",  # Prev. ISO code from 2010-2015.
        "WG": "WGN",
        "Waikato": "WKO",
        "WK": "WKO",
        "West Coast": "WTC",
        "Te Tai o Poutini": "WTC",
        "WC": "WTC",
    }
    _deprecated_subdivisions = (
        # Pre-1893 Naming in Previous Implementations.
        "New Plymouth",  # 1853-1859, Now Taranaki.
        "Westland",  # 1873-1876, Now West Coast.
        # Unofficial Code.
        "STC",  # For 'South Canterbury' Subregional Holidays.
        "WTL",  # Westland, Correct code is WTC (for West Coast).
    )

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, NewZelandStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Bank Holidays Act 1873
        # The Employment of Females Act 1873
        # Factories Act 1894
        # Industrial Conciliation and Arbitration Act 1894
        # Labour Day Act 1899
        # Anzac Day Act 1920, 1949, 1956
        # New Zealand Day Act 1973
        # Waitangi Day Act 1960, 1976
        # Sovereign's Birthday Observance Act 1937, 1952
        # Holidays Act 1981, 2003

        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)
        self._add_observed(
            # Day after New Year's Day.
            self._add_new_years_day_two("Day after New Year's Day"),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        if self._year >= 1974:
            feb_6 = self._add_holiday_feb_6(
                # Waitangi Day.
                "Waitangi Day"
                if self._year >= 1977
                # New Zealand Day.
                else "New Zealand Day"
            )
            if self._year >= 2014:
                self._add_observed(feb_6)

        if self._year >= 1921:
            # Anzac Day.
            apr_25 = self._add_anzac_day("Anzac Day")
            if self._year >= 2014:
                self._add_observed(apr_25)

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Sovereign's Birthday
        if self._year >= 1902:
            name = (
                # Queen's Birthday.
                "Queen's Birthday"
                if 1952 <= self._year <= 2022
                # King's Birthday.
                else "King's Birthday"
            )
            if self._year == 1952:
                self._add_holiday_jun_2(name)  # Elizabeth II
            elif self._year >= 1938:
                self._add_holiday_1st_mon_of_jun(name)  # EII & GVI
            elif self._year == 1937:
                self._add_holiday_jun_9(name)  # George VI
            elif self._year == 1936:
                self._add_holiday_jun_23(name)  # Edward VIII
            elif self._year >= 1912:
                self._add_holiday_jun_3(name)  # George V
            else:
                # https://web.archive.org/web/20250427125629/https://paperspast.natlib.govt.nz/cgi-bin/paperspast?a=d&d=NZH19091110.2.67
                self._add_holiday_nov_9(name)  # Edward VII

        # Matariki
        dates_obs = {
            2022: (JUN, 24),
            2023: (JUL, 14),
            2024: (JUN, 28),
            2025: (JUN, 20),
            2026: (JUL, 10),
            2027: (JUN, 25),
            2028: (JUL, 14),
            2029: (JUL, 6),
            2030: (JUN, 21),
            2031: (JUL, 11),
            2032: (JUL, 2),
            2033: (JUN, 24),
            2034: (JUL, 7),
            2035: (JUN, 29),
            2036: (JUL, 18),
            2037: (JUL, 10),
            2038: (JUN, 25),
            2039: (JUL, 15),
            2040: (JUL, 6),
            2041: (JUL, 19),
            2042: (JUL, 11),
            2043: (JUL, 3),
            2044: (JUN, 24),
            2045: (JUL, 7),
            2046: (JUN, 29),
            2047: (JUL, 19),
            2048: (JUL, 3),
            2049: (JUN, 25),
            2050: (JUL, 15),
            2051: (JUN, 30),
            2052: (JUN, 21),
        }
        if dt := dates_obs.get(self._year):
            # Matariki.
            self._add_holiday("Matariki", dt)

        if self._year >= 1900:
            # Labour Day.
            name = "Labour Day"
            if self._year >= 1910:
                self._add_holiday_4th_mon_of_oct(name)
            else:
                self._add_holiday_2nd_wed_of_oct(name)

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two("Boxing Day"), rule=SAT_SUN_TO_NEXT_MON_TUE)

        if self.subdiv == "New Plymouth":
            self._populate_subdiv_tki_public_holidays()
        elif self.subdiv == "STC":
            self._populate_subdiv_south_canterbury_public_holidays()
        elif self.subdiv in {"WTL", "Westland"}:
            self._populate_subdiv_wtc_public_holidays()

    def _populate_subdiv_auk_public_holidays(self):
        self._move_holiday_forced(
            # Auckland Anniversary Day.
            self._add_holiday_jan_29("Auckland Anniversary Day"),
            rule=ALL_TO_NEAREST_MON,
        )

    def _populate_subdiv_can_public_holidays(self):
        # Canterbury Anniversary Day.
        self._add_holiday_10_days_past_1st_tue_of_nov("Canterbury Anniversary Day")

    def _populate_subdiv_cit_public_holidays(self):
        self._move_holiday_forced(
            # Chatham Islands Anniversary Day.
            self._add_holiday_nov_30("Chatham Islands Anniversary Day"),
            rule=ALL_TO_NEAREST_MON,
        )

    def _populate_subdiv_hkb_public_holidays(self):
        # Hawke's Bay Anniversary Day.
        self._add_holiday_3_days_prior_4th_mon_of_oct("Hawke's Bay Anniversary Day")

    def _populate_subdiv_mbh_public_holidays(self):
        # Marlborough Anniversary Day.
        self._add_holiday_7_days_past_4th_mon_of_oct("Marlborough Anniversary Day")

    def _populate_subdiv_nsn_public_holidays(self):
        self._move_holiday_forced(
            # Nelson Anniversary Day.
            self._add_holiday_feb_1("Nelson Anniversary Day"),
            rule=ALL_TO_NEAREST_MON,
        )

    def _populate_subdiv_ntl_public_holidays(self):
        if 1964 <= self._year <= 1973:
            self._move_holiday_forced(
                # Waitangi Day.
                self._add_holiday_feb_6("Waitangi Day"),
                rule=ALL_TO_NEAREST_MON,
            )
        else:
            self._move_holiday_forced(
                # Auckland Anniversary Day.
                self._add_holiday_jan_29("Auckland Anniversary Day"),
                rule=ALL_TO_NEAREST_MON,
            )

    def _populate_subdiv_ota_public_holidays(self):
        # there is no easily determined single day of local observance?!?!
        # If Easter Monday, observed on Easter Tuesday instead.
        dt = self._get_observed_date(date(self._year, MAR, 23), rule=ALL_TO_NEAREST_MON)
        if dt == _timedelta(self._easter_sunday, +1):
            dt = _timedelta(dt, +1)
        # Otago Anniversary Day.
        self._add_holiday("Otago Anniversary Day", dt)

    def _populate_subdiv_south_canterbury_public_holidays(self):
        # South Canterbury Anniversary Day.
        self._add_holiday_4th_mon_of_sep("South Canterbury Anniversary Day")

    def _populate_subdiv_stl_public_holidays(self):
        # Southland Anniversary Day.
        name = "Southland Anniversary Day"
        if self._year >= 2012:
            self._add_easter_tuesday(name)
        else:
            self._move_holiday_forced(self._add_holiday_jan_17(name), rule=ALL_TO_NEAREST_MON)

    def _populate_subdiv_tki_public_holidays(self):
        # Taranaki Anniversary Day.
        self._add_holiday_2nd_mon_of_mar("Taranaki Anniversary Day")

    def _populate_subdiv_wgn_public_holidays(self):
        self._move_holiday_forced(
            # Wellington Anniversary Day.
            self._add_holiday_jan_22("Wellington Anniversary Day"),
            rule=ALL_TO_NEAREST_MON,
        )

    def _populate_subdiv_wtc_public_holidays(self):
        # West Coast Anniversary Day.
        name = "West Coast Anniversary Day"
        if self._year == 2005:
            self._add_holiday_dec_5(name)
        else:
            self._move_holiday_forced(self._add_holiday_dec_1(name), rule=ALL_TO_NEAREST_MON)


class NZ(NewZealand):
    pass


class NZL(NewZealand):
    pass


class NewZelandStaticHolidays:
    special_public_holidays = {
        # Queen Elizabeth II Memorial Day.
        2022: (SEP, 26, "Queen Elizabeth II Memorial Day"),
    }
