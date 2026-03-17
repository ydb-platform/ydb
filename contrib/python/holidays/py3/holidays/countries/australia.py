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

from holidays.calendars.gregorian import JAN, APR, JUN, AUG, SEP, OCT, DEC
from holidays.constants import BANK, HALF_DAY, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    SAT_TO_NEXT_MON,
    SUN_TO_NEXT_MON,
    SUN_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
    ALL_TO_NEXT_MON,
)


class Australia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Australia holidays.

    References:
          * <https://web.archive.org/web/20250414072303/https://www.qld.gov.au/recreation/travel/holidays>
          * [ACT Holidays Act 1958](https://web.archive.org/web/20250322061953/https://www.legislation.act.gov.au/a/1958-19/)
          * [ACT 2013-2023](https://web.archive.org/web/20240401072340/https://www.cmtedd.act.gov.au/archived-content/holidays/previous-years)
          * [ACT Holidays (Anzac Day 2026) Declaration 2025 (No 1)](https://web.archive.org/web/20260116225801/https://legislation.act.gov.au/View/ni/2025-650/current/html/2025-650.html)
          * [NSW Banks and Bank Holidays Act 1912](https://web.archive.org/web/20241107225523/https://legislation.nsw.gov.au/view/html/repealed/current/act-1912-043)
          * [NSW Public Holidays Act 2010](https://web.archive.org/web/20250316173922/https://legislation.nsw.gov.au/view/html/inforce/current/act-2010-115)
          * [NSW 2026-2027](https://web.archive.org/web/20260216073138/https://www.nsw.gov.au/about-nsw/public-holidays)
          * [NT Public Holidays Act 1981](https://web.archive.org/web/20250315072128/https://legislation.nt.gov.au/api/sitecore/Act/PDF?id=12145)
          * [QLD Holidays Act 1983](https://web.archive.org/web/20250404230918/https://www.legislation.qld.gov.au/view/html/inforce/current/act-1983-018)
          * [QLD 2013-2028](https://web.archive.org/web/20150703042947/http://www.qld.gov.au/recreation/travel/holidays/public/)
          * [SA Holidays Act 1910](https://web.archive.org/web/20250420203417/https://www.legislation.sa.gov.au/LZ/C/A/HOLIDAYS%20ACT%201910.aspx)
          * [SA Public Holidays Act 2023](https://web.archive.org/web/20250429092113/https://www.legislation.sa.gov.au/lz?path=/C/A/Public%20Holidays%20Act%202023)
          * [SA 2023-2024](https://web.archive.org/web/20250404084235/https://www.safework.sa.gov.au/resources/public-holidays)
          * [SA 2007-2021](https://web.archive.org/web/20240610084716/https://www.safework.sa.gov.au/__data/assets/pdf_file/0007/235474/Public-Holidays-since-2007.pdf)
          * [TAS Statutory Holidays Act 2000](https://web.archive.org/web/20250423095807/https://www.legislation.tas.gov.au/view/html/inforce/current/act-2000-096)
          * [VIC Public Holidays Act 1993](https://web.archive.org/web/20250212090816/https://www.legislation.vic.gov.au/in-force/acts/public-holidays-act-1993/027)
          * [VIC Minister appointment](https://web.archive.org/web/20240328142238/https://www.gazette.vic.gov.au/gazette/Gazettes2015/GG2015S229.pdf)
          * [VIC 2018-2024](https://web.archive.org/web/20250422235530/https://business.vic.gov.au/business-information/public-holidays)
          * [WA Public and Bank Holidays Act 1972](https://web.archive.org/web/20241106220940/https://www.legislation.wa.gov.au/legislation/statutes.nsf/law_a639.html)
          * [WA 2019-2023](https://web.archive.org/web/20240805123535/https://www.commerce.wa.gov.au/labour-relations/previous-years-public-holiday-dates)
    """

    country = "AU"
    default_language = "en_AU"
    # %s (observed).
    observed_label = tr("%s (observed)")
    start_year = 1801
    # fmt: off
    subdivisions = (
        "ACT",  # Australian Capital Territory.
        "NSW",  # New South Wales.
        "NT",   # Northern Territory.
        "QLD",  # Queensland.
        "SA",   # South Australia.
        "TAS",  # Tasmania.
        "VIC",  # Victoria.
        "WA",   # Western Australia.
    )
    # fmt: on
    subdivisions_aliases = {
        "Australian Capital Territory": "ACT",
        "New South Wales": "NSW",
        "Northern Territory": "NT",
        "Queensland": "QLD",
        "South Australia": "SA",
        "Tasmania": "TAS",
        "Victoria": "VIC",
        "Western Australia": "WA",
    }
    supported_categories = (BANK, HALF_DAY, PUBLIC)
    supported_languages = ("en_AU", "en_US", "th")

    @property
    def sovereign_birthday(self) -> str:
        """Sovereign's birthday holiday name."""
        return (
            # King's Birthday.
            tr("King's Birthday")
            if 1902 <= self._year <= 1951 or self._year >= 2023
            # Queen's Birthday.
            else tr("Queen's Birthday")
        )

    @property
    def australia_day(self) -> str:
        """Australia Day holiday name."""
        return (
            # Australia Day.
            tr("Australia Day")
            if self._year >= 1935
            # Anniversary Day.
            else tr("Anniversary Day")
        )

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, AustraliaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Sovereign's Birthday.
        if 1902 <= self._year <= 1935:
            if self._year >= 1912:
                self._add_holiday_jun_3(self.sovereign_birthday)  # George V
            else:
                self._add_holiday_nov_9(self.sovereign_birthday)  # Edward VII

        if self.subdiv:
            return None

        # Common holidays.

        # New Year's Day.
        self._add_new_years_day(tr("New Year's Day"))

        # Australia Day.
        if self._year >= 1935:
            self._add_holiday_jan_26(self.australia_day)

        if self._year >= 1921:
            # ANZAC Day.
            self._add_anzac_day(tr("ANZAC Day"))

        # Christmas Day.
        self._add_christmas_day(tr("Christmas Day"))

        # Boxing Day.
        self._add_christmas_day_two(tr("Boxing Day"))

    def _populate_subdiv_act_public_holidays(self):
        # New Year's Day.
        # 1959-1992: SUN - move to MON.
        # 1993-2014: SAT, SUN - move to MON.
        # from 2015: SAT, SUN - add MON.

        # New Year's Day.
        dt = self._add_new_years_day(tr("New Year's Day"))
        if self._year >= 2015:
            self._add_observed(dt)
        elif self._year >= 1959:
            self._move_holiday(
                dt,
                rule=SAT_SUN_TO_NEXT_MON if self._year >= 1993 else SUN_TO_NEXT_MON,
                show_observed_label=False,
            )

        # Australia Day.
        # 1959-1989: not MON - move to MON.
        # from 1990: SAT, SUN - move to MON.

        # Australia Day.
        if self._year >= 1888:
            dt = self._add_holiday_jan_26(self.australia_day)
            if self._year >= 1959:
                self._move_holiday(
                    dt,
                    rule=SAT_SUN_TO_NEXT_MON if self._year >= 1990 else ALL_TO_NEXT_MON,
                    show_observed_label=False,
                )

        if self._year >= 1913:
            # Canberra Day.
            name = tr("Canberra Day")
            if self._year <= 1958 or self._year == 2012:
                self._add_holiday_mar_12(name)
            elif self._year <= 2007:
                self._add_holiday_3rd_mon_of_mar(name)
            else:
                self._add_holiday_2nd_mon_of_mar(name)

        # ANZAC Day.
        # from 1959: SUN - move to MON.
        # in 2026: move to MON.

        if self._year >= 1921:
            # ANZAC Day.
            dt = self._add_anzac_day(tr("ANZAC Day"))
            if self._year == 2026:
                self._move_holiday(dt, rule=SAT_SUN_TO_NEXT_MON, show_observed_label=False)
            elif self._year >= 1959:
                self._move_holiday(dt, rule=SUN_TO_NEXT_MON, show_observed_label=False)

        # Easter Saturday.
        self._add_holy_saturday(tr("Easter Saturday"))

        if self._year >= 2016:
            # Easter Sunday.
            self._add_easter_sunday(tr("Easter Sunday"))

        if self._year >= 2018:
            # Reconciliation Day.
            self._add_holiday_1st_mon_from_may_27(tr("Reconciliation Day"))

        # Sovereign's Birthday.
        if self._year >= 1936:
            self._add_holiday_2nd_mon_of_jun(self.sovereign_birthday)

        # Family & Community Day.
        # (First Monday of the September/October school holidays;
        # moved to the second Monday if this falls on Labour day).

        if 2010 <= self._year <= 2017:
            fc_dates = {
                2010: (SEP, 26),
                2011: (OCT, 10),
                2012: (OCT, 8),
                2013: (SEP, 30),
                2014: (SEP, 29),
                2015: (SEP, 28),
                2016: (SEP, 26),
                2017: (SEP, 25),
            }
            # Family & Community Day.
            self._add_holiday(tr("Family & Community Day"), fc_dates[self._year])

        # Labor Day.
        self._add_holiday_1st_mon_of_oct(tr("Labour Day"))

        # Boxing Day.
        # 1958-1991: SUN - to MON, MON - to TUE.
        # 1992-2013: SAT - to MON, SUN - to TUE, MON - to TUE.
        # from 2014: SAT - add MON, SUN - add TUE.
        # (Placed before Christmas Day for proper observed calculation).

        # Boxing Day.
        dt = self._add_christmas_day_two(tr("Boxing Day"))
        if self._year >= 2014:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE)
        elif self._year >= 1958:
            self._move_holiday(
                dt,
                rule=(SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE)
                if self._year >= 1992
                else (SUN_TO_NEXT_MON + MON_TO_NEXT_TUE),
                show_observed_label=False,
            )

        # Christmas Day.
        # 1958-1991: SUN - to MON.
        # 1992-2013: SAT, SUN - to MON.
        # from 2014: SAT - add MON, SUN - add TUE.

        # Christmas Day.
        dt = self._add_christmas_day(tr("Christmas Day"))
        if self._year >= 2014:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE)
        elif self._year >= 1958:
            self._move_holiday(
                dt,
                rule=SAT_SUN_TO_NEXT_MON if self._year >= 1992 else SUN_TO_NEXT_MON,
                show_observed_label=False,
            )

    def _populate_subdiv_act_bank_holidays(self):
        # Bank Holiday.
        self._add_holiday_1st_mon_of_aug(tr("Bank Holiday"))

    def _populate_subdiv_nsw_public_holidays(self):
        # New Year's Day.
        # 1912-2010: SUN - add MON.
        # from 2011: SAT, SUN - add MON.

        self._add_observed(
            # New Year's Day.
            self._add_new_years_day(tr("New Year's Day")),
            rule=SAT_SUN_TO_NEXT_MON if self._year >= 2011 else SUN_TO_NEXT_MON,
        )

        # Australia Day.
        # 1912-2010: SUN - add MON.
        # from 2011: SAT, SUN - to MON.

        if self._year >= 1888:
            dt = self._add_holiday_jan_26(
                # Australia Day.
                tr("Australia Day")
                if self._year >= 1946
                # Anniversary Day.
                else tr("Anniversary Day")
            )
            if self._year >= 2011:
                self._move_holiday(dt, show_observed_label=False)
            else:
                self._add_observed(dt, rule=SUN_TO_NEXT_MON)

        # Easter Saturday.
        self._add_holy_saturday(tr("Easter Saturday"))

        if self._year >= 2011:
            # Easter Sunday.
            self._add_easter_sunday(tr("Easter Sunday"))

        # ANZAC Day.
        # 1912-2010: SUN - add MON.
        # from 2011: normal.
        # 2026-2027: SAT, SUN - add MON.

        if self._year >= 1921:
            # ANZAC Day.
            dt = self._add_anzac_day(tr("ANZAC Day"))
            if self._year <= 2010:
                self._add_observed(dt, rule=SUN_TO_NEXT_MON)
            elif 2026 <= self._year <= 2027:
                self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON)

        # Labor Day.
        self._add_holiday_1st_mon_of_oct(tr("Labour Day"))

        # Sovereign's Birthday.
        if self._year >= 1936:
            self._add_holiday_2nd_mon_of_jun(self.sovereign_birthday)

        if 1912 <= self._year <= 2010:
            # Bank Holiday.
            self._add_holiday_1st_mon_of_aug(tr("Bank Holiday"))

        # Christmas Day.
        # 1912-2010: SUN - add TUE.
        # from 2011: SAT - add MON, SUN - add TUE.

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE if self._year >= 2011 else SUN_TO_NEXT_TUE,
        )

        # Boxing Day.
        # 1912-2010: SUN - add MON.
        # from 2011: SAT - add MON, SUN - add TUE.

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE if self._year >= 2011 else SUN_TO_NEXT_MON,
        )

    def _populate_subdiv_nsw_bank_holidays(self):
        if self._year >= 2011:
            # Bank Holiday.
            self._add_holiday_1st_mon_of_aug(tr("Bank Holiday"))

    def _populate_subdiv_nt_public_holidays(self):
        # New Year's Day.
        # 1982-2016: SAT, SUN - move to MON.
        # from 2017: SAT, SUN - add MON.

        # New Year's Day.
        dt = self._add_new_years_day(tr("New Year's Day"))
        if self._year >= 2017:
            self._add_observed(dt)
        elif self._year >= 1982:
            self._move_holiday(dt, show_observed_label=False)

        # Australia Day.
        # from 1982: SAT, SUN - move to MON.

        # Australia Day.
        if self._year >= 1888:
            dt = self._add_holiday_jan_26(self.australia_day)
            if self._year >= 1982:
                self._move_holiday(dt, show_observed_label=False)

        # ANZAC Day.
        # from 1982: SUN - to MON.

        if self._year >= 1921:
            # ANZAC Day.
            dt = self._add_anzac_day(tr("ANZAC Day"))
            if self._year >= 1982:
                self._move_holiday(dt, rule=SUN_TO_NEXT_MON, show_observed_label=False)

        # Easter Saturday.
        self._add_holy_saturday(tr("Easter Saturday"))

        if self._year >= 2024:
            # Easter Sunday.
            self._add_easter_sunday(tr("Easter Sunday"))

        # May Day.
        self._add_holiday_1st_mon_of_may(tr("May Day"))

        # Sovereign's Birthday.
        if self._year >= 1936:
            self._add_holiday_2nd_mon_of_jun(self.sovereign_birthday)

        # Picnic Day.
        self._add_holiday_1st_mon_of_aug(tr("Picnic Day"))

        # Boxing Day.
        # 1981-2022: SAT - to MON, SUN - to TUE, MON - to TUE.
        # from 2023: SAT - add MON, SUN - add TUE, MON - add TUE.
        # (Placed before Christmas Day for proper observed calculation).

        # Boxing Day.
        dt = self._add_christmas_day_two(tr("Boxing Day"))
        if self._year >= 2023:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE)
        elif self._year >= 1981:
            self._move_holiday(
                dt, rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE, show_observed_label=False
            )

        # Christmas Day.
        # 1981-2015: SAT, SUN - to MON.
        # from 2016: SAT, SUN - add MON.

        # Christmas Day.
        dt = self._add_christmas_day(tr("Christmas Day"))
        if self._year >= 2016:
            self._add_observed(dt)
        elif self._year >= 1981:
            self._move_holiday(dt, show_observed_label=False)

    def _populate_subdiv_nt_half_day_holidays(self):
        if self._year >= 2016:
            # %s (from 7pm).
            begin_time_label = self.tr("%s (from 7pm)")

            # Christmas Eve.
            self._add_christmas_eve(begin_time_label % self.tr("Christmas Eve"))

            # New Year's Eve.
            self._add_new_years_eve(begin_time_label % self.tr("New Year's Eve"))

    def _populate_subdiv_qld_public_holidays(self):
        # New Year's Day.
        # 1984-2011: SUN - move to MON.
        # from 2012: SAT, SUN - add MON.

        # New Year's Day.
        dt = self._add_new_years_day(tr("New Year's Day"))
        if self._year >= 2012:
            self._add_observed(dt)
        elif self._year >= 1984:
            self._move_holiday(dt, rule=SUN_TO_NEXT_MON, show_observed_label=False)

        # Australia Day.
        # 1984-1995: not MON - move to MON.
        # from 1996: SAT, SUN - move to MON.

        # Australia Day.
        if self._year >= 1888:
            dt = self._add_holiday_jan_26(self.australia_day)
            if self._year >= 1984:
                self._move_holiday(
                    dt,
                    rule=SAT_SUN_TO_NEXT_MON if self._year >= 1996 else ALL_TO_NEXT_MON,
                    show_observed_label=False,
                )

        # ANZAC Day.
        # from 1984: SUN - move to MON.

        if self._year >= 1921:
            # ANZAC Day.
            dt = self._add_anzac_day(tr("ANZAC Day"))
            if self._year >= 1984:
                self._move_holiday(dt, rule=SUN_TO_NEXT_MON, show_observed_label=False)

        # Easter Saturday.
        self._add_holy_saturday(tr("Easter Saturday"))

        if self._year >= 2017:
            # Easter Sunday.
            self._add_easter_sunday(tr("Easter Sunday"))

        # Labor Day.
        name = tr("Labour Day")
        if 2013 <= self._year <= 2015:
            self._add_holiday_1st_mon_of_oct(name)
        else:
            self._add_holiday_1st_mon_of_may(name)

        # Sovereign's Birthday.
        if self._year >= 1936:
            if self._year <= 2015 and self._year != 2012:
                self._add_holiday_2nd_mon_of_jun(self.sovereign_birthday)
            else:
                self._add_holiday_1st_mon_of_oct(self.sovereign_birthday)

        # The Royal Queensland Show (Ekka).
        # The Show starts on the first Friday of August - providing this is
        # not prior to the 5th - in which case it will begin on the second
        # Friday. The Wednesday during the show is a public holiday.
        ekka_dates = {
            2020: (AUG, 14),
            2021: (OCT, 29),
        }
        # The Royal Queensland Show.
        name = tr("The Royal Queensland Show")
        if dt := ekka_dates.get(self._year):
            self._add_holiday(name, dt)
        else:
            # [1st FRI after Aug 5] + 5 days = [1st WED after Aug 10]
            self._add_holiday_1st_wed_from_aug_10(name)

        # Boxing Day.
        # 1984-2010: SUN - to MON, MON - to TUE.
        # from 2011: SAT - add MON, SUN - add TUE.
        # (Placed before Christmas Day for proper observed calculation).

        # Boxing Day.
        dt = self._add_christmas_day_two(tr("Boxing Day"))
        if self._year >= 2011:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE)
        elif self._year >= 1984:
            self._move_holiday(
                dt, rule=SUN_TO_NEXT_MON + MON_TO_NEXT_TUE, show_observed_label=False
            )

        # Christmas Day.
        # 1984-2010: SUN - to MON.
        # from 2011: SAT - add MON, SUN - add TUE.

        # Christmas Day.
        dt = self._add_christmas_day(tr("Christmas Day"))
        if self._year >= 2011:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE)
        elif self._year >= 1984:
            self._move_holiday(dt, rule=SUN_TO_NEXT_MON, show_observed_label=False)

    def _populate_subdiv_qld_half_day_holidays(self):
        if self._year >= 2019:
            # %s (from 6pm).
            begin_time_label = self.tr("%s (from 6pm)")

            # Christmas Eve.
            self._add_christmas_eve(begin_time_label % self.tr("Christmas Eve"))

    def _populate_subdiv_sa_public_holidays(self):
        # New Year's Day.
        # 1984-2003: SAT, SUN - move to MON.
        # 2004-2023: SAT - move to MON, SUN - add MON.
        # from 2024: SAT, SUN - add MON.

        # New Year's Day.
        dt = self._add_new_years_day(tr("New Year's Day"))
        if self._year >= 2024:
            self._add_observed(dt)
        elif self._year >= 2004:
            self._add_observed(dt, rule=SUN_TO_NEXT_MON)
            self._move_holiday(dt, rule=SAT_TO_NEXT_MON, show_observed_label=False)
        elif self._year >= 1984:
            self._move_holiday(dt, show_observed_label=False)

        # Australia Day.
        # 1984-1993: not MON - move to MON.
        # 1994-2003: SAT, SUN - move to MON.
        # 2004-2023: SAT - move to MON, SUN - add MON.
        # from 2024: SAT, SUN - move to MON.

        if self._year >= 1935:
            # Australia Day.
            dt = self._add_holiday_jan_26(self.australia_day)
            if 1994 <= self._year <= 2003 or self._year >= 2024:
                self._move_holiday(dt, show_observed_label=False)
            elif self._year >= 2004:
                self._add_observed(dt, rule=SUN_TO_NEXT_MON)
                self._move_holiday(dt, rule=SAT_TO_NEXT_MON, show_observed_label=False)
            elif self._year >= 1984:
                self._move_holiday(dt, rule=ALL_TO_NEXT_MON, show_observed_label=False)

        # Adelaide Cup Day.
        # First observed as Public Holidays in 1973: https://web.archive.org/web/20260122052707/https://www.racingsa.com.au/news/a-little-adelaide-cup-history
        # 2006-2023: changed each year by SA Government Proclamation from the 3rd Monday in May
        # to the 2nd Monday in March.
        # from 2024: changed to the 2nd Monday in March officially.

        # Adelaide Cup Day.
        name = tr("Adelaide Cup Day")
        if self._year >= 2006:
            self._add_holiday_2nd_mon_of_mar(name)
        elif self._year >= 1973:
            self._add_holiday_3rd_mon_of_may(name)

        # Easter Saturday.
        self._add_holy_saturday(tr("Easter Saturday"))

        if self._year >= 2024:
            # Easter Sunday.
            self._add_easter_sunday(tr("Easter Sunday"))

        if self._year >= 1921:
            # ANZAC Day.
            dt = self._add_anzac_day(tr("ANZAC Day"))
            if self._year <= 2023:
                self._add_observed(dt, rule=SUN_TO_NEXT_MON)

        # Sovereign's Birthday.
        if self._year >= 1936:
            self._add_holiday_2nd_mon_of_jun(self.sovereign_birthday)

        # Labor Day.
        self._add_holiday_1st_mon_of_oct(tr("Labour Day"))

        # Proclamation Day.
        # 1984-1992: SAT, SUN - move to MON.
        # 1993-2002: SAT - to MON, SUN - to TUE, MON - to TUE.
        # 2003-2023: SAT - to MON, SUN - add TUE, MON - add TUE.
        # from 2024: SAT - add MON, SUN - add TUE, MON - add TUE.
        # (Placed before Christmas Day for proper observed calculation).

        # Proclamation Day.
        dt = self._add_christmas_day_two(tr("Proclamation Day"))
        if self._year >= 2024:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE)
        elif self._year >= 2003:
            self._add_observed(dt, rule=SUN_TO_NEXT_TUE + MON_TO_NEXT_TUE)
            self._move_holiday(dt, rule=SAT_TO_NEXT_MON, show_observed_label=False)
        elif self._year >= 1993:
            self._move_holiday(
                dt, rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE, show_observed_label=False
            )
        elif self._year >= 1984:
            self._move_holiday(dt, show_observed_label=False)

        # Christmas Day.
        # 1984-2002: SAT, SUN - move to MON.
        # 2003-2023: SAT - move to MON, SUN - add MON.
        # from 2024: SAT, SUN - add MON.

        # Christmas Day.
        dt = self._add_christmas_day(tr("Christmas Day"))
        if self._year >= 2024:
            self._add_observed(dt)
        elif self._year >= 2003:
            self._add_observed(dt, rule=SUN_TO_NEXT_MON)
            self._move_holiday(dt, rule=SAT_TO_NEXT_MON, show_observed_label=False)
        elif self._year >= 1984:
            self._move_holiday(dt, show_observed_label=False)

    def _populate_subdiv_sa_half_day_holidays(self):
        if self._year >= 2012:
            # %s (from 7pm).
            begin_time_label = self.tr("%s (from 7pm)")

            # Christmas Eve.
            self._add_christmas_eve(begin_time_label % self.tr("Christmas Eve"))

            # New Year's Eve.
            self._add_new_years_eve(begin_time_label % self.tr("New Year's Eve"))

    def _populate_subdiv_tas_public_holidays(self):
        # New Year's Day.
        # from 2001: SAT, SUN - move to MON.

        # New Year's Day.
        dt = self._add_new_years_day(tr("New Year's Day"))
        if self._year >= 2001:
            self._move_holiday(dt, show_observed_label=False)

        # Australia Day.
        # from 2001: SAT, SUN - move to MON.

        # Australia Day.
        if self._year >= 1888:
            dt = self._add_holiday_jan_26(self.australia_day)
            if self._year >= 2001:
                self._move_holiday(dt, show_observed_label=False)

        # Eight Hours Day.
        self._add_holiday_2nd_mon_of_mar(tr("Eight Hours Day"))

        if self._year <= 2010:
            # Easter Tuesday.
            self._add_easter_tuesday(tr("Easter Tuesday"))

        if self._year >= 1921:
            # ANZAC Day.
            self._add_anzac_day(tr("ANZAC Day"))

        # Sovereign's Birthday.
        if self._year >= 1936:
            self._add_holiday_2nd_mon_of_jun(self.sovereign_birthday)

        # Christmas Day.
        # 2000-2009: SAT - to MON, SUN - to TUE.
        # from 2010: SAT - add MON, SUN - add TUE.

        # Christmas Day.
        dt = self._add_christmas_day(tr("Christmas Day"))
        if self._year >= 2010:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE)
        elif self._year >= 2000:
            self._move_holiday(dt, rule=SAT_SUN_TO_NEXT_MON_TUE, show_observed_label=False)

        # Boxing Day.
        # from 2000: SAT - to MON, SUN - to TUE.

        # Boxing Day.
        dt = self._add_christmas_day_two(tr("Boxing Day"))
        if self._year >= 2000:
            self._move_holiday(dt, rule=SAT_SUN_TO_NEXT_MON_TUE, show_observed_label=False)

    def _populate_subdiv_vic_public_holidays(self):
        # New Year's Day.
        # 1994-1997: SUN - add MON.
        # 1998-2008: SUN - move to MON.
        # from 2009: SAT, SUN - add MON.

        # New Year's Day.
        dt = self._add_new_years_day(tr("New Year's Day"))
        if self._year >= 2009:
            self._add_observed(dt)
        elif self._year >= 1998:
            self._move_holiday(dt, rule=SUN_TO_NEXT_MON, show_observed_label=False)
        elif self._year >= 1994:
            self._add_observed(dt, rule=SUN_TO_NEXT_MON)

        # Australia Day.
        # from 2009: SAT, SUN - move to MON.

        # Australia Day.
        if self._year >= 1888:
            dt = self._add_holiday_jan_26(self.australia_day)
            if self._year >= 2009:
                self._move_holiday(dt, show_observed_label=False)

        # Labor Day.
        self._add_holiday_2nd_mon_of_mar(tr("Labour Day"))

        if self._year >= 2003:
            # Easter Saturday.
            self._add_holy_saturday(tr("Easter Saturday"))

        if self._year >= 2016:
            # Easter Sunday.
            self._add_easter_sunday(tr("Easter Sunday"))

        if self._year >= 1921:
            # ANZAC Day.
            self._add_anzac_day(tr("ANZAC Day"))

        # Sovereign's Birthday.
        if self._year >= 1936:
            self._add_holiday_2nd_mon_of_jun(self.sovereign_birthday)

        if self._year >= 2015:
            grand_final_dates = {
                2015: (OCT, 2),
                2016: (SEP, 30),
                # Rescheduled due to COVID-19.
                2020: (OCT, 23),
            }
            # Grand Final Day.
            name = tr("Grand Final Day")
            if dt := grand_final_dates.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_holiday_1_day_prior_last_sat_of_sep(name)

        if self._year >= 2009:
            # Melbourne Cup Day.
            self._add_holiday_1st_tue_of_nov(tr("Melbourne Cup Day"))

        # Christmas Day.
        # 2008-2018: SAT - to MON, SUN - to TUE.
        # from 2019: SAT - add MON, SUN - add TUE.

        # Christmas Day.
        dt = self._add_christmas_day(tr("Christmas Day"))
        if self._year >= 2019:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE)
        elif self._year >= 2008:
            self._move_holiday(dt, rule=SAT_SUN_TO_NEXT_MON_TUE, show_observed_label=False)

        # Boxing Day.
        # 1994-2007: SUN - to MON.
        # from 2008: SAT - add MON, SUN - add TUE.

        # Boxing Day.
        dt = self._add_christmas_day_two(tr("Boxing Day"))
        if self._year >= 2008:
            self._add_observed(dt, rule=SAT_SUN_TO_NEXT_MON_TUE)
        elif self._year >= 1994:
            self._move_holiday(dt, rule=SUN_TO_NEXT_MON, show_observed_label=False)

    def _populate_subdiv_wa_public_holidays(self):
        # New Year's Day.
        # from 1973: SAT, SUN - add MON.

        # New Year's Day.
        dt = self._add_new_years_day(tr("New Year's Day"))
        if self._year >= 1973:
            self._add_observed(dt)

        # Australia Day.
        # 1973-1993: not MON - move to MON.
        # from 1994: SAT, SUN - move to MON.

        # Australia Day.
        if self._year >= 1888:
            dt = self._add_holiday_jan_26(self.australia_day)
            if self._year >= 1994:
                self._move_holiday(dt, show_observed_label=False)
            elif self._year >= 1973:
                self._move_holiday(dt, rule=ALL_TO_NEXT_MON, show_observed_label=False)

        # Labor Day.
        self._add_holiday_1st_mon_of_mar(tr("Labour Day"))

        if self._year >= 2022:
            # Easter Sunday.
            self._add_easter_sunday(tr("Easter Sunday"))

        if self._year >= 1921:
            # ANZAC Day.
            dt = self._add_anzac_day(tr("ANZAC Day"))
            if self._year >= 1973:
                self._add_observed(dt)

        if self._year >= 1833:
            self._add_holiday_1st_mon_of_jun(
                # Western Australia Day.
                tr("Western Australia Day")
                if self._year >= 2012
                # Foundation Day.
                else tr("Foundation Day")
            )

        # Sovereign's Birthday.
        if self._year >= 1936:
            if self._year >= 1984:
                # Celebration Day for the Anniversary of the Birthday of the Reigning Sovereign
                # to be appointed for each year by proclamation published in the Government Gazette
                sovereign_birthday_dates = {
                    2011: (OCT, 28),
                    2012: (OCT, 1),
                    2024: (SEP, 23),
                }
                if dt := sovereign_birthday_dates.get(self._year):
                    self._add_holiday(self.sovereign_birthday, dt)
                else:
                    self._add_holiday_last_mon_of_sep(self.sovereign_birthday)
            else:
                self._add_holiday_2nd_mon_of_oct(self.sovereign_birthday)

        # Boxing Day.
        # 1972-1975: SAT - add MON, SUN - add TUE.
        # from 1976: SAT - add MON, SUN - add TUE, MON - add TUE.
        # (Placed before Christmas Day for proper observed calculation).

        # Boxing Day.
        dt = self._add_christmas_day_two(tr("Boxing Day"))
        if self._year >= 1972:
            self._add_observed(
                dt,
                rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE
                if self._year >= 1976
                else SAT_SUN_TO_NEXT_MON_TUE,
            )

        # Christmas Day.
        # from 1972: SAT, SUN - add MON.

        # Christmas Day.
        dt = self._add_christmas_day(tr("Christmas Day"))
        if self._year >= 1972:
            self._add_observed(dt)


class AU(Australia):
    pass


class AUS(Australia):
    pass


class AustraliaStaticHolidays:
    # Special public holiday.
    special_public_holiday = tr("Special public holiday")

    special_public_holidays = {
        # National Day of Mourning for Queen Elizabeth II.
        2022: (SEP, 22, tr("National Day of Mourning for Queen Elizabeth II")),
    }

    special_act_public_holidays = {
        # Declared public holiday.
        2020: (APR, 20, tr("Declared public holiday")),
        # Additional public holiday.
        2021: (APR, 25, tr("Additional public holiday")),
    }

    special_qld_public_holidays = {
        # Queen's Diamond Jubilee.
        2012: (JUN, 11, tr("Queen's Diamond Jubilee")),
    }

    special_qld_public_holidays_observed = {
        # Christmas Day.
        2010: (DEC, 28, tr("Christmas Day")),
        # New Year's Day.
        2011: (JAN, 3, tr("New Year's Day")),
    }

    special_wa_public_holidays = {
        # In 2011 both ANZAC Day and Easter Monday fell on Monday 25 April.
        2011: (APR, 26, special_public_holiday),
    }
