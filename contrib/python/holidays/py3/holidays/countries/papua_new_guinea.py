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

from holidays.calendars.gregorian import JAN, FEB, MAR, APR, JUN, SEP, NOV
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class PapuaNewGuinea(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Papua New Guinea holidays.

    References:
        * [Public Holidays Act 1953](https://web.archive.org/web/20250406104407/http://www.paclii.org/pg/legis/consol_act/pha1953163.pdf)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Papua_New_Guinea>
        * <https://web.archive.org/web/20240612224551/https://pnghausbung.com/kings-birthday-holiday-to-be-observed-on-16th-june/>

    Checked With:
        * <https://web.archive.org/web/20250215234848/https://www.ipa.gov.pg/public/Holidays.aspx>
        * <https://web.archive.org/web/20240119163205/https://www.businessadvantagepng.com/papua-new-guinea-public-for-holidays/>
        * [2019](https://web.archive.org/web/20250429081247/https://www.scribd.com/document/465334129/PNG-2019-Gazetted-Public-Holidays-pdf)
        * [2020](https://web.archive.org/web/20250429081229/https://publicholidays.asia/wp-content/uploads/2020/01/PNG_PublicHolidays_2020.png)
        * [2021](https://web.archive.org/web/20240625200332/https://publicholidays.asia/wp-content/uploads/2020/12/PNG_PublicHolidays_2021.pdf)
        * [2022](https://web.archive.org/web/20230530042019/https://publicholidays.asia/wp-content/uploads/2022/01/PNG_PublicHolidays_2022.pdf)
        * [2023](https://web.archive.org/web/20240413053729/https://publicholidays.asia/wp-content/uploads/2022/12/PNG_PublicHolidays_2023.pdf)

    Should a holiday listed, other than the Christmas Day, fall on a Sunday the next Monday
    shall, unless the Head of State, acting on advice, declares otherwise, be observed as a
    public holiday throughout the country.

    When Christmas Day falls on a Sunday, the following Tuesday shall also be observed as a
    public holiday.
    """

    country = "PG"
    observed_label = "%s (observed)"
    # Public Holidays Law 1953 (No. 38 of 1953).
    start_year = 1953

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, PapuaNewGuineaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Section 1: Public Holidays.
        # - Easter Saturday is currently not gazetted as a Public Holiday in 2024.
        # - While Easter Sunday itself is not listed, this is de facto always a day-off.

        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        if self._year <= 2023:
            # Easter Saturday.
            self._add_holy_saturday("Easter Saturday")

        # Easter Sunday.
        self._add_easter_sunday("Easter Sunday")

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Papua New Guinea Remembrance Day.
        self._add_observed(self._add_holiday_jul_23("Papua New Guinea Remembrance Day"))

        # Christmas Day.
        self._add_observed(self._add_christmas_day("Christmas Day"), rule=SUN_TO_NEXT_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two("Boxing Day"))

        # Section 2: Independence Day or Days.

        # Independence Day
        # Commemorates the attainment of Independent Sovereign Statehood on Sep 16, 1975.

        if self._year >= 1975:
            # Independence Day.
            self._add_observed(self._add_holiday_sep_16("Independence Day"))

        # Section 3: Anniversary of Birthday of the Queen/King.
        # In Papua New Guinea, it is usually celebrated on the second Monday of June every year.
        # From 2023, the date changed to Jun 17 (Special Observed for 2023 on Jun 16).

        if self._year <= 2022:
            # Queen's Birthday.
            self._add_holiday_2nd_mon_of_jun("Queen's Birthday")
        else:
            # King's Birthday.
            self._add_holiday_jun_17("King's Birthday")

        # Section 5: Additional Public Holidays.

        # Grand Chief Sir Michael Somare Remembrance Day.
        # First observed on Feb 26, 2022.
        # As of 2024, all previous observances so far are done on an ad hoc basis.

        if self._year >= 2022:
            # Grand Chief Sir Michael Somare Remembrance Day.
            self._add_holiday_feb_26("Grand Chief Sir Michael Somare Remembrance Day")

        # National Repentance Day.
        # Enacted Aug 15, 2011, celebrated by "prayer ceremonies" across the country.
        # First observed on Aug 26, 2011, held annually since then on that date.

        if self._year >= 2011:
            # The National Repentance Day.
            self._add_observed(self._add_holiday_aug_26("National Repentance Day"))


class PG(PapuaNewGuinea):
    pass


class PNG(PapuaNewGuinea):
    pass


class PapuaNewGuineaStaticHolidays:
    """Papua New Guinea special holidays.

    References:
        * [2018 APEC Public Holiday](https://web.archive.org/web/20220813203130/https://www.rnz.co.nz/international/pacific-news/369989/papua-new-guinea-declares-apec-holiday)
        * [2021 Sir Mekere Morauta's Funeral](https://web.archive.org/web/20241206065336/https://pnghausbung.com/friday-8th-declared-a-nation-wide-public-holiday-to-honour-late-sir-mekere/)
        * [2021 Sir Michael Somare's Day of Mourning](https://web.archive.org/web/20241207121149/https://pnghausbung.com/2-weeks-mourning-period-for-late-sir-micheal-to-start-with-public-holiday-on-monday-pm/)
        * [2022 QE2's Funeral](https://web.archive.org/web/20220914111729/https://www.thenational.com.pg/column-1-553/)
        * [2023 Sir Rabbie Namaliu's Funeral](https://web.archive.org/web/20241109201443/https://pnghausbung.com/breaking-public-holiday-tomorrow-in-respect-of-late-sir-rabbies-state-funeral/)

    Nov 15, 2018 is an APEC Holiday too, but for Port Moresby-only:
    <https://web.archive.org/web/20250120182905/https://www.businessadvantagepng.com/public-holidays-declared-in-port-moresby-due-to-apec-summit-says-ceo/>

    Starting from 2021 afterwards all state funerals are given special public holidays, though only
    some are day-off nationwide.
    """

    # National Day of Mourning for Sir Michael Somare.
    sir_michael_somare_mourning_day = "National Day of Mourning for Sir Michael Somare"

    # Grand Chief Sir Michael Somare Remembrance Day
    sir_michael_somare_remembrance_day = "Grand Chief Sir Michael Somare Remembrance Day"

    special_public_holidays = {
        # APEC Leaders' Summit Public Holiday.
        2018: (NOV, 16, "APEC Leaders' Summit Public Holiday"),
        2021: (
            # State Funeral of Sir Mekere Morauta.
            (JAN, 8, "State Funeral of Sir Mekere Morauta"),
            (MAR, 1, sir_michael_somare_mourning_day),
            (MAR, 12, sir_michael_somare_mourning_day),
        ),
        # State Funeral of Queen Elizabeth II.
        2022: (SEP, 19, "State Funeral of Queen Elizabeth II"),
        # State Funeral of Sir Rabbie Namaliu.
        2023: (APR, 18, "State Funeral of Sir Rabbie Namaliu"),
    }

    special_public_holidays_observed = {
        2022: (FEB, 28, sir_michael_somare_remembrance_day),
        2023: (
            (FEB, 24, sir_michael_somare_remembrance_day),
            (JUN, 16, "King's Birthday"),
            (SEP, 15, "Independence Day"),
        ),
    }
