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
from gettext import gettext as tr

from holidays.calendars.gregorian import APR, SEP
from holidays.constants import GOVERNMENT, OPTIONAL, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    ALL_TO_NEAREST_MON,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
    SUN_TO_NEXT_MON,
    SUN_TO_NEXT_TUE,
)


class Canada(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Canada holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Canada>
        * <https://web.archive.org/web/20130703014214/http://www.hrsdc.gc.ca/eng/labour/overviews/employment_standards/holidays.shtml>
        * <https://web.archive.org/web/20250330082640/https://www.alberta.ca/alberta-general-holidays>
        * <https://web.archive.org/web/20250403044407/https://www2.gov.bc.ca/gov/content/employment-business/employment-standards-advice/employment-standards/statutory-holidays>
        * <https://web.archive.org/web/20250427131243/https://web2.gov.mb.ca/laws/statutes/ccsm/r120e.php>
        * <https://web.archive.org/web/20250112160822/https://www2.gnb.ca/content/gnb/en/departments/elg/local_government/content/governance/content/days_of_rest_act.html>
        * <https://web.archive.org/web/20250405170509/https://www.ontario.ca/document/your-guide-employment-standards-act-0/public-holidays>
        * <https://archive.org/details/nunavut-day-designated-as-a-general-holiday-start-date>
        * <https://web.archive.org/web/20250116143344/https://www.officeholidays.com/countries/canada>
        * <https://web.archive.org/web/20250425120843/https://www.timeanddate.com/holidays/canada>
        * <https://web.archive.org/web/20250122122256/https://www.warmuseum.ca/firstworldwar/history/after-the-war/remembrance/remembrance-day/>
        * <https://web.archive.org/web/20250428153936/https://www.thecanadianencyclopedia.ca/en/article/thanksgiving-day>
        * <https://web.archive.org/web/20250428154427/https://recherche-collection-search.bac-lac.gc.ca/eng/home/record?idnumber=9326&app=diawlmking&ecopy=80003QJW>
        * <https://web.archive.org/web/20240915001506/https://www.britannica.com/topic/Victoria-Day>
        * [NT National Aboriginal Day](https://web.archive.org/web/20160623071755/http://www.daair.gov.nt.ca/_live/pages/wpPages/National_Aboriginal_Day.aspx)
        * [MB National Day for Truth and Reconciliation](https://web.archive.org/web/20240714223654/https://web2.gov.mb.ca/bills/43-1/b004e.php)
    """

    country = "CA"
    default_language = "en_CA"
    # %s (observed).
    observed_label = tr("%s (observed)")
    start_year = 1867
    subdivisions = (
        "AB",  # Alberta.
        "BC",  # British Columbia (Colombie-Britannique).
        "MB",  # Manitoba.
        "NB",  # New Brunswick (Nouveau-Brunswick).
        "NL",  # Newfoundland and Labrador (Terre-Neuve-et-Labrador).
        "NS",  # Nova Scotia (Nouvelle-Écosse).
        "NT",  # Northwest Territories (Territoires du Nord-Ouest).
        "NU",  # Nunavut.
        "ON",  # Ontario.
        "PE",  # Prince Edward Island (Île-du-Prince-Édouard).
        "QC",  # Quebec (Québec).
        "SK",  # Saskatchewan.
        "YT",  # Yukon.
    )
    subdivisions_aliases = {
        "Alberta": "AB",
        "British Columbia": "BC",
        "Colombie-Britannique": "BC",
        "Manitoba": "MB",
        "New Brunswick": "NB",
        "Nouveau-Brunswick": "NB",
        "Newfoundland and Labrador": "NL",
        "Terre-Neuve-et-Labrador": "NL",
        "Nova Scotia": "NS",
        "Nouvelle-Écosse": "NS",
        "Northwest Territories": "NT",
        "Territoires du Nord-Ouest": "NT",
        "Nunavut": "NU",
        "Ontario": "ON",
        "Prince Edward Island": "PE",
        "Île-du-Prince-Édouard": "PE",
        "Quebec": "QC",
        "Québec": "QC",
        "Saskatchewan": "SK",
        "Yukon": "YT",
    }
    supported_categories = (GOVERNMENT, OPTIONAL, PUBLIC)
    supported_languages = ("ar", "en_CA", "en_US", "fr", "th")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, CanadaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _add_statutory_holidays(self):
        """Nationwide statutory holidays."""

        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        if self._year >= 1879:
            self._canada_day = self._add_holiday_jul_1(
                # Canada Day.
                tr("Canada Day")
                if self._year >= 1983
                # Dominion Day.
                else tr("Dominion Day")
            )

        if self._year >= 1894:
            # Labor Day.
            self._add_holiday_1st_mon_of_sep(tr("Labour Day"))

        # Christmas Day.
        self._add_christmas_day(tr("Christmas Day"))

    def _populate_public_holidays(self):
        self._add_statutory_holidays()

        self._add_observed(self._christmas_day)

    def _populate_government_holidays(self):
        """Holidays for federally regulated workplaces."""

        self._add_statutory_holidays()

        self._add_victoria_day()

        if self._year >= 1879:
            self._add_observed(self._canada_day)

        if self._year >= 2021:
            self._add_observed(
                # National Day for Truth and Reconciliation.
                self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))
            )

        self._add_thanksgiving_day()

        if self._year >= 1931:
            # Remembrance Day.
            self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

        self._add_observed(self._christmas_day, rule=SAT_SUN_TO_NEXT_MON_TUE)

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

    def _populate_optional_holidays(self):
        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

    def _add_thanksgiving_day(self) -> None:
        """Adds Thanksgiving Day / Armistice Day.

        In 1921, Thanksgiving Day was moved to "Armistice Day" (1st Monday in the week of Nov 11).
        "Remembrance Day" and "Thanksgiving Day" split again in 1931, with Thanksgiving usually on
        the 2nd Monday of October — except in 1935, when it was delayed 10 days due to a General
        Election. It was finally fixed to the 2nd Monday of October permanently in 1957.
        """
        if self._year >= 1931:
            # Thanksgiving Day.
            name = tr("Thanksgiving Day")
            if self._year == 1935:
                self._add_holiday_oct_24(name)
            else:
                self._add_holiday_2nd_mon_of_oct(name)
        elif self._year >= 1921:
            # Armistice Day.
            self._add_holiday_1st_mon_before_nov_12(tr("Armistice Day"))

    def _add_victoria_day(self) -> None:
        """Adds Victoria Day.

        After Queen Victoria's death in 1901, an act of the Canadian Parliament established
        Victoria Day as a legal holiday, to be celebrated on May 24 (or on May 25 when
        May 24 fell on a Sunday). This was later moved to the Monday preceding May 24 in 1952.
        """
        # Victoria Day.
        name = tr("Victoria Day")
        if self._year >= 1953:
            self._add_holiday_1st_mon_before_may_24(name)
        elif self._year >= 1901:
            self._add_observed(self._add_holiday_may_24(name), rule=SUN_TO_NEXT_MON)

    def _populate_subdiv_ab_public_holidays(self):
        if self._year >= 1990:
            # Family Day.
            self._add_holiday_3rd_mon_of_feb(tr("Family Day"))

        self._add_victoria_day()

        if self._year >= 1879:
            self._add_observed(self._canada_day, rule=SUN_TO_NEXT_MON)

        self._add_thanksgiving_day()

        if self._year >= 1931:
            # Remembrance Day.
            self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

    def _populate_subdiv_ab_optional_holidays(self):
        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # https://en.wikipedia.org/wiki/Civic_Holiday#Alberta
        if self._year >= 1974:
            # Heritage Day.
            self._add_holiday_1st_mon_of_aug(tr("Heritage Day"))

        if self._year >= 2021:
            # National Day for Truth and Reconciliation.
            self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))

        # Boxing Day.
        self._add_christmas_day_two(tr("Boxing Day"))

    def _populate_subdiv_bc_public_holidays(self):
        if self._year >= 2013:
            # Family Day.
            name = tr("Family Day")
            if self._year >= 2019:
                self._add_holiday_3rd_mon_of_feb(name)
            else:
                self._add_holiday_2nd_mon_of_feb(name)

        self._add_victoria_day()

        if self._year >= 1879:
            self._add_observed(self._canada_day, rule=SUN_TO_NEXT_MON)

        # https://en.wikipedia.org/wiki/Civic_Holiday#British_Columbia
        if self._year >= 1974:
            # British Columbia Day.
            self._add_holiday_1st_mon_of_aug(tr("British Columbia Day"))

        if self._year >= 2023:
            # National Day for Truth and Reconciliation.
            self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))

        self._add_thanksgiving_day()

        if self._year >= 1931:
            # Remembrance Day.
            self._add_remembrance_day(tr("Remembrance Day"))

    def _populate_subdiv_mb_public_holidays(self):
        if self._year >= 2008:
            # Louis Riel Day.
            self._add_holiday_3rd_mon_of_feb(tr("Louis Riel Day"))

        self._add_victoria_day()

        if self._year >= 2024:
            self._add_observed(
                # National Day for Truth and Reconciliation.
                self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))
            )

        self._add_thanksgiving_day()

    def _populate_subdiv_mb_optional_holidays(self):
        if self._year >= 1900:
            self._add_holiday_1st_mon_of_aug(
                # Terry Fox Day.
                tr("Terry Fox Day")
                if self._year >= 2015
                # Civic Holiday.
                else tr("Civic Holiday")
            )

        if self._year >= 1931:
            # Remembrance Day.
            self._add_remembrance_day(tr("Remembrance Day"))

    def _populate_subdiv_nb_public_holidays(self):
        if self._year >= 2018:
            # Family Day.
            self._add_holiday_3rd_mon_of_feb(tr("Family Day"))

        # https://en.wikipedia.org/wiki/Civic_Holiday#New_Brunswick
        if self._year >= 1975:
            # New Brunswick Day.
            self._add_holiday_1st_mon_of_aug(tr("New Brunswick Day"))

        if self._year >= 1931:
            # Remembrance Day.
            self._add_remembrance_day(tr("Remembrance Day"))

    def _populate_subdiv_nb_optional_holidays(self):
        self._add_victoria_day()

        self._add_thanksgiving_day()

        # Boxing Day.
        self._add_christmas_day_two(tr("Boxing Day"))

    def _populate_subdiv_nl_public_holidays(self):
        if self._year >= 1917:
            # Memorial Day.
            self._add_holiday_jul_1(tr("Memorial Day"))

        if self._year >= 1879:
            self._add_observed(self._canada_day)

        if self._year >= 1931:
            # Remembrance Day.
            self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

    def _populate_subdiv_nl_optional_holidays(self):
        if self._year >= 1900:
            self._move_holiday_forced(
                # Saint Patrick's Day.
                self._add_saint_patricks_day(tr("Saint Patrick's Day")),
                rule=ALL_TO_NEAREST_MON,
            )

        if self._year >= 1990:
            # Nearest Monday to April 23
            # 4/26 is the Monday closer to 4/23 in 2010
            # but the holiday was observed on 4/19? Crazy Newfies!

            # Saint George's Day.
            name = tr("Saint George's Day")

            if self._year == 2010:
                self._add_holiday_apr_19(name)
            else:
                # Prevents overlap with Good Friday.
                self._add_holiday(
                    name,
                    self._get_observed_date(date(self._year, APR, 23), rule=ALL_TO_NEAREST_MON),
                )

        if self._year >= 1997:
            self._move_holiday_forced(
                # Discovery Day.
                self._add_holiday_jun_24(tr("Discovery Day")),
                rule=ALL_TO_NEAREST_MON,
            )

        if self._year >= 1900:
            self._move_holiday_forced(
                # Orangemen's Day.
                self._add_holiday_jul_12(tr("Orangemen's Day")),
                rule=ALL_TO_NEAREST_MON,
            )

        self._add_thanksgiving_day()

        # Boxing Day.
        self._add_christmas_day_two(tr("Boxing Day"))

    def _populate_subdiv_ns_public_holidays(self):
        # https://web.archive.org/web/20240611215326/https://novascotia.ca/lae/employmentrights/NovaScotiaHeritageDay.asp
        if self._year >= 2015:
            # Heritage Day.
            self._add_holiday_3rd_mon_of_feb(tr("Heritage Day"))

        if self._year >= 1981:
            # Remembrance Day.
            self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

    def _populate_subdiv_ns_optional_holidays(self):
        if self._year >= 1996:
            # Natal Day.
            self._add_holiday_1st_mon_of_aug(tr("Natal Day"))

    def _populate_subdiv_nt_public_holidays(self):
        self._add_victoria_day()

        if self._year >= 2001:
            # National Aboriginal Day.
            self._add_holiday_jun_21(tr("National Aboriginal Day"))

        if self._year >= 1900:
            # Civic Holiday.
            self._add_holiday_1st_mon_of_aug(tr("Civic Holiday"))

        if self._year >= 2022:
            # National Day for Truth and Reconciliation.
            self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))

        self._add_thanksgiving_day()

        if self._year >= 1931:
            # Remembrance Day.
            self._add_remembrance_day(tr("Remembrance Day"))

    def _populate_subdiv_nu_public_holidays(self):
        self._add_victoria_day()

        if self._year >= 2020:
            # Nunavut Day.
            self._add_holiday_jul_9(tr("Nunavut Day"))

        if self._year >= 1900:
            # Civic Holiday.
            self._add_holiday_1st_mon_of_aug(tr("Civic Holiday"))

        if self._year >= 2022:
            # National Day for Truth and Reconciliation.
            self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))

        self._add_thanksgiving_day()

        if self._year >= 1931:
            # Remembrance Day.
            self._add_remembrance_day(tr("Remembrance Day"))

    def _populate_subdiv_nu_optional_holidays(self):
        if 2000 <= self._year <= 2019:
            # Nunavut Day.
            name = tr("Nunavut Day")
            if self._year == 2000:
                self._add_holiday_apr_1(name)
            else:
                self._add_holiday_jul_9(name)

    def _populate_subdiv_on_public_holidays(self):
        if self._year >= 2008:
            # Family Day.
            self._add_holiday_3rd_mon_of_feb(tr("Family Day"))

        self._add_victoria_day()

        self._add_thanksgiving_day()

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two(tr("Boxing Day")), rule=SUN_TO_NEXT_TUE)

    def _populate_subdiv_on_optional_holidays(self):
        if self._year >= 1900:
            # Civic Holiday.
            self._add_holiday_1st_mon_of_aug(tr("Civic Holiday"))

    def _populate_subdiv_pe_public_holidays(self):
        if self._year >= 2009:
            # Islander Day.
            name = tr("Islander Day")
            if self._year >= 2010:
                self._add_holiday_3rd_mon_of_feb(name)
            else:
                self._add_holiday_2nd_mon_of_feb(name)

        if self._year >= 1879:
            self._add_observed(self._canada_day)

        if self._year >= 2022:
            # National Day for Truth and Reconciliation.
            self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))

        if self._year >= 1931:
            # Remembrance Day.
            self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

    def _populate_subdiv_qc_public_holidays(self):
        if self._year >= 2003:
            # National Patriots' Day.
            self._add_holiday_1st_mon_before_may_24(tr("National Patriots' Day"))

        if self._year >= 1925:
            self._add_observed(
                # Saint John the Baptist Day.
                self._add_saint_johns_day(tr("Saint Jean Baptiste Day")),
                rule=SUN_TO_NEXT_MON,
            )

        if self._year >= 1879:
            self._add_observed(self._canada_day, rule=SUN_TO_NEXT_MON)

        self._add_thanksgiving_day()

    def _populate_subdiv_qc_optional_holidays(self):
        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

    def _populate_subdiv_sk_public_holidays(self):
        if self._year >= 2007:
            # Family Day.
            self._add_holiday_3rd_mon_of_feb(tr("Family Day"))

        self._add_victoria_day()

        if self._year >= 1879:
            self._add_observed(self._canada_day)

        # https://en.wikipedia.org/wiki/Civic_Holiday#Saskatchewan
        if self._year >= 1900:
            # Saskatchewan Day.
            self._add_holiday_1st_mon_of_aug(tr("Saskatchewan Day"))

        self._add_thanksgiving_day()

        if self._year >= 1931:
            # Remembrance Day.
            self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

    def _populate_subdiv_yt_public_holidays(self):
        self._add_victoria_day()

        if self._year >= 2017:
            # National Aboriginal Day.
            self._add_holiday_jun_21(tr("National Aboriginal Day"))

        if self._year >= 1879:
            self._add_observed(self._canada_day)

        if self._year >= 1912:
            # Discovery Day.
            self._add_holiday_3rd_mon_of_aug(tr("Discovery Day"))

        if self._year >= 2023:
            # National Day for Truth and Reconciliation.
            self._add_holiday_sep_30(tr("National Day for Truth and Reconciliation"))

        self._add_thanksgiving_day()

        if self._year >= 1931:
            # Remembrance Day.
            self._add_observed(self._add_remembrance_day(tr("Remembrance Day")))

    def _populate_subdiv_yt_optional_holidays(self):
        # Friday before the last Sunday in February
        if self._year >= 1976:
            # Heritage Day.
            self._add_holiday_2_days_prior_last_sun_of_feb(tr("Heritage Day"))


class CA(Canada):
    pass


class CAN(Canada):
    pass


class CanadaStaticHolidays:
    # Funeral of Queen Elizabeth II.
    queen_funeral = tr("Funeral of Her Majesty the Queen Elizabeth II")

    special_bc_public_holidays = {
        2022: (SEP, 19, queen_funeral),
    }

    special_nb_public_holidays = {
        2022: (SEP, 19, queen_funeral),
    }

    special_nl_public_holidays = {
        2022: (SEP, 19, queen_funeral),
    }

    special_ns_public_holidays = {
        2022: (SEP, 19, queen_funeral),
    }

    special_pe_public_holidays = {
        2022: (SEP, 19, queen_funeral),
    }

    special_yt_public_holidays = {
        2022: (SEP, 19, queen_funeral),
    }
