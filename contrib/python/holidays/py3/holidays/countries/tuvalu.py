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

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class Tuvalu(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Tuvalu holidays.

    References:
        * [Public holidays in Tuvalu](https://en.wikipedia.org/wiki/Public_holidays_in_Tuvalu)
        * [Today's and Upcoming Holidays in Tuvalu](https://web.archive.org/web/20250310164727/https://www.timeanddate.com/holidays/tuvalu/)
        * [Public Holidays (Amendment) Act 1990](https://web.archive.org/web/20250429073846/https://www.paclii.org/cgi-bin/sinodisp/tv/legis/num_act/pha1990243/pha1990243.html)
        * [Public Holidays Act 1](https://archive.org/details/tuvalu-public-holidays-act-revised-2008)
        * [Public Holidays (Amendment) Act 2018](https://archive.org/details/tuvalu-public-holidays-amendment-act-2018)
        * [Public Holidays (Amendment) Act 2020](https://archive.org/details/tuvalu-public-holidays-amendment-act-2020)
        * [Public Holidays Act 2](https://archive.org/details/tuvalu-public-holidays-act-revised-2022)
        * [Codes for the representation of names of countries and their subdivisions](https://www.iso.org/obp/ui/#iso:code:3166:TV)
        * [TUVALU-NEWS.TV](https://web.archive.org/web/20140915180104/http://www.tuvalu-news.tv/archives/2007/01/island_special_public_holidays.html)
    """

    country = "TV"
    default_language = "tvl"
    # %s (observed).
    observed_label = tr("%s (fakamatakuga)")
    # Tuvalu became fully independent of the United Kingdom on October 1, 1978
    # Tuvalu's PUBLIC HOLIDAYS (AMENDMENT) ACT 1990 (Act 2 of 1990)
    # It was first proclaimed on FEB 7th, 1990
    start_year = 1990
    subdivisions = (
        "FUN",  # Funafuti.
        "NIT",  # Niutao.
        "NKF",  # Nukufetau.
        "NKL",  # Nukulaelae.
        "NMA",  # Nanumea.
        "NMG",  # Nanumaga.
        "NUI",  # Nui.
        "VAI",  # Vaitupu.
    )
    subdivisions_aliases = {
        # Town/Island Councils.
        "Funafuti": "FUN",
        "Niutao": "NIT",
        "Nukufetau": "NKF",
        "Nukulaelae": "NKL",
        "Nanumea": "NMA",
        "Nanumaga": "NMG",
        "Nanumanga": "NMG",  # Ex-ISO code alias
        "Nui": "NUI",
        "Vaitupu": "VAI",
    }
    supported_languages = ("en_GB", "en_US", "tvl")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("Tausaga Fou")))

        if self._year <= 2020:
            # Commonwealth Day.
            self._add_holiday_2nd_mon_of_mar(tr("Aso Atefenua"))

        # Good Friday.
        self._add_good_friday(tr("Aso toe tu"))

        # Easter Monday.
        self._add_easter_monday(tr("Toe Tu aso gafua"))

        # Gospel Day.
        self._add_holiday_1_day_past_2nd_sun_of_may(tr("Te Aso o te Tala Lei"))

        self._add_holiday_2nd_sat_of_jun(
            # King's Birthday.
            tr("Asofanau Tupu")
            if self._year >= 2023
            # Queen's Birthday.
            else tr("Asofanau Fafine")
        )

        # National Children's Day.
        name = tr("Aso Tamaliki")
        if self._year >= 2019:
            # National Youth Day.
            self._add_holiday_1st_mon_of_aug(tr("Aso tupulaga"))

            # National Children's Day.
            self._add_holiday_1_day_past_2nd_sun_of_oct(name)
        else:
            # National Children's Day.
            self._add_holiday_1st_mon_of_aug(name)

        # Tuvalu Day.
        name = tr("Tutokotasi")
        self._add_observed(self._add_holiday_oct_1(name), rule=SAT_SUN_TO_NEXT_MON_TUE)
        self._add_observed(self._add_holiday_oct_2(name), rule=SAT_SUN_TO_NEXT_MON_TUE)

        # https://en.wikipedia.org/wiki/King's_Official_Birthday#Tuvalu
        if self._year <= 2022:
            # Heir to the Throne's Birthday.
            self._add_holiday_2nd_mon_of_nov(tr("Aso fanau o te sui ote Tupu"))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Kilisimasi")), rule=SAT_SUN_TO_NEXT_MON_TUE)
        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Aso Faipele")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

    def _populate_subdiv_fun_public_holidays(self):
        # The Day of the Bombing.
        self._add_holiday_apr_23(tr("Te Aso o te Paula"))

        # Cyclone Day.
        self._add_holiday_oct_21(tr("Aso o te matagi"))

    def _populate_subdiv_nit_public_holidays(self):
        # Niutao Day.
        self._add_holiday_sep_17(tr("Te Aso o te Setema"))

    def _populate_subdiv_nkf_public_holidays(self):
        # Nukufetau Day.
        self._add_holiday_feb_11(tr("Te Aso O Tutasi"))

    def _populate_subdiv_nkl_public_holidays(self):
        # Gospel Day.
        self._add_holiday_may_10(tr("Te Aso o te Tala Lei"))

    def _populate_subdiv_nma_public_holidays(self):
        # Golden Jubilee.
        self._add_holiday_jan_8(tr("Te Po o Tefolaha"))

        # Big Day.
        self._add_holiday_feb_3(tr("Po Lahi"))

    def _populate_subdiv_nmg_public_holidays(self):
        # Nanumaga Day.
        self._add_holiday_apr_15(tr("Aho o te Fakavae"))

    def _populate_subdiv_nui_public_holidays(self):
        # Day of the Flood.
        self._add_holiday_feb_16(tr("Bogin te Ieka"))

    def _populate_subdiv_vai_public_holidays(self):
        # Happy Day.
        self._add_holiday_nov_25(tr("Te Aso Fiafia"))


class TV(Tuvalu):
    pass


class TUV(Tuvalu):
    pass
