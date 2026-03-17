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

from __future__ import annotations

from gettext import gettext as tr
from typing import TYPE_CHECKING

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import (
    JAN,
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
    _timedelta,
)
from holidays.constants import BANK, PUBLIC
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)

if TYPE_CHECKING:
    from datetime import date


class Tanzania(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Tanzania holidays.

    References:
        * [Act No. 57 of 1964](https://web.archive.org/web/20250426030228/http://www.parliament.go.tz/polis/uploads/bills/acts/1565172859-The%20Public%20Holidays%20Ordinance%20(amendment)%20Act,%201964.pdf)
        * [Act No. 28 of 1966](https://web.archive.org/web/20241130122429/http://www.parliament.go.tz/polis/uploads/bills/acts/1565692654-The%20Public%20Holidays%20Ordinance%20(Amendment)%20Act,%201966.pdf)
        * [Regular Notice No. 115 of 1973](https://web.archive.org/web/20250726035131/https://archive.gazettes.africa/archive/tz/1973/tz-government-gazette-dated-1973-02-02-no-5.pdf)
        * [Act No. 1 of 1993](https://web.archive.org/web/20250425095011/https://www.parliament.go.tz/polis/uploads/bills/acts/1566639469-The%20Written%20Laws%20(Miscellaneous%20Amendments)%20Act,%201993.pdf)
        * [Act No. 10 of 1994](https://web.archive.org/web/20240515125908/https://www.parliament.go.tz/polis/uploads/bills/acts/1566638051-The%20Written%20Laws%20(Miscellaneous%20Amendments)%20(No.%202)%20Act,%201994.pdf)
        * [Supplements No. 39 of 2002](https://web.archive.org/web/20250726035133/https://media.tanzlii.org/media/legislation/375064/source_file/276a8691d29bd8a3/tz-act-gn-2002-451-publication-document.pdf)
        * [Act No. 25 of 2002](https://web.archive.org/web/20250427172313/http://parliament.go.tz/polis/uploads/bills/acts/1454076376-ActNo-25-2002.pdf)
        * [Government Notice No. 486 of 2022](https://web.archive.org/web/20250815171402/https://media.tanzlii.org/media/legislation/325046/source_file/aaaaa31bca89a995/tz-act-gn-2022-486-publication-document.pdf)
        * [The Public Holidays Act (as of 2025)](https://web.archive.org/web/20250726035253/https://www.nps.go.tz/uploads/documents/sw-1751397436-THE%20PUBLIC%20HOLIDAYS%20ACT.pdf)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Tanzania>
        * <https://web.archive.org/web/20250427125715/https://mytanzaniatimes.blogspot.com/2014/08/holiday-nane-nane-farmers-day-in.html>

    Checked With:
        * [2025](https://web.archive.org/web/20250214094413/https://www.bot.go.tz/webdocs/Other/PUBLIC%20HOLIDAYS%202025.pdf)
        * [2024](https://web.archive.org/web/20250403094322/https://www.bot.go.tz/webdocs/Other/2023%20public%20holidays.pdf)
        * [2023](https://web.archive.org/web/20230811104654/https://www.bot.go.tz/webdocs/Other/2023%20public%20holidays.pdf)
        * [2022](https://web.archive.org/web/20230123001009/https://www.bot.go.tz/webdocs/Other/PUBLIC%20HOLIDAYS%202022.pdf)
        * [2021](https://web.archive.org/web/20210812134750/https://www.bot.go.tz/webdocs/Other/PUBLIC%20HOLIDAYS%202021.pdf)
        * [2020 (2)](https://web.archive.org/web/20201219014125/https://www.bot.go.tz/webdocs/Other/PUBLIC%20HOLIDAYS%202020.pdf)
        * [2020 (1)](https://web.archive.org/web/20250717184907/https://archive.gazettes.africa/archive/tz/2019/tz-government-gazette-dated-2019-11-08-no-46.pdf)
        * [2018](https://web.archive.org/web/20250726041516/https://www.michuzi.co.tz/2017/11/sikukuu-za-kitaifa-zenye-mapumziko-kwa.html)
        * [2014](https://web.archive.org/web/20250726041327/https://archive.gazettes.africa/archive/tz/2013/tz-government-gazette-dated-2013-07-12-no-28.pdf)
        * [2013](https://web.archive.org/web/20250717184650/https://archive.gazettes.africa/archive/tz/2012/tz-government-gazette-dated-2012-11-30-no-48.pdf)
        * [2011](https://web.archive.org/web/20250726041328/https://archive.gazettes.africa/archive/tz/2010/tz-government-gazette-dated-2010-10-08-no-41.pdf)
        * [2010](https://web.archive.org/web/20250726041331/https://archive.gazettes.africa/archive/tz/2009/tz-government-gazette-dated-2009-09-25-no-39.pdf)
        * [2009](https://web.archive.org/web/20250726042634/https://archive.gazettes.africa/archive/tz/2008/tz-government-gazette-dated-2008-09-19-no-38.pdf)
        * [2007](https://web.archive.org/web/20250726035107/https://archive.gazettes.africa/archive/tz/2006/tz-government-gazette-dated-2006-09-29-no-39.pdf)
        * [2006](https://web.archive.org/web/20250726042507/https://archive.gazettes.africa/archive/tz/2005/tz-government-gazette-dated-2005-11-11-no-45.pdf)
        * [2004](https://web.archive.org/web/20250726042505/https://archive.gazettes.africa/archive/tz/2003/tz-government-gazette-dated-2003-10-24-no-43.pdf)
        * [2003](https://web.archive.org/web/20250726042558/https://archive.gazettes.africa/archive/tz/2002/tz-government-gazette-dated-2002-12-06-no-49.pdf)
        * [2002](https://web.archive.org/web/20250726042713/https://archive.gazettes.africa/archive/tz/2001/tz-government-gazette-dated-2001-09-28-no-39.pdf)
        * [2001](https://web.archive.org/web/20250726042632/https://archive.gazettes.africa/archive/tz/2000/tz-government-gazette-dated-2000-09-22-no-38.pdf)
        * [1998](https://web.archive.org/web/20250726042845/https://archive.gazettes.africa/archive/tz/1997/tz-government-gazette-dated-1997-10-17-no-42.pdf)
        * [1994](https://web.archive.org/web/20250726043005/https://archive.gazettes.africa/archive/tz/1993/tz-government-gazette-dated-1993-10-15-no-42.pdf)
        * [1992](https://web.archive.org/web/20250726043006/https://archive.gazettes.africa/archive/tz/1991/tz-government-gazette-dated-1991-08-30-no-35.pdf)
        * [1991](https://web.archive.org/web/20250726043011/https://archive.gazettes.africa/archive/tz/1990/tz-government-gazette-dated-1990-08-24-no-36.pdf)
        * [1987](https://web.archive.org/web/20250726043022/https://archive.gazettes.africa/archive/tz/1986/tz-government-gazette-dated-1986-10-10-no-41.pdf)
        * [1984](https://web.archive.org/web/20250726043037/https://archive.gazettes.africa/archive/tz/1983/tz-government-gazette-dated-1983-11-18-no-46.pdf)
        * [1983](https://web.archive.org/web/20250726042943/https://archive.gazettes.africa/archive/tz/1982/tz-government-gazette-dated-1982-11-12-no-46.pdf)
        * [1982](https://web.archive.org/web/20250726043116/https://archive.gazettes.africa/archive/tz/1981/tz-government-gazette-dated-1981-09-25-no-39.pdf)
        * [1975](https://web.archive.org/web/20250726043340/https://archive.gazettes.africa/archive/tz/1974/tz-government-gazette-dated-1974-10-25-no-43.pdf)
        * [1970](https://web.archive.org/web/20250717155730/https://archive.gazettes.africa/archive/tz/1969/tz-government-gazette-dated-1969-09-26-no-42.pdf)
        * [1969](https://web.archive.org/web/20250726043344/https://archive.gazettes.africa/archive/tz/1968/tz-government-gazette-dated-1968-11-29-no-53.pdf)
        * [1965](https://web.archive.org/web/20250717150107/https://archive.gazettes.africa/archive/tz/1964/tz-government-gazette-dated-1964-12-18-no-75.pdf)
        * <https://web.archive.org/web/20250726043450/https://www.timeanddate.com/holidays/tanzania/>
    """

    country = "TZ"
    supported_categories = (BANK, PUBLIC)
    default_language = "sw"
    # %s (estimated).
    estimated_label = tr("%s (makisio)")
    # %s (observed).
    observed_label = tr("Badala ya %s")
    # %s (observed, estimated).
    observed_estimated_label = tr("Badala ya %s (makisio)")
    supported_languages = ("en_US", "sw")
    # Public Holidays Ordinance (Amendment) Act, 1964.
    start_year = 1965

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In Tanzania, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self,
            cls=TanzaniaIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        StaticHolidays.__init__(self, TanzaniaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_observed(self, dts: set[date], dts_special: set[date]) -> None:
        """
        Applies `SAT_SUN_TO_NEXT_MON_TUE` instead of `SAT_SUN_TO_NEXT_MON`
        observed_rule for Christmas Day and the first day of Eid al-Fitr
        holidays, based on section 4 of the Ordinance.

        Added via Act No. 10 of 1994 on July 29th, 1994.
        Removed via G.N. No. 486 of 2022 on July 15th, 2022.

        For 1994 and 2022 edge cases, see `special_public_holidays_observed`.
        """
        if self._year <= 1994 or self._year >= 2022:
            return

        # If a holiday falls on Holy Saturday.
        holy_saturday = _timedelta(self._easter_sunday, -1)

        for dt in sorted(dts):
            if dt in dts_special:
                rule = SAT_SUN_TO_NEXT_MON_TUE
            elif dt == holy_saturday:
                rule = SAT_TO_NEXT_TUE
            else:
                rule = SAT_SUN_TO_NEXT_MON
            for name in self.get_list(dt):
                self._add_observed(dt, name, rule=rule)

    def _populate_bank_holidays(self):
        # Sikukuu ya Pasaka.
        # Status: In-Use.
        # Only observed by financial institutions.

        # Easter Sunday.
        self._add_easter_sunday(tr("Sikukuu ya Pasaka"))

    def _populate_public_holidays(self):
        dts_observed = set()
        dts_special = set()

        # Removed via Act No. 28 of 1966 on August 4th, 1966.
        # Readded in 1987.
        if self._year <= 1966 or self._year >= 1987:
            # New Year's Day.
            dts_observed.add(self._add_new_years_day(tr("Mwaka Mpya")))

        # Zanzibar Revolution Day.
        dts_observed.add(self._add_holiday_jan_12(tr("Mapinduzi ya Zanzibar")))

        # Added via Regular Notice No. 115 of 1973 on January 27th, 1973.
        # Name changed in 1977 with the merger of ASP and TANU into CCM.
        # Removed via Act No. 1 of 1993 on February 19th, 1993.
        if 1973 <= self._year <= 1993:
            self._add_holiday_feb_5(
                # CCM Party Founding Day.
                tr("Kuzaliwa kwa Chama cha Mapinduzi")
                if self._year >= 1977
                # Afro-Shirazi Party Founding Day.
                else tr("Kuzaliwa kwa ASP")
            )

        # Added in 2006.
        if self._year >= 2006:
            dts_observed.add(
                self._add_holiday_apr_7(
                    # The Sheikh Abeid Amani Karume Day.
                    tr(
                        "Siku ya kumbukumbu ya Rais wa Kwanza wa Serikali ya Mapinduzi Zanzibar "
                        "Sheikh Abeid Amani Karume"
                    )
                )
            )

        # Good Friday.
        self._add_good_friday(tr("Ijumaa Kuu"))

        # Easter Monday.
        self._add_easter_monday(tr("Jumatatu ya Pasaka"))

        dts_observed.add(
            self._add_holiday_apr_26(
                # Union Celebrations.
                tr("Muungano wa Tanzania")
                if self._year >= 1991
                # Union Day.
                else tr("Sikukuu ya Muungano")
            )
        )

        # International Workers' Day.
        dts_observed.add(self._add_labor_day(tr("Sikukuu ya Wafanyakazi Ulimwenguni")))

        # Renamed Peasants' Day in 1977.
        # Name reverted back, with English name changed to International Trade Fair in 1994.
        if self._year >= 1994:
            # International Trade Fair.
            name = tr("Sabasaba")
        elif self._year >= 1977:
            # Peasants' Day.
            name = tr("Sikukuu ya Wakulima")
        else:
            # Saba Saba Day.
            name = tr("Saba Saba")
        dts_observed.add(self._add_holiday_jul_7(name))

        # Made into its own holiday separate from Saba Saba Day in 1995.
        if self._year >= 1995:
            # Peasants' Day.
            dts_observed.add(self._add_holiday_aug_8(tr("Sikukuu ya Wakulima")))

        # Added via Supplements No. 39 of 2002 on October 4th, 2002.
        # Reaffirmed via Act No. 25 of 2002 on November 14th, 2002.
        if self._year >= 2002:
            dts_observed.add(
                self._add_holiday_oct_14(
                    # The Mwalimu Nyerere Day and Climax of the Uhuru Torch Race.
                    tr("Kumbukumbu ya Mwalimu Nyerere na Kilele cha mbio za Mwenge")
                )
            )

        # Independence and Republic Day.
        dts_observed.add(self._add_holiday_dec_9(tr("Uhuru na Jamhuri")))

        # Christmas Day.
        dt = self._add_christmas_day(tr("Kuzaliwa Kristo"))
        dts_observed.add(dt)
        dts_special.add(dt)

        # Removed via Act No. 28 of 1966 on August 4th, 1966
        # Readded via Act No. 1 of 1993 on February 19th, 1993.
        if self._year <= 1965 or self._year >= 1993:
            # Boxing Day.
            dts_observed.add(self._add_christmas_day_two(tr("Siku ya Kupeana Zawadi")))

        # Islamic Holidays.

        # The 1975 official calendar lists only one Eid al-Fitr, unlike other contemporary sources
        # (e.g., 1970, 1982). No legal changes have been found to explain this, so it is assumed
        # to be a clerical error.

        # Eid al-Fitr.
        name = tr("Eid El-Fitri")
        dts = self._add_eid_al_fitr_day(name)
        dts_observed.update(dts)
        dts_special.update(dts)
        dts_observed.update(self._add_eid_al_fitr_day_two(name))

        # Eid al-Adha.
        name = tr("Eid El Hajj")
        dts_observed.update(self._add_eid_al_adha_day(name))
        # Removed via Act No. 28 of 1966 on August 4th, 1966
        if self._year <= 1966:
            dts_observed.update(self._add_eid_al_adha_day_two(name))

        # Prophet's Birthday.
        dts_observed.update(self._add_mawlid_day(tr("Maulidi")))

        if self.observed:
            self._populate_observed(dts_observed, dts_special)


class TZ(Tanzania):
    pass


class TZA(Tanzania):
    pass


class TanzaniaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2013, 2025)
    EID_AL_ADHA_DATES = {
        # https://web.archive.org/web/20250717150107/https://archive.gazettes.africa/archive/tz/1964/tz-government-gazette-dated-1964-12-18-no-75.pdf
        1965: (APR, 13),
        # https://web.archive.org/web/20250717095924/https://archive.gazettes.africa/archive/tz/1967/tz-government-gazette-dated-1967-03-17-no-12.pdf
        1967: (MAR, 22),
        # https://web.archive.org/web/20250726040536/https://archive.gazettes.africa/archive/tz/1968/tz-government-gazette-dated-1968-03-01-no-9.pdf
        1968: (MAR, 10),
        1969: (FEB, 28),
        # https://web.archive.org/web/20250726040512/https://archive.gazettes.africa/archive/tz/1969/tz-government-gazette-dated-1969-09-26-no-42.pdf
        1970: (FEB, 17),
        1975: (DEC, 14),
        1982: (OCT, 5),
        1983: (SEP, 25),
        1984: (SEP, 9),
        1987: (AUG, 7),
        1991: (JUN, 21),
        1992: (JUN, 12),
        1994: (MAY, 20),
        1998: (APR, 7),
        2001: (MAR, 5),
        2003: (FEB, 12),
        2004: (FEB, 2),
        2006: (JAN, 9),
        2007: (JAN, 1),
        2013: (OCT, 15),
        2015: (SEP, 23),
        2016: (SEP, 13),
        2019: (AUG, 13),
        2020: (JUL, 31),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2013, 2025)
    EID_AL_FITR_DATES = {
        # https://web.archive.org/web/20250726040204/https://archive.gazettes.africa/archive/tz/1964/tz-government-gazette-dated-1964-12-18-no-75.pdf
        1965: (FEB, 3),
        # https://web.archive.org/web/20250726040214/https://archive.gazettes.africa/archive/tz/1967/tz-government-gazette-dated-1967-01-10-no-2.pdf
        1967: (JAN, 13),
        # https://web.archive.org/web/20250726040216/https://archive.gazettes.africa/archive/tz/1967/tz-government-gazette-dated-1967-12-30-no-58.pdf
        1968: (JAN, 2),
        1969: (DEC, 11),
        # 1970 Rough Estimate wasn't updated from last year figure.
        1975: (OCT, 7),
        1982: (JUL, 24),
        1983: (JUL, 13),
        1984: (JUL, 4),
        1987: (MAY, 29),
        1991: (APR, 15),
        1992: (APR, 5),
        1994: (MAR, 12),
        1998: (JAN, 29),
        2001: (DEC, 26),
        2003: (NOV, 27),
        2004: (OCT, 18),
        2006: (OCT, 29),
        2007: (OCT, 13),
        2013: (AUG, 8),
        2018: (JUN, 15),
        2020: (MAY, 24),
        2024: (APR, 10),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2013, 2025)
    MAWLID_DATES = {
        # https://web.archive.org/web/20250726040036/https://archive.gazettes.africa/archive/tz/1965/tz-government-gazette-dated-1965-02-12-no-7.pdf
        1965: (JUL, 12),
        # https://web.archive.org/web/20250726040035/https://archive.gazettes.africa/archive/tz/1967/tz-government-gazette-dated-1967-06-16-no-26.pdf
        1967: (JUN, 21),
        # https://web.archive.org/web/20250717095809/https://archive.gazettes.africa/archive/tz/1968/tz-government-gazette-dated-1968-06-07-no-23.pdf
        1968: (JUN, 10),
        1969: (MAY, 29),
        # 1970 Rough Estimate wasn't updated from last year figure.
        1975: (MAR, 25),
        1982: (JAN, 8),
        1983: (DEC, 20),
        1984: (DEC, 8),
        1987: (NOV, 4),
        1991: (OCT, 19),
        1992: (SEP, 10),
        1994: (AUG, 18),
        1998: (APR, 7),
        2001: (JUL, 6),
        2003: (MAY, 12),
        2004: (MAY, 2),
        2006: (APR, 10),
        2007: (MAR, 31),
        2013: (JAN, 24),
        2015: ((JAN, 3), (DEC, 24)),
        2020: (OCT, 29),
        2025: (SEP, 4),
    }


class TanzaniaStaticHolidays:
    """Tanzania special holidays.

    References:
        * [G.N. No. 2440 of 1967](https://web.archive.org/web/20250726035513/https://archive.gazettes.africa/archive/tz/1967/tz-government-gazette-dated-1967-11-28-no-50a.pdf)
        * <https://web.archive.org/web/20231209061602/https://www.theeastafrican.co.ke/tea/business/tanzania-declares-public-holiday-on-census-day-3918836>
        * <https://web.archive.org/web/20250413194112/https://www.dw.com/en/samia-suluhu-hassan-who-is-tanzanias-new-president/a-56932264>
    """

    # Tanzania General Election Day.
    tz_election_day = tr("Sikukuu ya Uchaguzi Mkuu wa Tanzania")

    # National Population and Housing Census Day.
    phc_census_day = tr("Siku ya Sensa ya Kitaifa ya Watu na Makazi")

    # John Pombe Magufuli's Funeral.
    john_magufuli_funeral = tr("Mazishi ya John Pombe Magufuli")

    special_public_holidays = {
        # Inauguration of East African Economic Community.
        1967: (DEC, 1, tr("Uzinduzi wa Jumuiya ya Kiuchumi ya Afrika Mashariki")),
        2002: (AUG, 25, phc_census_day),
        # John Pombe Magufuli Inauguration Day.
        2015: (NOV, 5, tr("Sikukuu ya Kuapishwa kwa John Pombe Magufuli")),
        2020: (OCT, 28, tz_election_day),
        2021: (
            (MAR, 22, john_magufuli_funeral),
            (MAR, 25, john_magufuli_funeral),
        ),
        2022: (AUG, 23, phc_census_day),
    }
    special_public_holidays_observed = {
        # Christmas Day.
        1994: (DEC, 27, tr("Kuzaliwa Kristo")),
        2022: (
            # New Year's Day.
            (JAN, 3, tr("Mwaka Mpya")),
            # International Workers' Day.
            (MAY, 2, tr("Sikukuu ya Wafanyakazi Ulimwenguni")),
            # Eid al-Adha.
            (JUL, 11, tr("Eid El Hajj")),
        ),
    }
