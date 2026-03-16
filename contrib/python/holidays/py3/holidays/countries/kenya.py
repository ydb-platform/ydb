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

from holidays.calendars import _CustomHinduHolidays, _CustomIslamicHolidays
from holidays.calendars.gregorian import FEB, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV
from holidays.constants import HINDU, ISLAMIC, PUBLIC
from holidays.groups import (
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_WORKDAY


class Kenya(
    ObservedHolidayBase,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Kenya holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Kenya>
        * [Public Holidays Act Cap. 110](https://web.archive.org/web/20250118013422/https://new.kenyalaw.org/akn/ke/act/1912/21/eng@2024-04-26)
        * [Constitution of Kenya (Art. 9)](https://web.archive.org/web/20250417082347/https://new.kenyalaw.org/akn/ke/act/2010/constitution/eng@2010-09-03)
    """

    country = "KE"
    default_language = "en_KE"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    # %s (observed).
    observed_label = tr("%s (observed)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observed, estimated)")
    supported_categories = (HINDU, ISLAMIC, PUBLIC)
    supported_languages = ("en_KE", "en_US", "sw")
    # Kenya gained independence on December 12, 1963.
    start_year = 1964

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=KenyaHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=KenyaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=KenyaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 1985)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("New Year's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Labor Day.
        dts_observed.add(self._add_labor_day(tr("Labour Day")))

        if self._year >= 2011:
            # Madaraka Day.
            dts_observed.add(self._add_holiday_jun_1(tr("Madaraka Day")))

        if 1990 <= self._year <= 2009 or self._year >= 2018:
            if self._year >= 2025:
                # Mazingira Day.
                name = tr("Mazingira Day")
            elif self._year >= 2021:
                # Utamaduni Day.
                name = tr("Utamaduni Day")
            else:
                # Moi Day.
                name = tr("Moi Day")
            dts_observed.add(self._add_holiday_oct_10(name))

        dts_observed.add(
            self._add_holiday_oct_20(
                # Mashujaa Day.
                tr("Mashujaa Day")
                if self._year >= 2011
                # Kenyatta Day.
                else tr("Kenyatta Day")
            )
        )

        dts_observed.add(
            self._add_holiday_dec_12(
                # Jamhuri Day.
                tr("Jamhuri Day")
                if self._year >= 2011
                # Independence Day.
                else tr("Independence Day")
            )
        )

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Christmas Day")))

        # Boxing Day.
        dts_observed.add(self._add_christmas_day_two(tr("Boxing Day")))

        # Eid-al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Idd-ul-Fitr")))

        if self.observed:
            self._populate_observed(dts_observed)

    def _populate_hindu_holidays(self):
        """Additional Hindu public holidays."""

        if self._year >= 1984:
            # Diwali.
            self._add_diwali(tr("Diwali"))

    def _populate_islamic_holidays(self):
        """Additional Islamic public holidays."""

        # Eid-al-Adha.
        self._add_eid_al_adha_day(tr("Idd-ul-Azha"))


class KE(Kenya):
    pass


class KEN(Kenya):
    pass


class KenyaHinduHolidays(_CustomHinduHolidays):
    DIWALI_DATES = {
        2014: (OCT, 22),
        2015: (NOV, 10),
        2016: (OCT, 29),
        2017: (OCT, 18),
        2018: (NOV, 6),
        2019: (OCT, 28),
        2020: (NOV, 14),
        2021: (NOV, 4),
        2022: (OCT, 24),
        2023: (NOV, 12),
        2024: (OCT, 31),
    }


class KenyaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2014, 2024)
    EID_AL_ADHA_DATES = {
        2014: (OCT, 6),
        2015: (SEP, 24),
        2016: (SEP, 12),
        2019: (AUG, 12),
        2022: (JUL, 11),
        2024: (JUN, 17),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2014, 2024)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2019: (JUN, 5),
        2020: (MAY, 25),
        2021: (MAY, 14),
        2022: (MAY, 3),
    }


class KenyaStaticHolidays:
    """Kenya special holidays.

    References:
        * <https://web.archive.org/web/20250413191908/https://new.kenyalaw.org/akn/ke/officialGazette/2015-11-24/129/eng@2015-11-24>
        * <https://web.archive.org/web/20250413191918/https://new.kenyalaw.org/akn/ke/officialGazette/2017-08-01/107/eng@2017-08-01>
        * <https://web.archive.org/web/20250413192055/https://new.kenyalaw.org/akn/ke/officialGazette/2017-10-19/156/eng@2017-10-19>
        * <https://web.archive.org/web/20250413192105/https://new.kenyalaw.org/akn/ke/officialGazette/2017-10-24/159/eng@2017-10-24>
        * <https://web.archive.org/web/20250413191954/https://new.kenyalaw.org/akn/ke/officialGazette/2017-11-23/174/eng@2017-11-23>
        * <https://web.archive.org/web/20250413192041/https://new.kenyalaw.org/akn/ke/officialGazette/gazette/2022-07-29/147/eng@2022-07-29>
        * <https://web.archive.org/web/20250413192054/https://new.kenyalaw.org/akn/ke/officialGazette/gazette/2022-09-08/182/eng@2022-09-08>
        * <https://web.archive.org/web/20250413192209/https://new.kenyalaw.org/akn/ke/officialGazette/gazette/2023-11-06/238/eng@2023-11-06>
        * <https://web.archive.org/web/20250413192043/https://new.kenyalaw.org/akn/ke/officialGazette/gazette/2024-05-08/61/eng@2024-05-08>
        * <https://web.archive.org/web/20250413192123/https://new.kenyalaw.org/akn/ke/officialGazette/2024-10-31/184/eng@2024-10-31>
    """

    # Election Day.
    election_day = tr("Election Day")

    # Inauguration Day.
    inauguration_day = tr("Inauguration Day")

    # Day of Mourning for Queen Elizabeth II.
    mourning_for_queen_elizabeth = tr("Day of Mourning for Queen Elizabeth II")

    # National Tree Growing Day.
    national_tree_growing_day = tr("National Tree Growing Day")

    special_public_holidays = {
        # Visit of Pope Francis to Kenya.
        2015: (NOV, 26, tr("Visit of Pope Francis to Kenya")),
        2017: (
            (AUG, 8, election_day),
            (OCT, 25, election_day),
            (OCT, 26, election_day),
            (NOV, 28, inauguration_day),
        ),
        # President Moi Memorial Day.
        2020: (FEB, 11, tr("President Moi Memorial Day")),
        2022: (
            # State Funeral for Former President Mwai Kibaki.
            (APR, 29, tr("State Funeral for Former President Mwai Kibaki")),
            (AUG, 9, election_day),
            (SEP, 10, mourning_for_queen_elizabeth),
            (SEP, 11, mourning_for_queen_elizabeth),
            (SEP, 12, mourning_for_queen_elizabeth),
            (SEP, 13, inauguration_day),
        ),
        2023: (NOV, 13, national_tree_growing_day),
        2024: (
            (MAY, 10, national_tree_growing_day),
            (NOV, 1, inauguration_day),
        ),
    }
