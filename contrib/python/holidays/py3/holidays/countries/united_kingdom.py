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

from holidays.calendars.gregorian import APR, MAY, JUN, JUL, SEP, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class UnitedKingdom(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """United Kingdom holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_United_Kingdom>
        * <https://archive.org/details/treatiseonbanki00walk/page/334/mode/2up>
        * <https://web.archive.org/web/20250426173714/https://www.gov.uk/bank-holidays>
        * <https://web.archive.org/web/20250423223001/https://www.timeanddate.com/holidays/uk>

    The Anniversary of the Battle of the Boyne bank holiday is proclaimed annually by the
    Secretary of State for Northern Ireland.

    In-Lieu observance was first provided in the Bank Holidays Extension Act 1875.
    """

    country = "GB"
    default_language = "en_GB"
    # %s (observed).
    observed_label = tr("%s (observed)")
    subdivisions: tuple[()] | tuple[str, ...] = (
        "ENG",  # England
        "NIR",  # Northern Ireland
        "SCT",  # Scotland
        "WLS",  # Wales
    )
    subdivisions_aliases = {
        "England": "ENG",
        "Northern Ireland": "NIR",
        "Scotland": "SCT",
        "Wales": "WLS",
    }
    supported_languages = ("en_GB", "en_US", "th")
    # Bank Holidays Act 1871.
    start_year = 1872
    _deprecated_subdivisions = ("UK",)

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, UnitedKingdomStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        # Bank Holidays Extension Act 1875.
        kwargs.setdefault("observed_since", 1875)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self) -> None:
        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        if self._year >= 1978:
            # May Day.
            name = tr("May Day")
            if self._year in {1995, 2020}:
                self._add_holiday_may_8(name)
            else:
                self._add_holiday_1st_mon_of_may(name)

        if self._year >= 1971:
            spring_bank_dates = {
                2002: (JUN, 4),
                2012: (JUN, 4),
                2022: (JUN, 2),
            }
            # Spring Bank Holiday.
            name = tr("Spring Bank Holiday")
            if dt := spring_bank_dates.get(self._year):
                self._add_holiday(name, dt)
            else:
                self._add_holiday_last_mon_of_may(name)

    def _populate_common(self):
        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        if self._year >= 1971:
            # Late Summer Bank Holiday.
            self._add_holiday_last_mon_of_aug(tr("Late Summer Bank Holiday"))
        else:
            # Whit Monday.
            self._add_whit_monday(tr("Whit Monday"))

    def _populate_subdiv_holidays(self):
        if self.subdiv != "SCT":
            if self._year >= 1975:
                # New Year's Day.
                self._add_observed(self._add_new_years_day(tr("New Year's Day")))

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

        super()._populate_subdiv_holidays()

    def _populate_subdiv_eng_public_holidays(self):
        self._populate_common()

    def _populate_subdiv_nir_public_holidays(self):
        self._populate_common()

        if self._year >= 1903:
            # Saint Patrick's Day.
            self._add_observed(self._add_saint_patricks_day(tr("Saint Patrick's Day")))

        # Battle of the Boyne.
        self._add_observed(self._add_holiday_jul_12(tr("Battle of the Boyne")))

    def _populate_subdiv_sct_public_holidays(self):
        # New Year's Day.
        jan_1 = self._add_new_years_day(tr("New Year's Day"))

        self._add_observed(
            # New Year Holiday.
            self._add_new_years_day_two(tr("New Year Holiday")),
            rule=SAT_SUN_TO_NEXT_MON_TUE + MON_TO_NEXT_TUE,
        )
        self._add_observed(jan_1)

        # Summer Bank Holiday.
        self._add_holiday_1st_mon_of_aug(tr("Summer Bank Holiday"))

        if self._year >= 2006:
            # Saint Andrew's Day.
            self._add_observed(self._add_holiday_nov_30(tr("Saint Andrew's Day")))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE if self._year >= 1974 else SAT_SUN_TO_NEXT_MON,
        )

        if self._year >= 1974:
            self._add_observed(
                # Boxing Day.
                self._add_christmas_day_two(tr("Boxing Day")),
                rule=SAT_SUN_TO_NEXT_MON_TUE,
            )

    def _populate_subdiv_wls_public_holidays(self):
        self._populate_common()


class UK(UnitedKingdom):
    pass


class GB(UnitedKingdom):
    pass


class GBR(UnitedKingdom):
    pass


class UnitedKingdomStaticHolidays:
    """United Kingdom special holidays.

    References:
        <https://web.archive.org/web/20260214143756/https://privycouncil.independent.gov.uk/wp-content/uploads/2026/02/Scotland-Bank-Holiday-Proclamation.pdf>
    """

    special_public_holidays = {
        # Silver Jubilee of Elizabeth II.
        1977: (JUN, 7, tr("Silver Jubilee of Elizabeth II")),
        # Wedding of Charles and Diana.
        1981: (JUL, 29, tr("Wedding of Charles and Diana")),
        # Millennium Celebrations.
        1999: (DEC, 31, tr("Millennium Celebrations")),
        # Golden Jubilee of Elizabeth II.
        2002: (JUN, 3, tr("Golden Jubilee of Elizabeth II")),
        # Wedding of William and Catherine.
        2011: (APR, 29, tr("Wedding of William and Catherine")),
        # Diamond Jubilee of Elizabeth II.
        2012: (JUN, 5, tr("Diamond Jubilee of Elizabeth II")),
        2022: (
            # Platinum Jubilee of Elizabeth II.
            (JUN, 3, tr("Platinum Jubilee of Elizabeth II")),
            # State Funeral of Queen Elizabeth II.
            (SEP, 19, tr("State Funeral of Queen Elizabeth II")),
        ),
        # Coronation of Charles III.
        2023: (MAY, 8, tr("Coronation of Charles III")),
    }

    special_sct_public_holidays = {
        # Scotland's participation in the FIFA World Cup final.
        2026: (JUN, 15, tr("Scotland's participation in the FIFA World Cup final"))
    }
