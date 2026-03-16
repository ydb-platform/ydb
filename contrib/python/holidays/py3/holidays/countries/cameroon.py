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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_WORKDAY


class Cameroon(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Cameroon holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Cameroon>
        * <https://web.archive.org/web/20250408001410/https://www.timeanddate.com/holidays/cameroon/>
        * <https://web.archive.org/web/20231004010829/https://www.officeholidays.com/countries/cameroon>
    """

    country = "CM"
    # %s (estimated).
    estimated_label = "%s (estimated)"
    # %s (observed, estimated).
    observed_estimated_label = "%s (observed, estimated)"
    # %s (observed).
    observed_label = "%s (observed)"
    # On 1 January 1960, French Cameroun gained independence from France.
    start_year = 1960

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=CameroonIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=CameroonStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day("New Year's Day"))

        # Youth Day.
        if self._year >= 1966:
            dts_observed.add(self._add_holiday_feb_11("Youth Day"))

        # Good Friday.
        self._add_good_friday("Good Friday")

        # Labour Day.
        dts_observed.add(self._add_labor_day("Labour Day"))

        # National Day.
        if self._year >= 1972:
            dts_observed.add(self._add_holiday_may_20("National Day"))

        # Ascension Day.
        self._add_ascension_thursday("Ascension Day")

        # Assumption Day.
        dts_observed.add(self._add_assumption_of_mary_day("Assumption Day"))

        # Christmas Day.
        dts_observed.add(self._add_christmas_day("Christmas Day"))

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day("Eid al-Fitr"))

        # Eid al-Adha.
        dts_observed.update(self._add_eid_al_adha_day("Eid al-Adha"))

        # Mawlid.
        dts_observed.update(self._add_mawlid_day("Mawlid"))

        if self.observed:
            self._populate_observed(dts_observed)


class CM(Cameroon):
    pass


class CMR(Cameroon):
    pass


class CameroonIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2023)
    EID_AL_ADHA_DATES = {
        2001: (MAR, 6),
        2002: (FEB, 23),
        2003: (FEB, 12),
        2004: (FEB, 2),
        2008: (DEC, 9),
        2009: (NOV, 28),
        2010: (NOV, 17),
        2011: (NOV, 7),
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2024)
    EID_AL_FITR_DATES = {
        2001: (DEC, 17),
        2002: (DEC, 6),
        2003: (NOV, 26),
        2005: (NOV, 4),
        2006: (OCT, 24),
        2008: (OCT, 2),
        2009: (SEP, 21),
        2011: (AUG, 31),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2001, 2022)
    MAWLID_DATES = {
        2003: (MAY, 14),
        2004: (MAY, 2),
        2006: (APR, 11),
        2011: (FEB, 16),
        2012: (FEB, 5),
        2014: (JAN, 14),
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2018: (NOV, 21),
        2019: (NOV, 10),
        2021: (OCT, 19),
    }


class CameroonStaticHolidays:
    special_public_holidays = {
        2021: (
            (MAY, 14, "Public Holiday"),
            (JUL, 19, "Public Holiday"),
        ),
    }

    special_public_holidays_observed = {
        2007: (JAN, 2, "Eid al-Adha"),
    }
