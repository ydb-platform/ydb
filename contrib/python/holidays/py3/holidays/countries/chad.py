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
from holidays.calendars.gregorian import JAN, APR, JUN, JUL, AUG, SEP, NOV, DEC
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Chad(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Chad holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Chad>
        * <https://web.archive.org/web/20240619220557/https://www.ilo.org/dyn/natlex/docs/ELECTRONIC/97323/115433/F-316075167/TCD-97323.pdf>
    """

    country = "TD"
    # %s (estimated).
    estimated_label = "%s (estimated)"
    # %s (observed, estimated).
    observed_estimated_label = "%s (observed, estimated)"
    # %s (observed).
    observed_label = "%s (observed)"
    # On 11 August 1960, Chad gained independence from France.
    start_year = 1961

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
            self, cls=ChadIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, ChadStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day("New Year's Day"))

        # International Women's Day.
        self._add_observed(self._add_womens_day("International Women's Day"))

        # Easter Monday.
        self._add_easter_monday("Easter Monday")

        # Labour Day.
        self._add_observed(self._add_labor_day("Labour Day"))

        # Independence Day.
        self._add_observed(self._add_holiday_aug_11("Independence Day"))

        # All Saints' Day.
        self._add_all_saints_day("All Saints' Day")

        # Republic Day.
        self._add_observed(self._add_holiday_nov_28("Republic Day"))

        if self._year >= 1991:
            # Freedom and Democracy Day.
            self._add_observed(self._add_holiday_dec_1("Freedom and Democracy Day"))

        # Christmas Day.
        self._add_christmas_day("Christmas Day")

        # Eid al-Fitr.
        self._add_eid_al_fitr_day("Eid al-Fitr")

        # Eid al-Adha.
        self._add_eid_al_adha_day("Eid al-Adha")

        # Mawlid.
        self._add_mawlid_day("Mawlid")


class TD(Chad):
    pass


class TCD(Chad):
    pass


class ChadIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2015, 2023)
    EID_AL_ADHA_DATES = {
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2015, 2024)
    EID_AL_FITR_DATES = {
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2015, 2022)
    MAWLID_DATES = {
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2018: (NOV, 21),
    }


class ChadStaticHolidays:
    special_public_holidays = {
        2021: (APR, 23, "Funeral of Idriss Déby Itno"),
    }
