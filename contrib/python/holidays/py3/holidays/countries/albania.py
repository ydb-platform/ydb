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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, MAR
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_WORKDAY


class Albania(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Albania holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Albania>
        * [Law No. 7651](https://web.archive.org/web/20250119183539/http://kqk.gov.al/sites/default/files/publikime/ligj_7651_-_per_festat_zyrtare_e_ditet_perkujtimore.pdf)
        * [Holidays for 2018-2024](https://web.archive.org/web/20250119183537/https://www.bankofalbania.org/Shtypi/Kalendari_i_festave_zyrtare_2024/)
    """

    country = "AL"
    default_language = "sq"
    # %s (estimated).
    estimated_label = tr("%s (e vlerësuar)")
    # %s (observed).
    observed_label = tr("%s (ditë pushimi e shtyrë)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (ditë pushimi e shtyrë, e vlerësuar)")
    supported_languages = ("en_US", "sq", "uk")
    # Law No. 7651 from 21.12.1992.
    start_year = 1993

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
            self, cls=AlbaniaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, AlbaniaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        name = tr("Festat e Vitit të Ri")
        dts_observed.add(self._add_new_years_day(name))
        dts_observed.add(self._add_new_years_day_two(name))

        if self._year >= 2004:
            # Summer Day.
            dts_observed.add(self._add_holiday_mar_14(tr("Dita e Verës")))

        if self._year >= 1996:
            # Nowruz Day.
            dts_observed.add(self._add_holiday_mar_22(tr("Dita e Nevruzit")))

        # Catholic Easter Sunday.
        dts_observed.add(self._add_easter_sunday(tr("E diela e Pashkëve Katolike")))

        dts_observed.add(
            # Orthodox Easter Sunday.
            self._add_easter_sunday(tr("E diela e Pashkëve Ortodokse"), JULIAN_CALENDAR)
        )

        # International Workers' Day.
        dts_observed.add(self._add_labor_day(tr("Dita Ndërkombëtare e Punëtorëve")))

        if 2004 <= self._year <= 2017:
            # Mother Teresa Beatification Day.
            dts_observed.add(self._add_holiday_oct_19(tr("Dita e Lumturimit të Shenjt Terezës")))
        elif self._year >= 2018:
            # Mother Teresa Canonization Day.
            dts_observed.add(self._add_holiday_sep_5(tr("Dita e Shenjtërimit të Shenjt Terezës")))

        if self._year >= 2024:
            # Alphabet Day.
            dts_observed.add(self._add_holiday_nov_22(tr("Dita e Alfabetit")))

        # Flag and Independence Day.
        dts_observed.add(self._add_holiday_nov_28(tr("Dita Flamurit dhe e Pavarësisë")))

        # Liberation Day.
        dts_observed.add(self._add_holiday_nov_29(tr("Dita e Çlirimit")))

        if self._year >= 2009:
            # National Youth Day.
            dts_observed.add(self._add_holiday_dec_8(tr("Dita Kombëtare e Rinisë")))

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Krishtlindjet")))

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Dita e Bajramit të Madh")))

        # Eid al-Adha.
        dts_observed.update(self._add_eid_al_adha_day(tr("Dita e Kurban Bajramit")))

        if self.observed:
            self._populate_observed(dts_observed)


class AL(Albania):
    pass


class ALB(Albania):
    pass


class AlbaniaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2018, 2025)
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2018, 2025)


class AlbaniaStaticHolidays:
    # Public Holiday.
    public_holiday = tr("Ditë pushimi")

    special_public_holidays = {
        2020: (JAN, 3, public_holiday),
        2022: (MAR, 21, public_holiday),
        2024: (MAR, 15, public_holiday),
    }

    special_public_holidays_observed = {
        2007: (JAN, 3, tr("Dita e Kurban Bajramit")),
    }
