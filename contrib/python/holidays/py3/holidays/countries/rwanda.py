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
from holidays.calendars.gregorian import MAR, JUN, JUL, AUG
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_NEXT_WORKDAY,
    SAT_SUN_TO_NEXT_WORKDAY,
)

if TYPE_CHECKING:
    from datetime import date


class Rwanda(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Rwanda holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Rwanda>
        * [Presidential Order N° 06/01 of 16/02/2011](https://web.archive.org/web/20250601161219/https://archive.gazettes.africa/archive/rw/2011/rw-government-gazette-dated-2011-03-07-no-10.pdf)
        * [Presidential Order N° 42/03 of 30/06/2015](https://web.archive.org/web/20180417062635/http://www.igihe.com/IMG/pdf/iteka_rya_perezida_rigena_iminsi_y_ikiruhuko_rusange.pdf)
        * [Presidential Order N° 54/01 of 24/02/2017](https://web.archive.org/web/20220626143357/https://www.ngoma.gov.rw/index.php?eID=dumpFile&t=f&f=44336&token=fc82c76109af7950f8895d40ddd3e15bd8c57e8c)
        * [Presidential Order N° 62/01 of 19/10/2022](https://web.archive.org/web/20250815032533/https://mifotra.prod.risa.rw/index.php?eID=dumpFile&t=f&f=85230&token=bcf0bf166638c11f4bf3d2f8c629052db26d38b6)
    """

    country = "RW"
    default_language = "rw"
    # %s (estimated).
    estimated_label = tr("%s (yagereranijwe)")
    # %s (observed).
    observed_label = tr("%s (yizihijwe)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (yizihijwe, yagereranijwe)")
    supported_languages = ("en_US", "fr", "rw")
    # Presidential Order N° 06/01 of 16/02/2011.
    start_year = 2012

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=RwandaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_since", 2017)
        super().__init__(*args, **kwargs)

    def _populate_observed(self, dts: set[date], *, multiple: bool = False) -> None:
        """
        Applies `SAT_TO_NEXT_WORKDAY` instead of `SAT_SUN_TO_NEXT_WORKDAY`
        observed_rule for Day after New Year's Day and Boxing Day.
        """
        special_cases = {
            # Day after New Year's Day.
            self.tr("Umunsi ukurikira Ubunani"),
            # Boxing Day.
            self.tr("Umunsi ukurikira Noheli"),
        }
        for dt in sorted(dts):
            for name in self.get_list(dt):
                self._add_observed(
                    dt,
                    name,
                    rule=SAT_TO_NEXT_WORKDAY if name in special_cases else SAT_SUN_TO_NEXT_WORKDAY,
                )

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dt = self._add_new_years_day(tr("Ubunani"))
        if self._year >= 2018:
            dts_observed.add(dt)

        # Added via Presidential Order N° 42/03 of 30/06/2015.
        if self._year >= 2016:
            # Day after New Year's Day.
            dts_observed.add(self._add_new_years_day_two(tr("Umunsi ukurikira Ubunani")))

        # National Heroes' Day.
        dts_observed.add(self._add_holiday_feb_1(tr("Umunsi w'Intwari")))

        # Memorial Day of Genocide perpetrated against the Tutsi in 1994.
        self._add_holiday_apr_7(tr("Umunsi wo Kwibuka Jenoside yakorewe Abatutsi mu 1994"))

        # Good Friday.
        self._add_good_friday(tr("Umunsi wa Gatanu Mutagatifu"))

        # Added via Presidential Order N° 54/01 of 24/02/2017.
        if self._year >= 2017:
            # Easter Monday.
            self._add_easter_monday(tr("Ku wa mbere wa Pasika"))

        # Labor Day.
        dts_observed.add(self._add_labor_day(tr("Umunsi Mukuru w'Umurimo")))

        # Independence Day.
        dts_observed.add(self._add_holiday_jul_1(tr("Umunsi w'Ubwigenge")))

        # Liberation Day.
        dts_observed.add(self._add_holiday_jul_4(tr("Umunsi wo Kwibohora")))

        # Added via Presidential Order N° 42/03 of 30/06/2015.
        if self._year >= 2015:
            # Umuganura Day.
            self._add_holiday_1st_fri_of_aug(tr("Umunsi w'Umuganura"))

        # Assumption Day.
        dts_observed.add(self._add_assumption_of_mary_day(tr("Ijyanwa mu Ijuru rya Bikiramariya")))

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Noheli")))

        # Boxing Day.
        dts_observed.add(self._add_christmas_day_two(tr("Umunsi ukurikira Noheli")))

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Eid El Fitr")))

        # Added via Presidential Order N° 42/03 of 30/06/2015.
        if self._year >= 2015:
            # Eid al-Adha.
            dts_observed.update(self._add_eid_al_adha_day(tr("Eid al-Adha")))

        if self.observed:
            self._populate_observed(dts_observed)


class RW(Rwanda):
    pass


class RWA(Rwanda):
    pass


class RwandaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2018, 2025)
    EID_AL_ADHA_DATES = {
        2018: (AUG, 22),
        2019: (AUG, 12),
        2022: (JUL, 11),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2012, 2025)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2025: (MAR, 31),
    }
