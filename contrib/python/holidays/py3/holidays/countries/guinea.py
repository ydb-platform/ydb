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
from holidays.calendars.gregorian import MAR, APR, JUN, JUL, AUG, SEP, NOV, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Guinea(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Guinea holidays.

    References:
        * [Decree No. 2022-0526](https://archive.org/details/d-2022-0526-prg-cnrd-221103-131021)
        * <https://web.archive.org/web/20250408180953/https://www.timeanddate.com/holidays/guinea/>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Guinea>
        * <https://web.archive.org/web/20241205013852/https://anydayguide.com/calendar/1878>
        * <https://web.archive.org/web/20250408180953/https://web.archive.org/web/20240622164935/https://www.timeanddate.com/holidays/guinea/second-republic-day>
        * <https://web.archive.org/web/20250408180953/https://web.archive.org/web/20240806075538/https://www.timeanddate.com/holidays/guinea/all-saints-day>

    According to Decree No. 2022-0526 of 2 November 2022:
        * Eid al-Adha became a two-day holiday (Article 1).
        * If New Year's Day, Independence Day or Eid al-Fitr fall on a non-working day,
            the next working day is also a holiday (Article 2).
    """

    country = "GN"
    default_language = "fr"
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    # Day after the %s.
    observed_label = tr("Lendemain de la %s")
    # Day after the %s (estimated).
    observed_estimated_label = tr("Lendemain de la %s (estimé)")
    supported_languages = ("en_US", "fr")

    # Guinea gained independence from France on October 2, 1958.
    start_year = 1959

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
            self, cls=GuineaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        dt = self._add_new_years_day(tr("Fête du Nouvel an"))
        if self._year >= 2023:
            self._add_observed(dt)

        if 1985 <= self._year <= 2021:
            # Second Republic Day.
            self._add_holiday_apr_3(tr("Jour de la Deuxième République"))

        # Easter Monday.
        self._add_easter_monday(tr("Lundi de Pâques"))

        # Labor Day.
        self._add_labor_day(tr("Fête du Travail"))

        # Africa Day.
        self._add_africa_day(tr("Anniversaire de l'Union Africaine"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assomption"))

        # Independence Day.
        dt = self._add_holiday_oct_2(tr("Fête anniversaire de l'indépendance de la Guinée"))
        if self._year >= 2023:
            self._add_observed(dt)

        if self._year <= 2021:
            # All Saints' Day.
            self._add_all_saints_day(tr("Toussaint"))

        # Christmas Day.
        self._add_christmas_day(tr("Fête de Noël"))

        # Day after Prophet's Birthday.
        self._add_mawlid_day(tr("Lendemain de la nuit du Maoloud"))

        # Day after Night of Power.
        self._add_laylat_al_qadr_day(tr("Lendemain de la nuit Lailatoul Qadr"))

        # Eid al-Fitr.
        for dt in self._add_eid_al_fitr_day(tr("Jour de l'Aïd el-Fitr")):
            if self._year >= 2023:
                self._add_observed(dt)

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Jour de la Tabaski"))

        if self._year >= 2023:
            # Day after Eid al-Adha.
            self._add_eid_al_adha_day_two(tr("Lendemain de la Tabaski"))


class GN(Guinea):
    pass


class GIN(Guinea):
    pass


class GuineaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_ADHA_DATES = {
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
        2022: (JUL, 10),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_FITR_DATES = {
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2025: (MAR, 31),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2015, 2025)
    MAWLID_DATES = {
        2015: (DEC, 24),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2019: (NOV, 10),
        2025: (SEP, 5),
    }

    LAYLAT_AL_QADR_DATES_CONFIRMED_YEARS = (2015, 2025)
    LAYLAT_AL_QADR_DATES = {
        2016: (JUL, 3),
        2018: (JUN, 12),
        2022: (APR, 29),
    }
