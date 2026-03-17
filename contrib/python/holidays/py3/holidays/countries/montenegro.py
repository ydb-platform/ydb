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
from holidays.calendars.gregorian import GREGORIAN_CALENDAR, JUN, JUL, AUG, SEP, OCT, NOV
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.constants import CATHOLIC, HEBREW, ISLAMIC, ORTHODOX, PUBLIC, WORKDAY
from holidays.groups import (
    ChristianHolidays,
    HebrewCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class Montenegro(
    ObservedHolidayBase,
    ChristianHolidays,
    HebrewCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Montenegro holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Montenegro>
        * [Zakon o državnim i drugim praznicima](https://web.archive.org/web/20241227132503/https://wapi.gov.me/download-preview/927f23a3-db4e-4f65-9f29-ce3c9dde0c90?version=1.0)
        * [Zakon o svetkovanju vjerskih praznika](https://web.archive.org/web/20250427181807/https://wapi.gov.me/download-preview/4f0b05a4-c85b-4eb2-bc29-0ad8363a9ba3?version=1.0)
    """

    country = "ME"
    default_language = "cnr"
    # %s (estimated).
    estimated_label = tr("%s (procijenjeno)")
    # %s (observed).
    observed_label = tr("%s (neradni dan)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (neradni dan, procijenjeno)")
    supported_languages = ("cnr", "en_US", "uk")
    supported_categories = (CATHOLIC, ISLAMIC, HEBREW, ORTHODOX, PUBLIC, WORKDAY)
    start_year = 2007

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, calendar=JULIAN_CALENDAR)
        HebrewCalendarHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=MontenegroIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=MontenegroStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("Nova godina")
        self._add_observed(self._add_new_years_day(name), rule=SUN_TO_NEXT_TUE)
        self._add_observed(self._add_new_years_day_two(name))

        # Labor Day.
        name = tr("Praznik rada")
        self._add_observed(self._add_labor_day(name), rule=SUN_TO_NEXT_TUE)
        self._add_observed(self._add_labor_day_two(name))

        # Independence Day.
        name = tr("Dan nezavisnosti")
        self._add_observed(self._add_holiday_may_21(name), rule=SUN_TO_NEXT_TUE)
        self._add_observed(self._add_holiday_may_22(name))

        # Statehood Day.
        name = tr("Dan državnosti")
        self._add_observed(self._add_holiday_jul_13(name), rule=SUN_TO_NEXT_TUE)
        self._add_observed(self._add_holiday_jul_14(name))

        if self._year >= 2022:
            # Njegos Day.
            self._add_observed(self._add_holiday_nov_13(tr("Njegošev dan")))

    def _populate_catholic_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Veliki petak"), GREGORIAN_CALENDAR)

        # Easter.
        self._add_easter_monday(tr("Uskrs"), GREGORIAN_CALENDAR)

        # All Saints' Day.
        self._add_all_saints_day(tr("Svi Sveti"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Badnji dan"), GREGORIAN_CALENDAR)

        # Christmas.
        name = tr("Božić")
        self._add_christmas_day(name, GREGORIAN_CALENDAR)
        self._add_christmas_day_two(name, GREGORIAN_CALENDAR)

    def _populate_hebrew_holidays(self):
        # Pesach.
        self._add_passover(tr("Pasha"), range(2))

        # Yom Kippur.
        self._add_yom_kippur(tr("Jom Kipur"), range(2))

    def _populate_islamic_holidays(self):
        # Eid al-Fitr.
        name = tr("Ramazanski bajram")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Eid al-Adha.
        name = tr("Kurbanski bajram")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)

    def _populate_orthodox_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Veliki petak"))

        # Easter.
        self._add_easter_monday(tr("Uskrs"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Badnji dan"))

        # Christmas.
        name = tr("Božić")
        self._add_christmas_day(name)
        self._add_christmas_day_two(name)

    def _populate_workday_holidays(self):
        if self._year >= 2022:
            # Ecological State Day.
            self._add_holiday_sep_20(tr("Dan Ekološke države"))


class ME(Montenegro):
    pass


class MNE(Montenegro):
    pass


class MontenegroIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2012, 2024)
    EID_AL_ADHA_DATES = {
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2012, 2024)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
    }


class MontenegroStaticHolidays:
    special_public_holidays = {
        2024: (NOV, 14, tr("Njegošev dan")),
    }
