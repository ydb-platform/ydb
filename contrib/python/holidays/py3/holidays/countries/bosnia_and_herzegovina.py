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
from holidays.calendars.gregorian import (
    GREGORIAN_CALENDAR,
    FEB,
    MAR,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
)
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import ChristianHolidays, IslamicHolidays, InternationalHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_NEXT_MON,
    SUN_TO_NEXT_MON,
    SUN_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class BosniaAndHerzegovina(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays
):
    """Bosnia and Herzegovina holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Bosnia_and_Herzegovina>
        * <https://web.archive.org/web/20250415045455/https://www.paragraf.ba/neradni-dani-fbih.html>
        * <https://web.archive.org/web/20250415085409/https://www.paragraf.ba/neradni-dani-republike-srpske.html>
        * <https://web.archive.org/web/20250414212923/https://www.paragraf.ba/neradni-dani-brcko.html>

    Observed holidays rules:
        * BIH: if first day of New Year's Day and Labor Day fall on Sunday, observed on Tuesday.
        * BRC: if holiday fall on Sunday, observed on next working day.
        * SRP: if second day of New Year's Day and Labor Day fall on Sunday, observed on Monday.
    """

    country = "BA"
    default_language = "bs"
    # %s (estimated).
    estimated_label = tr("%s (procijenjeno)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (slobodan dan, procijenjeno)")
    # %s (observed).
    observed_label = tr("%s (slobodan dan)")
    subdivisions = (
        "BIH",  # Federacija Bosne i Hercegovine.
        "BRC",  # Brčko distrikt.
        "SRP",  # Republika Srpska.
    )
    subdivisions_aliases = {
        "Federacija Bosne i Hercegovine": "BIH",
        "FBiH": "BIH",
        "Brčko distrikt": "BRC",
        "BD": "BRC",
        "Republika Srpska": "SRP",
        "RS": "SRP",
    }
    supported_languages = ("bs", "en_US", "sr", "uk")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=BosniaAndHerzegovinaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Orthodox Good Friday.
        self._add_good_friday(tr("Veliki petak (Pravoslavni)"))

        # Catholic Easter Monday.
        self._add_easter_monday(tr("Uskrsni ponedjeljak (Katolički)"), GREGORIAN_CALENDAR)

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Ramazanski Bajram"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Kurban Bajram"))

    def _populate_subdiv_holidays(self):
        if not self.subdiv:
            # New Year's Day.
            name = tr("Nova godina")
            self._add_new_years_day(name)
            self._add_new_years_day_two(name)

            # Orthodox Christmas Day.
            self._add_christmas_day(tr("Božić (Pravoslavni)"))

            # International Labor Day.
            name = tr("Međunarodni praznik rada")
            self._add_labor_day(name)
            self._add_labor_day_two(name)

            # Catholic Christmas Day.
            self._add_christmas_day(tr("Božić (Katolički)"), GREGORIAN_CALENDAR)

        super()._populate_subdiv_holidays()

    def _populate_subdiv_bih_public_holidays(self):
        # New Year's Day.
        name = tr("Nova godina")
        self._add_observed(self._add_new_years_day(name), rule=SUN_TO_NEXT_TUE)
        self._add_new_years_day_two(name)

        # Orthodox Christmas Eve.
        self._add_christmas_eve(tr("Badnji dan (Pravoslavni)"))

        # Orthodox Christmas Day.
        self._add_christmas_day(tr("Božić (Pravoslavni)"))

        # Independence Day.
        self._add_holiday_mar_1(tr("Dan nezavisnosti"))

        # Catholic Good Friday.
        self._add_good_friday(tr("Veliki petak (Katolički)"), GREGORIAN_CALENDAR)

        # Catholic Easter Sunday.
        self._add_easter_sunday(tr("Uskrs (Katolički)"), GREGORIAN_CALENDAR)

        # Orthodox Easter Sunday.
        self._add_easter_sunday(tr("Vaskrs (Pravoslavni)"))

        # Orthodox Easter Monday.
        self._add_easter_monday(tr("Uskrsni ponedjeljak (Pravoslavni)"))

        # International Labor Day.
        name = tr("Međunarodni praznik rada")
        self._add_observed(self._add_labor_day(name), rule=SUN_TO_NEXT_TUE)
        self._add_labor_day_two(name)

        # Victory Day.
        self._add_world_war_two_victory_day(tr("Dan pobjede nad fašizmom"), is_western=False)

        # Statehood Day.
        self._add_holiday_nov_25(tr("Dan državnosti"))

        # Catholic Christmas Eve.
        self._add_christmas_eve(tr("Badnji dan (Katolički)"), GREGORIAN_CALENDAR)

        # Catholic Christmas Day.
        self._add_christmas_day(tr("Božić (Katolički)"), GREGORIAN_CALENDAR)

        # Eid al-Fitr.
        self._add_eid_al_fitr_day_two(tr("Ramazanski Bajram"))

        # Eid al-Adha.
        self._add_eid_al_adha_day_two(tr("Kurban Bajram"))

    def _populate_subdiv_brc_public_holidays(self):
        # New Year's Day.
        name = tr("Nova godina")
        self._add_observed(self._add_new_years_day(name), rule=SAT_SUN_TO_NEXT_MON_TUE)
        self._add_new_years_day_two(name)

        # Orthodox Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Božić (Pravoslavni)")))

        self._add_observed(
            # Day of establishment of Brčko District.
            self._add_holiday_mar_8(tr("Dan uspostavljanja Brčko distrikta"))
        )

        # International Labor Day.
        name = tr("Međunarodni praznik rada")
        self._add_observed(self._add_labor_day(name), rule=SAT_SUN_TO_NEXT_MON_TUE)
        self._add_labor_day_two(name)

        self._add_observed(
            # Catholic Christmas Day.
            self._add_christmas_day(tr("Božić (Katolički)"), GREGORIAN_CALENDAR)
        )

    def _populate_subdiv_srp_public_holidays(self):
        # New Year's Day.
        name = tr("Nova godina")
        self._add_observed(self._add_new_years_day(name), rule=SAT_TO_NEXT_MON)
        self._add_new_years_day_two(name)

        # Orthodox Christmas Eve.
        self._add_christmas_eve(tr("Badnji dan (Pravoslavni)"))

        # Orthodox Christmas Day.
        self._add_christmas_day(tr("Božić (Pravoslavni)"))

        # Orthodox New Year.
        self._add_holiday_jan_14(tr("Pravoslavna Nova godina"))

        # Catholic Good Friday.
        self._add_good_friday(tr("Veliki petak (Katolički)"), GREGORIAN_CALENDAR)

        # Catholic Easter Sunday.
        self._add_easter_sunday(tr("Uskrs (Katolički)"), GREGORIAN_CALENDAR)

        # Orthodox Easter Sunday.
        self._add_easter_sunday(tr("Vaskrs (Pravoslavni)"))

        # Orthodox Easter Monday.
        self._add_easter_monday(tr("Uskrsni ponedjeljak (Pravoslavni)"))

        # International Labor Day.
        name = tr("Međunarodni praznik rada")
        self._add_observed(self._add_labor_day(name), rule=SAT_TO_NEXT_MON)
        self._add_labor_day_two(name)

        # Victory Day.
        self._add_world_war_two_victory_day(tr("Dan pobjede nad fašizmom"), is_western=False)

        self._add_holiday_nov_21(
            # Dayton Agreement Day.
            tr("Dan uspostave Opšteg okvirnog sporazuma za mir u Bosni i Hercegovini")
        )

        # Catholic Christmas Eve.
        self._add_christmas_eve(tr("Badnji dan (Katolički)"), GREGORIAN_CALENDAR)

        # Catholic Christmas Day.
        self._add_christmas_day(tr("Božić (Katolički)"), GREGORIAN_CALENDAR)

        # Eid al-Fitr.
        self._add_eid_al_fitr_day_two(tr("Ramazanski Bajram"))

        # Eid al-Adha.
        self._add_eid_al_adha_day_two(tr("Kurban Bajram"))


class BA(BosniaAndHerzegovina):
    pass


class BIH(BosniaAndHerzegovina):
    pass


class BosniaAndHerzegovinaIslamicHolidays(_CustomIslamicHolidays):
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
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
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
