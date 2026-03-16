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
from holidays.calendars.gregorian import GREGORIAN_CALENDAR, APR, MAY, JUL, SEP, OCT
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.constants import (
    ALBANIAN,
    BOSNIAN,
    CATHOLIC,
    HEBREW,
    ISLAMIC,
    ORTHODOX,
    PUBLIC,
    ROMA,
    SERBIAN,
    TURKISH,
    VLACH,
)
from holidays.groups import (
    ChristianHolidays,
    HebrewCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class NorthMacedonia(
    ObservedHolidayBase,
    ChristianHolidays,
    HebrewCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """North Macedonia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_North_Macedonia>
        * [Law on Holidays 21/98](https://web.archive.org/web/20250404064336/https://www.mtsp.gov.mk/content/pdf/dokumenti/Zakon_Praznici_na_RM%20(2_2).pdf)
        * [Law Amending Law on Holidays 18/2007](https://web.archive.org/web/20250505061325/https://www.mtsp.gov.mk/content/pdf/dokumenti/Zakon_Izmenuvanje_Praznici_na_RM%20(2_2).pdf)

    Checked With:
        * [Non-working days program](https://web.archive.org/web/20250420034141/https://mtsp.gov.mk/programa-za-nerabotni-denovi.nspx)
        * [2010](https://web.archive.org/web/20250430235227/https://www.mtsp.gov.mk/WBStorage/Files/programadeseta.doc)
        * [2011](https://web.archive.org/web/20250501104555/https://www.mtsp.gov.mk/WBStorage/Files/programaedinaeseta.doc)
        * [2012](https://web.archive.org/web/20250501001337/https://www.mtsp.gov.mk/WBStorage/Files/programadvanaeseta.doc)
        * [2013](https://web.archive.org/web/20250430234446/https://www.mtsp.gov.mk/WBStorage/Files/programa_trinaeseta_nova.doc)
        * [2014](https://web.archive.org/web/20241206035550/https://www.mtsp.gov.mk/WBStorage/Files/programa_nerabotni_denovi_2014.pdf)
        * [2015](https://web.archive.org/web/20240304070837/https://www.mtsp.gov.mk/content/pdf/Programa_nerabotni_denovi_2015.pdf)
        * [2016](https://web.archive.org/web/20241206052140/https://www.mtsp.gov.mk/content/pdf/Programa_nerabotni_2016.pdf)
        * [2017](https://web.archive.org/web/20240629115141/https://www.mtsp.gov.mk/content/pdf/19.10._Програма%20на%20неработни%20денови%20за%202017%20година.pdf)
        * [2018](https://web.archive.org/web/20240630201651/https://www.mtsp.gov.mk/content/pdf/programi/Програма%20на%20неработни%20денови%20за%202018%20година.pdf)
        * [2019](https://web.archive.org/web/20240629172352/https://www.mtsp.gov.mk/content/pdf/programi/Programa_na_nerabotni_denovi_za_2019_godina..pdf)
        * [2020](https://web.archive.org/web/20240629104945/https://www.mtsp.gov.mk/content/pdf/dokumenti/2019/Програма%20на%20неработни%20денови%20за%202020%20година%201.pdf)
        * [2021](https://web.archive.org/web/20250502065435/https://www.mtsp.gov.mk/content/Програма%20на%20неработни%20денови%20за%202021%20година%20PDF.pdf)
        * [2022](https://web.archive.org/web/20250420054039/https://www.mtsp.gov.mk/content/word/2021/nerabotni/2022_programa_praznici_nerabotni_2.doc)
        * [2023](https://web.archive.org/web/20250420050919/https://mtsp.gov.mk/content/word/programi/2022/programa_nerabotni_2023.doc)
        * [2024](https://web.archive.org/web/20250420060645/https://www.mtsp.gov.mk/content/word/programi/2023/programa_nerabotni_2024_mkd.doc)
        * [2025](https://web.archive.org/web/20250421022811/https://www.mtsp.gov.mk/content/word/2024/programa_nerabotni_2025.docx)
    """

    country = "MK"
    default_language = "mk"
    # %s (estimated).
    estimated_label = tr("%s (проценето)")
    # %s (observed).
    observed_label = tr("%s (неработен ден)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (неработен ден, проценето)")
    start_year = 1999
    supported_categories = (
        ALBANIAN,
        BOSNIAN,
        CATHOLIC,
        HEBREW,
        ISLAMIC,
        ORTHODOX,
        PUBLIC,
        ROMA,
        SERBIAN,
        TURKISH,
        VLACH,
    )
    supported_languages = ("en_US", "mk", "uk")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        HebrewCalendarHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=NorthMacedoniaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, NorthMacedoniaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("Нова Година")
        self._add_observed(self._add_new_years_day(name))
        if self._year <= 2007:
            self._add_observed(self._add_new_years_day_two(name))

        if self._year >= 2008:
            # Christmas Day.
            self._add_observed(self._add_christmas_day(tr("Божиќ")))

        if self._year >= 2007:
            # Easter Monday.
            self._add_easter_monday(tr("Велигден"))

        # Labor Day.
        name = tr("Ден на трудот")
        self._add_observed(self._add_labor_day(name))
        if self._year <= 2006:
            self._add_observed(self._add_labor_day_two(name))

        if self._year >= 2007:
            self._add_observed(
                self._add_holiday_may_24(
                    # Saints Cyril and Methodius Day.
                    tr("Светите Кирил и Методиј - Ден на сесловенските просветители")
                )
            )

        # Republic Day.
        self._add_observed(self._add_holiday_aug_2(tr("Ден на Републиката")))

        # Independence Day.
        self._add_observed(self._add_holiday_sep_8(tr("Ден на независноста")))

        # National Uprising Day.
        self._add_observed(self._add_holiday_oct_11(tr("Ден на народното востание")))

        if self._year >= 2007:
            self._add_observed(
                # Macedonian Revolutionary Struggle Day.
                self._add_holiday_oct_23(tr("Ден на македонската револуционерна борба"))
            )

            # Saint Clement of Ohrid Day.
            self._add_observed(self._add_holiday_dec_8(tr("Свети Климент Охридски")))

        if self._year >= 2007:
            # Eid al-Fitr.
            for dt in self._add_eid_al_fitr_day(tr("Рамазан Бајрам")):
                self._add_observed(dt)

    def _populate_albanian_holidays(self):
        if self._year >= 2007:
            # Albanian Alphabet Day.
            self._add_holiday_nov_22(tr("Ден на Албанската азбука"))

    def _populate_bosnian_holidays(self):
        if self._year >= 2007:
            # International Bosniaks Day.
            self._add_holiday_sep_28(tr("Меѓународен ден на Бошњаците"))

    def _populate_catholic_holidays(self):
        # Easter Monday.
        self._add_easter_monday(tr("Велигден"), GREGORIAN_CALENDAR)

        if self._year >= 2007:
            # All Saints' Day.
            self._add_all_saints_day(tr("Сите Светци"))

        # Christmas Day.
        self._add_christmas_day(tr("Божиќ"), GREGORIAN_CALENDAR)

    def _populate_hebrew_holidays(self):
        # Yom Kippur.
        self._add_yom_kippur(tr("Јом Кипур"))

    def _populate_islamic_holidays(self):
        if self._year >= 2007:
            # Eid al-Adha.
            self._add_eid_al_adha_day(tr("Курбан Бајрам"))

    def _populate_orthodox_holidays(self):
        if self._year <= 2007:
            # Christmas Day.
            self._add_christmas_day(tr("Божиќ"))

        if self._year <= 2006:
            # Easter Monday.
            self._add_easter_monday(tr("Велигден"))

        if self._year >= 2007:
            # Christmas Eve.
            self._add_christmas_eve(tr("Бадник"))

            # Epiphany.
            self._add_epiphany_day(tr("Богојавление"))

            # Good Friday.
            self._add_good_friday(tr("Велики Петок"))

            # Dormition of the Mother of God.
            self._add_assumption_of_mary_day(tr("Успение на Пресвета Богородица"))

            # Pentecost.
            self._add_holiday_47_days_past_easter(tr("Духовден"))

    def _populate_roma_holidays(self):
        if self._year >= 2007:
            # International Romani Day.
            self._add_holiday_apr_8(tr("Меѓународен ден на Ромите"))

    def _populate_serbian_holidays(self):
        if self._year >= 2008:
            # Saint Sava's Day.
            self._add_holiday_jan_27(tr("Свети Сава"))

    def _populate_turkish_holidays(self):
        if self._year >= 2007:
            # Turkish Language Teaching Day.
            self._add_holiday_dec_21(tr("Ден на настава на турски јазик"))

    def _populate_vlach_holidays(self):
        if self._year >= 2007:
            # Vlachs National Day.
            self._add_holiday_may_23(tr("Национален ден на Власите"))


class MK(NorthMacedonia):
    pass


class MKD(NorthMacedonia):
    pass


class NorthMacedoniaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2010, 2025)
    EID_AL_ADHA_DATES = {
        2012: (OCT, 25),
        2015: (SEP, 24),
        2016: (SEP, 12),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2010, 2025)
    EID_AL_FITR_DATES = {
        2010: (SEP, 9),
        2016: (JUL, 5),
    }


class NorthMacedoniaStaticHolidays:
    """North Macedonia special holidays.

    References:
        * <https://web.archive.org/web/20250201212849/https://mk.usembassy.gov/alert-presidential-and-parliamentary-elections-on-may-8-2024/>
    """

    # Election Day.
    election_day = tr("Ден на изборите")

    special_public_holidays = {
        # Election Day.
        2024: (
            (APR, 24, election_day),
            (MAY, 8, election_day),
        ),
    }
