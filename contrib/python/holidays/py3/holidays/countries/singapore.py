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

from holidays.calendars import (
    _CustomBuddhistHolidays,
    _CustomChineseHolidays,
    _CustomIslamicHolidays,
    _CustomHinduHolidays,
)
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.groups import (
    BuddhistCalendarHolidays,
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_WORKDAY


class Singapore(
    ObservedHolidayBase,
    BuddhistCalendarHolidays,
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Singapore holidays.

    References:
        * [Wikipedia](https://en.wikipedia.org/wiki/Public_holidays_in_Singapore)
        * [Holidays Act 1998](https://web.archive.org/web/20250405061431/https://sso.agc.gov.sg/Act/HA1998)
        * [Ministry of Manpower](https://web.archive.org/web/20250616105633/https://mom.gov.sg/employment-practices/public-holidays)

    Limitations:
        * Prior to 1969: holidays are estimated.
        * Prior to 2000: holidays may not be accurate.
        * 2024 and later: the following four moving date holidays (whose exact
            date is announced yearly) are estimated, and so denoted:
            * Hari Raya Puasa
            * Hari Raya Haji
            * Vesak Day
            * Deepavali
    """

    country = "SG"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    default_language = "en_SG"
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observed, estimated)")
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_SG", "en_US", "th")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        BuddhistCalendarHolidays.__init__(self, cls=SingaporeBuddhistHolidays, show_estimated=True)
        ChineseCalendarHolidays.__init__(self, cls=SingaporeChineseHolidays, show_estimated=True)
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=SingaporeHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=SingaporeIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=SingaporeStaticHolidays)
        # Implement Section 4(2) of the Holidays Act:
        # "if any day specified in the Schedule falls on a Sunday,
        # the day next following not being itself a public holiday
        # is declared a public holiday in Singapore."
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 1998)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self) -> None:
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("New Year's Day")))

        # Chinese New Year.
        name = tr("Chinese New Year")
        dts_observed.add(self._add_chinese_new_years_day(name))  # type: ignore[arg-type]
        dts_observed.add(self._add_chinese_new_years_day_two(name))  # type: ignore[arg-type]

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Hari Raya Puasa")))
        if self._year <= 1968:
            # Second Day of Eid al-Fitr.
            self._add_eid_al_fitr_day_two(tr("Second Day of Hari Raya Puasa"))

        # Eid al-Adha.
        dts_observed.update(self._add_eid_al_adha_day(tr("Hari Raya Haji")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        if self._year <= 1968:
            # Holy Saturday.
            self._add_holy_saturday(tr("Holy Saturday"))

            # Easter Monday.
            self._add_easter_monday(tr("Easter Monday"))

        # Labor Day.
        dts_observed.add(self._add_labor_day(tr("Labour Day")))

        # Vesak Day.
        dts_observed.add(self._add_vesak(tr("Vesak Day")))  # type: ignore[arg-type]

        # National Day.
        dts_observed.add(self._add_holiday_aug_9(tr("National Day")))

        # Deepavali.
        dts_observed.add(self._add_diwali(tr("Deepavali")))  # type: ignore[arg-type]

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Christmas Day")))

        if self._year <= 1968:
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day"))

        if self.observed:
            self._populate_observed(dts_observed)


class SG(Singapore):
    pass


class SGP(Singapore):
    pass


class SingaporeBuddhistHolidays(_CustomBuddhistHolidays):
    VESAK_DATES = {
        2001: (MAY, 7),
        2002: (MAY, 26),
        2003: (MAY, 15),
        2004: (JUN, 2),
        2005: (MAY, 22),
        2006: (MAY, 12),
        2007: (MAY, 31),
        2008: (MAY, 19),
        2009: (MAY, 9),
        2010: (MAY, 28),
        2011: (MAY, 17),
        2012: (MAY, 5),
        2013: (MAY, 24),
        2014: (MAY, 13),
        2015: (JUN, 1),
        2016: (MAY, 21),
        2017: (MAY, 10),
        2018: (MAY, 29),
        2019: (MAY, 19),
        2020: (MAY, 7),
        2021: (MAY, 26),
        2022: (MAY, 15),
        2023: (JUN, 2),
        2024: (MAY, 22),
        2025: (MAY, 12),
        2026: (MAY, 31),
    }


class SingaporeChineseHolidays(_CustomChineseHolidays):
    LUNAR_NEW_YEAR_DATES_CONFIRMED_YEARS = (2001, 2026)
    LUNAR_NEW_YEAR_DATES = {
        2006: (JAN, 30),
        2007: (FEB, 19),
    }


class SingaporeHinduHolidays(_CustomHinduHolidays):
    # Deepavali
    DIWALI_DATES = {
        2001: (NOV, 14),
        2002: (NOV, 3),
        2003: (OCT, 23),
        2004: (NOV, 11),
        2005: (NOV, 1),
        2006: (OCT, 21),
        2007: (NOV, 8),
        2008: (OCT, 27),
        2009: (NOV, 15),
        2010: (NOV, 5),
        2011: (OCT, 26),
        2012: (NOV, 13),
        2013: (NOV, 2),
        2014: (OCT, 22),
        2015: (NOV, 10),
        2016: (OCT, 29),
        2017: (OCT, 18),
        2018: (NOV, 6),
        2019: (OCT, 27),
        2020: (NOV, 14),
        2021: (NOV, 4),
        2022: (OCT, 24),
        2023: (NOV, 12),
        2024: (OCT, 31),
        2025: (OCT, 20),
        2026: (NOV, 8),
    }


class SingaporeIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2026)
    EID_AL_ADHA_DATES = {
        2001: (MAR, 6),
        2002: (FEB, 23),
        2003: (FEB, 12),
        2010: (NOV, 17),
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 12),
        2018: (AUG, 22),
        2022: (JUL, 10),
        2023: (JUN, 29),
        2024: (JUN, 17),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2026)
    EID_AL_FITR_DATES = {
        2002: (DEC, 6),
        2006: (OCT, 24),
        2019: (JUN, 5),
        2022: (MAY, 3),
        2023: (APR, 22),
        2025: (MAR, 31),
        2026: (MAR, 21),
    }


class SingaporeStaticHolidays:
    """Singapore special holidays.

    References:
        * <https://web.archive.org/web/20241015024728/https://www.mom.gov.sg/newsroom/press-releases/2015/sg50-public-holiday-on-7-august-2015>
        * <https://web.archive.org/web/20240809195048/https://www.mom.gov.sg/newsroom/press-releases/2020/0624-public-holiday-on-polling-day---10-july-2020>
        * <https://web.archive.org/web/20241113193000/https://www.mom.gov.sg/newsroom/press-releases/2023/0822-public-holiday-on-polling-day---1-sep-2023>
        * <https://web.archive.org/web/20250424145037/https://www.mom.gov.sg/newsroom/press-releases/2025/0415-public-holiday-on-polling-day_3-may-2025>
    """

    # Polling Day.
    polling_day_name = tr("Polling Day")

    special_public_holidays = {
        2001: (NOV, 3, polling_day_name),
        2006: (MAY, 6, polling_day_name),
        2011: (MAY, 7, polling_day_name),
        2015: (
            # SG50 Public Holiday.
            (AUG, 7, tr("SG50 Public Holiday")),
            (SEP, 11, polling_day_name),
        ),
        2020: (JUL, 10, polling_day_name),
        2023: (SEP, 1, polling_day_name),
        2025: (MAY, 3, polling_day_name),
    }

    special_public_holidays_observed = {
        # Eid al-Adha.
        2007: (JAN, 2, tr("Hari Raya Haji")),
    }
