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

from holidays.calendars import _CustomChineseHolidays, _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, FEB, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import (
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.holiday_base import HolidayBase


class Philippines(
    HolidayBase,
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Philippines holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Philippines>
        * [Revised Administrative Code of 1987](https://web.archive.org/web/20241203234427/https://www.officialgazette.gov.ph/1987/07/25/executive-order-no-292-book-ichapter-7-regular-holidays-and-nationwide-special-days/)
        * [Republic Act No. 9177](https://web.archive.org/web/20230930164310/https://www.officialgazette.gov.ph/2002/11/13/republic-act-no-9177/)
        * [Republic Act No. 9256](https://web.archive.org/web/20240706140401/https://www.officialgazette.gov.ph/2004/02/25/republic-act-no-9256/)
        * [Republic Act No. 9492](https://web.archive.org/web/20250413180041/http://www.officialgazette.gov.ph/2007/07/24/republic-act-no-9492/)
        * [Republic Act No. 9645](https://web.archive.org/web/20231014172648/https://www.officialgazette.gov.ph/2009/06/12/republic-act-no-9645/)
        * [Republic Act No. 9849](https://web.archive.org/web/20250424053703/http://officialgazette.gov.ph/2009/12/11/republic-act-no-9849/)
        * [Republic Act No. 10966](https://web.archive.org/web/20250419183417/http://www.officialgazette.gov.ph/2017/12/28/republic-act-no-10966/)
        * [Proclamation No. 944/2020](https://web.archive.org/web/20250428055016/https://www.officialgazette.gov.ph/2020/05/19/proclamation-no-944-s-2020/)
        * [Proclamation No. 985/2020](https://web.archive.org/web/20230901112559/https://www.officialgazette.gov.ph/2020/07/29/proclamation-no-985-s-2020/)
        * [Proclamation No. 90/2022](https://web.archive.org/web/20231026052921/https://www.officialgazette.gov.ph/2022/11/09/proclamation-no-90-s-2022/)
        * [Proclamation No. 665/2024](https://archive.org/details/20241015-proc-665-frm)
        * [Proclamation No. 729/2024](https://archive.org/details/20241030-proc-729-frm)
        * [Nationwide holidays 2018-2025](https://web.archive.org/web/20240515022447/https://www.officialgazette.gov.ph/nationwide-holidays/2018/)
        * [Proclamation No. 839/2025](https://archive.org/details/20250320-proc-839-frm_202506)
        * [Proclamation No. 878/2025](https://archive.org/details/20250506-proc-878-frm_202506)
        * [Proclamation No. 911/2025](https://archive.org/details/20250521-proc-911-frm_20250606_1800)
        * [Proclamation No. 1006/2025](https://archive.org/details/20250903-proc-1006-frm)
    """

    country = "PH"
    supported_categories = (PUBLIC, WORKDAY)
    default_language = "en_PH"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    supported_languages = ("en_PH", "en_US", "fil", "th")
    start_year = 1988

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChineseCalendarHolidays.__init__(self, cls=PhilippinesChineseHolidays)
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=PhilippinesIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=PhilippinesStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("New Year's Day"))

        if self._year >= 2012 and self._year != 2023:
            # Chinese New Year.
            self._add_chinese_new_years_day(tr("Chinese New Year"))

        if 2016 <= self._year <= 2023 and self._year != 2017:
            dates_obs = {
                2023: (FEB, 24),
            }
            self._add_holiday(
                # EDSA People Power Revolution Anniversary.
                tr("EDSA People Power Revolution Anniversary"),
                dates_obs.get(self._year, (FEB, 25)),
            )

        # Maundy Thursday.
        self._add_holy_thursday(tr("Maundy Thursday"))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        if self._year >= 2013:
            # Black Saturday.
            self._add_holy_saturday(tr("Black Saturday"))

        dates_obs = {
            2008: (APR, 7),
            2009: (APR, 6),
            2023: (APR, 10),
        }
        # Day of Valor.
        self._add_holiday(tr("Araw ng Kagitingan"), dates_obs.get(self._year, (APR, 9)))

        # Labor Day.
        self._add_labor_day(tr("Labor Day"))

        dates_obs = {
            2007: (JUN, 11),
            2008: (JUN, 9),
            2010: (JUN, 14),
        }
        # Independence Day.
        self._add_holiday(tr("Independence Day"), dates_obs.get(self._year, (JUN, 12)))

        if self._year >= 2004:
            dates_obs = {
                2007: (AUG, 20),
                2008: (AUG, 18),
                2010: (AUG, 23),
                2024: (AUG, 23),
            }
            # Ninoy Aquino Day.
            self._add_holiday(tr("Ninoy Aquino Day"), dates_obs.get(self._year, (AUG, 21)))

        # National Heroes Day.
        name = tr("National Heroes Day")
        if self._year >= 2007:
            self._add_holiday_last_mon_of_aug(name)
        else:
            self._add_holiday_last_sun_of_aug(name)

        # All Saints' Day.
        self._add_all_saints_day(tr("All Saints' Day"))

        dates_obs = {
            2008: (DEC, 1),
            2010: (NOV, 29),
            2023: (NOV, 27),
        }
        # Bonifacio Day.
        self._add_holiday(tr("Bonifacio Day"), dates_obs.get(self._year, (NOV, 30)))

        if self._year >= 2019:
            # Immaculate Conception.
            self._add_immaculate_conception_day(tr("Feast of the Immaculate Conception of Mary"))

        # Christmas Day.
        self._add_christmas_day(tr("Christmas Day"))

        dates_obs = {
            2010: (DEC, 27),
        }
        # Rizal Day.
        self._add_holiday(tr("Rizal Day"), dates_obs.get(self._year, (DEC, 30)))

        if self._year not in {2021, 2022}:
            # New Year's Eve.
            self._add_new_years_eve(tr("Last Day of the Year"))

        if self._year >= 2002:
            # Eid al-Fitr.
            self._add_eid_al_fitr_day(tr("Eid'l Fitr"))

        if self._year >= 2010:
            # Eid al-Adha.
            self._add_eid_al_adha_day(tr("Eid'l Adha"))

    def _populate_workday_holidays(self):
        # Added in 2009, get special non-working day status in 2025:
        if self._year >= 2009 and self._year != 2025:
            # Founding Anniversary of Iglesia ni Cristo.
            self._add_holiday_jul_27(tr("Founding Anniversary of Iglesia ni Cristo"))

        # Added from 2025 onwards as first decreed in
        # https://web.archive.org/web/20250326064645/https://www.officialgazette.gov.ph/downloads/2024/10oct/20241030-PROC-727-FRM.pdf
        if self._year >= 2025:
            # EDSA People Power Revolution Anniversary.
            self._add_holiday_feb_25(tr("EDSA People Power Revolution Anniversary"))


class PH(Philippines):
    pass


class PHL(Philippines):
    pass


class PhilippinesChineseHolidays(_CustomChineseHolidays):
    LUNAR_NEW_YEAR_DATES_CONFIRMED_YEARS = (2012, 2025)


class PhilippinesIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2010, 2025)
    EID_AL_ADHA_DATES = {
        2010: (NOV, 17),
        2011: (NOV, 7),
        2014: (OCT, 6),
        2015: (SEP, 25),
        2016: (SEP, 10),
        2017: (SEP, 2),
        2019: (AUG, 12),
        2024: (JUN, 17),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2002, 2025)
    EID_AL_FITR_DATES = {
        2002: (DEC, 6),
        2003: (NOV, 26),
        2005: (NOV, 4),
        2006: (OCT, 24),
        2007: (OCT, 12),
        2009: (SEP, 21),
        2012: (AUG, 20),
        2013: (AUG, 9),
        2014: (JUL, 29),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2019: (JUN, 5),
        2020: (MAY, 25),
        2022: (MAY, 3),
        2025: (APR, 1),
    }


class PhilippinesStaticHolidays:
    # Additional special (non-working) day.
    additional_special = tr("Additional special (non-working) day")

    # Elections special (non-working) day.
    election_special = tr("Elections special (non-working) day")

    # Christmas Eve.
    christmas_eve = tr("Christmas Eve")

    special_public_holidays = {
        2008: (
            (DEC, 26, additional_special),
            (DEC, 29, additional_special),
        ),
        2009: (
            (NOV, 2, additional_special),
            (DEC, 24, additional_special),
        ),
        2010: (DEC, 24, additional_special),
        2012: (NOV, 2, additional_special),
        2013: (
            (NOV, 2, additional_special),
            (DEC, 24, additional_special),
        ),
        2014: (
            (DEC, 24, additional_special),
            (DEC, 26, additional_special),
        ),
        2015: (
            (JAN, 2, additional_special),
            (DEC, 24, additional_special),
        ),
        2016: (
            (JAN, 2, additional_special),
            (OCT, 31, additional_special),
            (DEC, 24, additional_special),
        ),
        2017: (
            (JAN, 2, additional_special),
            (OCT, 31, additional_special),
        ),
        2018: (
            (MAY, 14, election_special),
            (NOV, 2, additional_special),
            (DEC, 24, additional_special),
        ),
        2019: (
            (MAY, 13, election_special),
            (NOV, 2, additional_special),
            (DEC, 24, additional_special),
        ),
        2020: (
            (NOV, 2, additional_special),
            (DEC, 24, additional_special),
        ),
        2022: (
            (MAY, 9, election_special),
            (OCT, 31, additional_special),
        ),
        2023: (
            (JAN, 2, additional_special),
            (OCT, 30, election_special),
            (NOV, 2, additional_special),
            (DEC, 26, additional_special),
        ),
        2024: (
            (FEB, 9, additional_special),
            (NOV, 2, additional_special),
            (DEC, 24, additional_special),
        ),
        2025: (
            (MAY, 12, election_special),
            (JUL, 27, additional_special),
            # All Saints' Day Eve.
            (OCT, 31, tr("All Saints' Day Eve")),
            (DEC, 24, christmas_eve),
        ),
        2026: (
            # All Souls' Day.
            (NOV, 2, tr("All Souls' Day")),
            (DEC, 24, christmas_eve),
        ),
    }
