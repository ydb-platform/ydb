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

from holidays.calendars.gregorian import GREGORIAN_CALENDAR, JAN, MAR, APR, MAY, JUN, JUL, NOV, DEC
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Belarus(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Belarus holidays.

    References:
        * <https://web.archive.org/web/20250415105200/https://president.gov.by/en/gosudarstvo/prazdniki>
        * <https://web.archive.org/web/20250413194347/https://president.gov.by/be/gosudarstvo/prazdniki>
        * <https://web.archive.org/web/20250413194359/https://president.gov.by/ru/gosudarstvo/prazdniki>
        * <https://web.archive.org/web/20250129213846/https://www.belarus.by/en/about-belarus/national-holidays>
        * <https://web.archive.org/web/20250123174511/https://laws.newsby.org/documents/ukazp/pos05/ukaz05806.htm>
        * <https://web.archive.org/web/20250413194444/https://president.gov.by/uploads/documents/2019/464uk.pdf>
        * <https://ru.wikipedia.org/wiki/Праздники_Белоруссии>

    Cross-checked With:
        * <https://web.archive.org/web/20250413194446/https://president.gov.by/en/gosudarstvo/prazdniki/calendar-2024>
    """

    country = "BY"
    default_language = "be"
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("be", "en_US", "ru", "th")
    # Declaration of State Sovereignty of the BSSR.
    start_year = 1991

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, BelarusStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("Новы год")
        self._add_new_years_day(name)
        if self._year >= 2020:
            self._add_new_years_day_two(name)

        # Orthodox Christmas Day.
        self._add_christmas_day(tr("Нараджэнне Хрыстова (праваслаўнае Раство)"))

        # Women's Day.
        self._add_womens_day(tr("Дзень жанчын"))

        if 1995 <= self._year <= 1998:
            # Constitution Day.
            self._add_holiday_mar_15(tr("Дзень Канстытуцыі"))

        # Labor Day.
        self._add_labor_day(tr("Свята працы"))

        # Victory Day.
        self._add_world_war_two_victory_day(tr("Дзень Перамогі"), is_western=False)

        # Radunitsa (Day of Rejoicing).
        self._add_rejoicing_day(tr("Радаўніца"))

        # Independence Day of the Republic of Belarus (Day of the Republic).
        name = tr("Дзень Незалежнасці Рэспублікі Беларусь (Дзень Рэспублікі)")
        if self._year >= 1997:
            self._add_holiday_jul_3(name)
        else:
            self._add_holiday_jul_27(name)

        if self._year >= 1995:
            # October Revolution Day.
            self._add_holiday_nov_7(tr("Дзень Кастрычніцкай рэвалюцыі"))

        # Catholic Christmas Day.
        self._add_christmas_day(tr("Нараджэнне Хрыстова (каталіцкае Раство)"), GREGORIAN_CALENDAR)

        if self._year >= 1992:
            # Catholic Easter.
            name_catholic = tr("Каталiцкi Вялiкдзень")
            self._add_easter_sunday(name_catholic, GREGORIAN_CALENDAR)

            # Orthodox Easter.
            name_orthodox = tr("Праваслаўны Вялiкдзень")
            self._add_easter_sunday(name_orthodox)

            if self._year <= 1997:
                self._add_easter_monday(name_catholic, GREGORIAN_CALENDAR)
                self._add_easter_monday(name_orthodox)

                # Dzyady (All Souls' Day).
                self._add_all_souls_day(tr("Дзень памяці"))

    def _populate_workday_holidays(self):
        # Day of the Fatherland's Defenders and the Armed Forces of the Republic of Belarus.
        self._add_holiday_feb_23(tr("Дзень абаронцаў Айчыны і Узброеных Сіл Рэспублікі Беларусь"))

        if self._year >= 1999:
            # Constitution Day.
            self._add_holiday_mar_15(tr("Дзень Канстытуцыі"))

        if self._year >= 1996:
            # Day of Unity of the Peoples of Belarus and Russia.
            self._add_holiday_apr_2(tr("Дзень яднання народаў Беларусі і Расіі"))

        if self._year >= 1998:
            self._add_holiday_2nd_sun_of_may(
                # Day of the National Coat of Arms of the Republic of Belarus,
                # the National Flag of the Republic of Belarus
                # and the National Anthem of the Republic of Belarus.
                tr(
                    "Дзень Дзяржаўнага сцяга, Дзяржаўнага герба і Дзяржаўнага "
                    "гімна Рэспублікі Беларусь"
                )
            )

        if self._year >= 2021:
            # Day of People's Unity.
            self._add_holiday_sep_17(tr("Дзень народнага адзінства"))

        if self._year >= 1998:
            # Dzyady (All Souls' Day).
            self._add_all_souls_day(tr("Дзень памяці"))


class BY(Belarus):
    pass


class BLR(Belarus):
    pass


class BelarusStaticHolidays:
    """Belarus special holidays.

    References:
        * [2024](https://web.archive.org/web/20241005054448/https://belarusbank.by/en/financial-institutions/11151)
        * [2025](https://web.archive.org/web/20241210150222/https://belarusbank.by/en/financial-institutions/11160)
        * [2026](https://web.archive.org/web/20251230174418/https://belarusbank.by/en/financial-institutions/11163)
    """

    # Date format (see strftime() Format Codes)
    substituted_date_format = tr("%d.%m.%Y")
    # Day off (substituted from %s).
    substituted_label = tr("Выходны (перанесены з %s)")
    special_public_holidays = {
        1998: (
            (JAN, 2, JAN, 10),
            (APR, 27, APR, 25),
        ),
        1999: (
            (JAN, 8, JAN, 16),
            (APR, 19, APR, 17),
        ),
        2000: (
            (MAY, 8, MAY, 13),
            (NOV, 6, NOV, 11),
        ),
        2001: (
            (JAN, 2, JAN, 20),
            (MAR, 9, MAR, 3),
            (APR, 23, APR, 21),
            (APR, 30, APR, 28),
            (JUL, 2, JUL, 7),
            (DEC, 24, DEC, 22),
            (DEC, 31, DEC, 29),
        ),
        2002: (
            (JAN, 2, JAN, 5),
            (MAY, 10, MAY, 18),
            (NOV, 8, NOV, 16),
        ),
        2003: (
            (JAN, 6, JAN, 4),
            (MAY, 5, MAY, 3),
        ),
        2004: (
            (JAN, 2, JAN, 10),
            (JAN, 5, JAN, 17),
            (JAN, 6, JAN, 31),
            (APR, 19, APR, 17),
        ),
        2005: (MAR, 7, MAR, 12),
        2006: (
            (JAN, 2, JAN, 21),
            (MAY, 8, MAY, 6),
            (NOV, 6, NOV, 4),
        ),
        2007: (
            (JAN, 2, DEC, 30, 2006),
            (MAR, 9, MAR, 17),
            (APR, 16, APR, 14),
            (APR, 30, MAY, 5),
            (JUL, 2, JUL, 7),
            (DEC, 24, DEC, 22),
            (DEC, 31, DEC, 29),
        ),
        2008: (
            (JAN, 2, JAN, 12),
            (MAY, 5, MAY, 3),
            (JUL, 4, JUN, 28),
            (DEC, 26, DEC, 20),
        ),
        2009: (
            (JAN, 2, JAN, 10),
            (APR, 27, APR, 25),
        ),
        2010: (
            (JAN, 8, JAN, 23),
            (APR, 12, APR, 17),
            (MAY, 10, MAY, 15),
        ),
        2011: (
            (MAR, 7, MAR, 12),
            (MAY, 2, MAY, 14),
        ),
        2012: (
            (MAR, 9, MAR, 11),
            (APR, 23, APR, 28),
            (JUL, 2, JUN, 30),
            (DEC, 24, DEC, 22),
            (DEC, 31, DEC, 29),
        ),
        2013: (
            (JAN, 2, JAN, 5),
            (MAY, 10, MAY, 18),
        ),
        2014: (
            (JAN, 2, JAN, 4),
            (JAN, 6, JAN, 11),
            (APR, 30, MAY, 3),
            (JUL, 4, JUL, 12),
            (DEC, 26, DEC, 20),
        ),
        2015: (
            (JAN, 2, JAN, 10),
            (APR, 20, APR, 25),
        ),
        2016: (
            (JAN, 8, JAN, 16),
            (MAR, 7, MAR, 5),
        ),
        2017: (
            (JAN, 2, JAN, 21),
            (APR, 24, APR, 29),
            (MAY, 8, MAY, 6),
            (NOV, 6, NOV, 4),
        ),
        2018: (
            (JAN, 2, JAN, 20),
            (MAR, 9, MAR, 3),
            (APR, 16, APR, 14),
            (APR, 30, APR, 28),
            (JUL, 2, JUL, 7),
            (DEC, 24, DEC, 22),
            (DEC, 31, DEC, 29),
        ),
        2019: (
            (MAY, 6, MAY, 4),
            (MAY, 8, MAY, 11),
            (NOV, 8, NOV, 16),
        ),
        2020: (
            (JAN, 6, JAN, 4),
            (APR, 27, APR, 4),
        ),
        2021: (
            (JAN, 8, JAN, 16),
            (MAY, 10, MAY, 15),
        ),
        2022: (
            (MAR, 7, MAR, 12),
            (MAY, 2, MAY, 14),
        ),
        2023: (
            (APR, 24, APR, 29),
            (MAY, 8, MAY, 13),
            (NOV, 6, NOV, 11),
        ),
        2024: (
            (MAY, 13, MAY, 18),
            (NOV, 8, NOV, 16),
        ),
        2025: (
            (JAN, 6, JAN, 11),
            (APR, 28, APR, 26),
            (JUL, 4, JUL, 12),
            (DEC, 26, DEC, 20),
        ),
        2026: (APR, 20, APR, 25),
    }
