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

from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, NOV, DEC
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase


class Russia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Russia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Russia>
        * <https://ru.wikipedia.org/wiki/Праздники_России>
    """

    country = "RU"
    default_language = "ru"
    supported_languages = ("en_US", "ru", "th")
    start_year = 1991

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, RussiaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        if self._year <= 2004:
            # New Year's Day.
            name = tr("Новый год")
            self._add_new_years_day(name)
            if self._year >= 1993:
                self._add_new_years_day_two(name)
        else:
            # New Year Holidays.
            name = tr("Новогодние каникулы")
            self._add_multiday_holiday(self._add_new_years_day(name), 4)
            if self._year >= 2013:
                self._add_holiday_jan_6(name)
                self._add_holiday_jan_8(name)

        # Christmas Day.
        self._add_christmas_day(tr("Рождество Христово"))

        if self._year >= 2002:
            # Defender of the Fatherland Day.
            self._add_holiday_feb_23(tr("День защитника Отечества"))

        # International Women's Day.
        self._add_womens_day(tr("Международный женский день"))

        name = (
            # Holiday of Spring and Labor.
            tr("Праздник Весны и Труда")
            if self._year >= 1992
            # International Workers' Solidarity Day.
            else tr("День международной солидарности трудящихся")
        )
        self._add_labor_day(name)
        if self._year <= 2004:
            self._add_labor_day_two(name)

        # Victory Day.
        self._add_world_war_two_victory_day(tr("День Победы"), is_western=False)

        if self._year >= 1992:
            self._add_holiday_jun_12(
                # Russia Day.
                tr("День России")
                if self._year >= 2002
                # Day of the Adoption of the Declaration of Sovereignty of the Russian Federation.
                else tr(
                    "День принятия Декларации о государственном суверенитете Российской Федерации"
                )
            )

        if self._year >= 2005:
            # Unity Day.
            self._add_holiday_nov_4(tr("День народного единства"))

        if self._year <= 2004:
            name = (
                # Day of consent and reconciliation.
                tr("День согласия и примирения")
                if self._year >= 1996
                # Anniversary of the Great October Socialist Revolution.
                else tr("Годовщина Великой Октябрьской социалистической революции")
            )
            self._add_holiday_nov_7(name)
            if self._year <= 1991:
                self._add_holiday_nov_8(name)


class RU(Russia):
    pass


class RUS(Russia):
    pass


class RussiaStaticHolidays:
    # Date format (see strftime() Format Codes).
    substituted_date_format = tr("%d.%m.%Y")
    # Day off (substituted from %s).
    substituted_label = tr("Выходной (перенесено с %s)")

    special_public_holidays = {
        # Substituted Holidays 1991
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1991: (
            (MAY, 3, MAY, 5),
            (MAY, 10, MAY, 12),
        ),
        # Substituted Holidays 1994
        # src: https://web.archive.org/web/20250414071308/https://www.consultant.ru/document/cons_doc_LAW_3235/
        1994: (MAR, 7, MAR, 5),
        # Substituted Holidays 1995
        # src: https://web.archive.org/web/20250414071446/https://www.consultant.ru/document/cons_doc_LAW_6316/
        #      https://web.archive.org/web/20250414071453/https://www.consultant.ru/document/cons_doc_LAW_8134/
        #      https://web.archive.org/web/20250414071503/https://www.consultant.ru/document/cons_doc_LAW_8499/
        1995: (
            (MAY, 8, MAY, 6),
            (NOV, 6, NOV, 4),
            (DEC, 11, DEC, 9),
        ),
        # Substituted Holidays 1996
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1996: (
            (MAY, 3, MAY, 5),
            (MAY, 10, MAY, 12),
            (JUL, 3, DEC, 14),
            (NOV, 8, NOV, 10),
            (DEC, 13, DEC, 15),
        ),
        # Substituted Holidays 1997
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1997: (
            (JAN, 3, JAN, 5),
            (JUN, 13, JUN, 15),
        ),
        # Substituted Holidays 1999
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1999: (JAN, 8, JAN, 10),
        # Substituted Holidays 2000
        # src: https://web.archive.org/web/20250414071351/https://www.consultant.ru/document/cons_doc_LAW_25401/
        2000: (
            (MAY, 8, MAY, 6),
            (NOV, 6, NOV, 4),
            (DEC, 11, DEC, 9),
        ),
        # Substituted Holidays 2001
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2001: (
            (MAR, 9, MAR, 11),
            (APR, 30, APR, 28),
            (JUN, 11, JUN, 9),
            (DEC, 31, DEC, 29),
        ),
        # Substituted Holidays 2002
        # src: https://web.archive.org/web/20240919074957/https://www.consultant.ru/document/cons_doc_LAW_33943/
        2002: (
            (MAY, 3, APR, 27),
            (MAY, 10, MAY, 18),
            (NOV, 8, NOV, 10),
            (DEC, 13, DEC, 15),
        ),
        # Substituted Holidays 2003
        # src: https://web.archive.org/web/20190220082540/http://www.consultant.ru:80/document/cons_doc_LAW_39997/16abd0212dcc02f64a1daed24286d2ffc2a4a9e9
        2003: (
            (JAN, 3, JAN, 4),
            (JAN, 6, JAN, 5),
            (JUN, 13, JUN, 21),
        ),
        # Substituted Holidays 2005
        # src: https://web.archive.org/web/20250414071549/https://www.consultant.ru/document/cons_doc_LAW_50948/
        #      https://ru.wikipedia.org/wiki/Праздники_России
        2005: (
            (MAR, 7, MAR, 5),
            (MAY, 10, MAY, 14),
        ),
        # Substituted Holidays 2006
        # src: https://web.archive.org/web/20250418072310/https://www.consultant.ru/document/cons_doc_LAW_57425/
        2006: (
            (FEB, 24, FEB, 26),
            (MAY, 8, MAY, 6),
        ),
        # Substituted Holidays 2007
        # src: https://web.archive.org/web/20250414071613/https://www.consultant.ru/document/cons_doc_LAW_63838/
        2007: (
            (APR, 30, APR, 28),
            (JUN, 11, JUN, 9),
            (DEC, 31, DEC, 29),
        ),
        # Substituted Holidays 2008
        # src: https://web.archive.org/web/20250420141031/https://www.consultant.ru/document/cons_doc_LAW_70469/
        2008: (
            (MAY, 2, MAY, 4),
            (JUN, 13, JUN, 7),
            (NOV, 3, NOV, 1),
        ),
        # Substituted Holidays 2009
        # src: https://web.archive.org/web/20250418041918/https://www.consultant.ru/document/cons_doc_LAW_81981/
        2009: (JAN, 9, JAN, 11),
        # Substituted Holidays 2010
        # src: https://web.archive.org/web/20250419192633/https://www.consultant.ru/document/cons_doc_LAW_93374/
        2010: (
            (FEB, 22, FEB, 27),
            (NOV, 5, NOV, 13),
        ),
        # Substituted Holidays 2011
        # src: https://web.archive.org/web/20250416021056/https://www.consultant.ru/document/cons_doc_LAW_103530/
        2011: (MAR, 7, MAR, 5),
        # Substituted Holidays 2012
        # src: https://web.archive.org/web/20250420053252/https://www.consultant.ru/document/cons_doc_LAW_117190/
        2012: (
            (MAR, 9, MAR, 11),
            (APR, 30, APR, 28),
            (MAY, 7, MAY, 5),
            (MAY, 8, MAY, 12),
            (JUN, 11, JUN, 9),
            (DEC, 31, DEC, 29),
        ),
        # Substituted Holidays 2013
        # src: https://web.archive.org/web/20250422040920/https://www.consultant.ru/document/cons_doc_LAW_136654/
        2013: (
            (MAY, 2, JAN, 5),
            (MAY, 3, JAN, 6),
            (MAY, 10, FEB, 25),
        ),
        # Substituted Holidays 2014
        # src: https://web.archive.org/web/20250415234224/https://www.consultant.ru/document/cons_doc_LAW_146983/
        2014: (
            (MAY, 2, JAN, 4),
            (JUN, 13, JAN, 5),
            (NOV, 3, FEB, 24),
        ),
        # Substituted Holidays 2015
        # src: https://web.archive.org/web/20250415134053/https://www.consultant.ru/document/cons_doc_LAW_167928/
        2015: (
            (JAN, 9, JAN, 3),
            (MAY, 4, JAN, 4),
        ),
        # Substituted Holidays 2016
        # src: https://web.archive.org/web/20250417021701/https://www.consultant.ru/document/cons_doc_LAW_186505/
        2016: (
            (MAY, 3, JAN, 2),
            (MAR, 7, JAN, 3),
            (FEB, 22, FEB, 20),
        ),
        # Substituted Holidays 2017
        # src: https://web.archive.org/web/20250414184011/https://www.consultant.ru/document/cons_doc_LAW_202871/
        2017: (
            (FEB, 24, JAN, 1),
            (MAY, 8, JAN, 7),
        ),
        # Substituted Holidays 2018
        # src: https://web.archive.org/web/20250414223644/https://www.consultant.ru/document/cons_doc_LAW_280526/
        2018: (
            (MAR, 9, JAN, 6),
            (MAY, 2, JAN, 7),
            (APR, 30, APR, 28),
            (JUN, 11, JUN, 9),
            (DEC, 31, DEC, 29),
        ),
        # Substituted Holidays 2019
        # src: https://web.archive.org/web/20250414103842/https://www.consultant.ru/document/cons_doc_LAW_307996/
        2019: (
            (MAY, 2, JAN, 5),
            (MAY, 3, JAN, 6),
            (MAY, 10, FEB, 23),
        ),
        # Substituted Holidays 2020
        # src: https://web.archive.org/web/20250414071714/https://www.consultant.ru/document/cons_doc_law_328918/
        2020: (
            (MAY, 4, JAN, 4),
            (MAY, 5, JAN, 5),
        ),
        # Substituted Holidays 2021
        # src: https://web.archive.org/web/20250414071843/https://www.consultant.ru/document/cons_doc_law_365179/
        2021: (
            (NOV, 5, JAN, 2),
            (DEC, 31, JAN, 3),
            (FEB, 22, FEB, 20),
        ),
        # Substituted Holidays 2022
        # src: https://web.archive.org/web/20240613213507/https://www.consultant.ru/document/cons_doc_LAW_395538/
        2022: (
            (MAY, 3, JAN, 1),
            (MAY, 10, JAN, 2),
            (MAR, 7, MAR, 5),
        ),
        # Substituted Holidays 2023
        # src: https://web.archive.org/web/20240908190857/https://www.consultant.ru/document/cons_doc_LAW_425407/
        2023: (
            (FEB, 24, JAN, 1),
            (MAY, 8, JAN, 8),
        ),
        # Substituted Holidays 2024
        # src: https://web.archive.org/web/20250417024116/https://www.consultant.ru/document/cons_doc_LAW_455140/
        2024: (
            (APR, 29, APR, 27),
            (APR, 30, NOV, 2),
            (MAY, 10, JAN, 6),
            (DEC, 30, DEC, 28),
            (DEC, 31, JAN, 7),
        ),
        # Substituted Holidays 2025
        # src: https://web.archive.org/web/20250414071812/https://www.consultant.ru/document/cons_doc_LAW_481586/
        2025: (
            (MAY, 2, JAN, 4),
            (DEC, 31, JAN, 5),
            (MAY, 8, FEB, 23),
            (JUN, 13, MAR, 8),
            (NOV, 3, NOV, 1),
        ),
    }
    special_public_holidays_observed = {
        # These are cases where additional in-lieus are given
        # without actually making weekend workdays.
        # Substituted Holidays 1992 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1992: (
            (MAY, 4, MAY, 2),
            (MAY, 11, MAY, 9),
            (NOV, 9, NOV, 7),
        ),
        # Substituted Holidays 1993 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1993: (
            (JAN, 4, JAN, 2),
            (MAY, 3, MAY, 1),
            (MAY, 4, MAY, 2),
            (MAY, 10, MAY, 9),
            (JUN, 14, JUN, 12),
            (NOV, 8, NOV, 7),
        ),
        # Substituted Holidays 1994 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1994: (
            (JAN, 3, JAN, 1),
            (JAN, 4, JAN, 2),
            (MAY, 3, MAY, 1),
            (MAY, 10, MAY, 9),
            (JUN, 13, JUN, 12),
        ),
        # Substituted Holidays 1995 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1995: (
            (JAN, 3, JAN, 1),
            (JAN, 9, JAN, 7),
        ),
        # Substituted Holidays 1996 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1996: (JAN, 8, JAN, 7),
        # Substituted Holidays 1997 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1997: (MAR, 10, MAR, 8),
        # Substituted Holidays 1998 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1998: (
            (MAR, 9, MAR, 8),
            (MAY, 4, MAY, 2),
            (MAY, 11, MAY, 9),
            (NOV, 9, NOV, 7),
            (DEC, 14, DEC, 12),
        ),
        # Substituted Holidays 1999 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        1999: (
            (JAN, 4, JAN, 2),
            (MAY, 3, MAY, 1),
            (MAY, 4, MAY, 2),
            (MAY, 10, MAY, 9),
            (JUN, 14, JUN, 12),
            (NOV, 8, NOV, 7),
            (DEC, 13, DEC, 12),
        ),
        # Substituted Holidays 2000 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2000: (
            (JAN, 3, JAN, 1),
            (JAN, 4, JAN, 2),
        ),
        # Substituted Holidays 2001 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2001: (JAN, 8, JAN, 7),
        # Substituted Holidays 2002 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2002: (FEB, 25, FEB, 23),
        # Substituted Holidays 2003 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2003: (
            (FEB, 24, FEB, 23),
            (MAR, 10, MAR, 8),
        ),
        # Substituted Holidays 2004 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2004: (
            (MAY, 3, MAY, 1),
            (MAY, 4, MAY, 2),
            (MAY, 10, MAY, 9),
            (JUN, 14, JUN, 12),
            (NOV, 8, NOV, 7),
            (DEC, 13, DEC, 12),
        ),
        # Substituted Holidays 2005 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2005: (
            (JAN, 6, JAN, 1),
            (JAN, 10, JAN, 2),
            (MAY, 2, MAY, 1),
            (JUN, 13, JUN, 12),
        ),
        # Substituted Holidays 2006 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2006: (
            (JAN, 6, JAN, 1),
            (JAN, 9, JAN, 7),
            (NOV, 6, NOV, 4),
        ),
        # Substituted Holidays 2007 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2007: (
            (JAN, 8, JAN, 7),
            (NOV, 5, NOV, 4),
        ),
        # Substituted Holidays 2008 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2008: (
            (JAN, 8, JAN, 5),
            (FEB, 25, FEB, 23),
            (MAR, 10, MAR, 8),
        ),
        # Substituted Holidays 2009 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2009: (
            (JAN, 6, JAN, 3),
            (JAN, 8, JAN, 4),
            (MAR, 9, MAR, 8),
            (MAY, 11, MAY, 9),
        ),
        # Substituted Holidays 2010 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2010: (
            (JAN, 6, JAN, 2),
            (JAN, 8, JAN, 3),
            (MAY, 3, MAY, 1),
            (MAY, 10, MAY, 9),
            (JUN, 14, JUN, 12),
        ),
        # Substituted Holidays 2011 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2011: (
            (JAN, 6, JAN, 1),
            (JAN, 10, JAN, 2),
            (MAY, 2, MAY, 1),
            (JUN, 13, JUN, 12),
        ),
        # Substituted Holidays 2012 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2012: (
            (JAN, 6, JAN, 1),
            (JAN, 9, JAN, 7),
            (NOV, 5, NOV, 4),
        ),
        # Substituted Holidays 2014 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2014: (MAR, 10, MAR, 8),
        # Substituted Holidays 2015 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2015: (
            (MAR, 9, MAR, 8),
            (MAY, 11, MAY, 9),
        ),
        # Substituted Holidays 2016 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2016: (
            (MAY, 2, MAY, 1),
            (JUN, 13, JUN, 12),
        ),
        # Substituted Holidays 2017 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2017: (NOV, 6, NOV, 4),
        # Substituted Holidays 2018 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2018: (NOV, 5, NOV, 4),
        # Substituted Holidays 2020 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2020: (
            (FEB, 24, FEB, 23),
            (MAR, 9, MAR, 8),
            (MAY, 11, MAY, 9),
        ),
        # Substituted Holidays 2021 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2021: (
            (MAY, 3, MAY, 1),
            (MAY, 10, MAY, 9),
            (JUN, 14, JUN, 12),
        ),
        # Substituted Holidays 2022 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2022: (
            (MAY, 2, MAY, 1),
            (JUN, 13, JUN, 12),
        ),
        # Substituted Holidays 2023 (observed)
        # src: https://ru.wikipedia.org/wiki/Праздники_России
        2023: (NOV, 6, NOV, 4),
    }
