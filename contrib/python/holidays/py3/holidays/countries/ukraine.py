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

from datetime import date
from gettext import gettext as tr

from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.calendars.julian_revised import JULIAN_REVISED_CALENDAR
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_WORKDAY


class Ukraine(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Ukraine holidays.

    References:
        * [Labor Code of Ukraine, Art. 73](https://web.archive.org/web/20240607021920/https://zakon1.rada.gov.ua/laws/show/322-08/paran454)
        * <https://web.archive.org/web/20240612025118/https://zakon.rada.gov.ua/laws/show/585-12>
    """

    country = "UA"
    default_language = "uk"
    # %s (observed).
    observed_label = tr("%s (вихідний)")
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("ar", "en_US", "th", "uk")
    # The current set of holidays came into force in 1991.
    start_year = 1991

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_REVISED_CALENDAR)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, UkraineStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _is_observed(self, dt: date) -> bool:
        # 27.01.1995: holiday on weekend move to next workday
        # https://web.archive.org/web/20230731064358/https://zakon.rada.gov.ua/laws/show/35/95-вр#Text
        # 10.01.1998: cancelled
        # https://web.archive.org/web/20250121184919/https://zakon.rada.gov.ua/laws/show/785/97-вр
        # 23.04.1999: holiday on weekend move to next workday
        # https://web.archive.org/web/20240315074159/http://zakon.rada.gov.ua/laws/show/576-14
        return date(1995, JAN, 27) <= dt <= date(1998, JAN, 9) or dt >= date(1999, APR, 23)

    def _populate_common(self, *, is_martial_law: bool = False):
        # There is no public holidays in Ukraine during the period of martial law
        # https://web.archive.org/web/20250418204733/https://zakon.rada.gov.ua/laws/show/2136-20
        # law is in force from March 15, 2022
        dts_observed = set()

        if (self._year >= 2023) == is_martial_law:
            # New Year's Day.
            dts_observed.add(self._add_new_years_day(tr("Новий рік")))

            if self._year <= 2023:
                # Christmas Day.
                dts_observed.add(self._add_christmas_day(tr("Різдво Христове"), JULIAN_CALENDAR))

            # International Women's Day.
            dts_observed.add(self._add_womens_day(tr("Міжнародний жіночий день")))

        if (self._year >= 2022) is is_martial_law:
            # Easter Sunday (Pascha).
            dts_observed.add(self._add_easter_sunday(tr("Великдень (Пасха)")))

            # Holy Trinity Day.
            dts_observed.add(self._add_whit_sunday(tr("Трійця")))

            name = (
                # Labor Day.
                tr("День праці")
                if self._year >= 2018
                # International Workers' Solidarity Day.
                else tr("День міжнародної солідарності трудящих")
            )
            dts_observed.add(self._add_labor_day(name))
            if self._year <= 2017:
                dts_observed.add(self._add_labor_day_two(name))

            name = (
                # Day of Remembrance and Victory over Nazism in World War II 1939-1945.
                tr("День памʼяті та перемоги над нацизмом у Другій світовій війні 1939-1945 років")
                if self._year >= 2024
                # Day of Victory over Nazism in World War II (Victory Day).
                else tr("День перемоги над нацизмом у Другій світовій війні (День перемоги)")
                if self._year >= 2016
                # Victory Day.
                else tr("День Перемоги")
            )
            dts_observed.add(
                self._add_world_war_two_victory_day(name, is_western=(self._year >= 2024))
            )

            if self._year >= 1997:
                # Day of the Constitution of Ukraine.
                dts_observed.add(self._add_holiday_jun_28(tr("День Конституції України")))

            if self._year >= 2022:
                # Ukrainian Statehood Day.
                name = tr("День Української Державності")
                dts_observed.add(
                    self._add_holiday_jul_15(name)
                    if self._year >= 2024
                    else self._add_holiday_jul_28(name)
                )

            # Independence Day.
            name = tr("День незалежності України")
            if self._year >= 1992:
                dts_observed.add(self._add_holiday_aug_24(name))
            else:
                self._add_holiday_jul_16(name)

            if self._year >= 2015:
                name = (
                    # Day of defenders of Ukraine.
                    tr("День захисників і захисниць України")
                    if self._year >= 2021
                    # Defender of Ukraine Day.
                    else tr("День захисника України")
                )
                dts_observed.add(
                    self._add_holiday_oct_1(name)
                    if self._year >= 2023
                    else self._add_holiday_oct_14(name)
                )

            if self._year <= 1999:
                # Anniversary of the Great October Socialist Revolution.
                name = tr("Річниця Великої Жовтневої соціалістичної революції")
                dts_observed.add(self._add_holiday_nov_7(name))
                dts_observed.add(self._add_holiday_nov_8(name))

            if self._year >= 2017:
                # Christmas Day.
                dts_observed.add(self._add_christmas_day(tr("Різдво Христове")))

        if self.observed and not is_martial_law:
            self._populate_observed(dts_observed)

    def _populate_public_holidays(self):
        self._populate_common()

    def _populate_workday_holidays(self):
        self._populate_common(is_martial_law=True)


class UA(Ukraine):
    pass


class UKR(Ukraine):
    pass


class UkraineStaticHolidays:
    """Ukraine special holidays.

    Substituted holidays References:
        * [1991](https://web.archive.org/web/20220830105426/https://zakon.rada.gov.ua/laws/show/60-91-п)
        * [1992 [1]](https://web.archive.org/web/20220816132241/https://zakon.rada.gov.ua/laws/show/202-92-п)
        * [1992 [2]](https://web.archive.org/web/20220514124422/https://zakon.rada.gov.ua/laws/show/377-91-п)
        * [1993 [1]](https://web.archive.org/web/20220429231922/https://zakon.rada.gov.ua/laws/show/563-93-п/)
        * [1993 [2]](https://web.archive.org/web/20220501192004/https://zakon.rada.gov.ua/laws/show/725-92-п/)
        * [1994](https://web.archive.org/web/20220423134711/https://zakon.rada.gov.ua/laws/show/98-94-п)
        * [1995 [1]](https://web.archive.org/web/20220416193351/https://zakon.rada.gov.ua/laws/show/852-95-п/)
        * [1995 [2]](https://web.archive.org/web/20220727233924/https://zakon.rada.gov.ua/laws/show/634-95-п)
        * [1995 [3]](https://web.archive.org/web/20220404230852/https://zakon.rada.gov.ua/laws/show/266-95-п)
        * [1996](https://web.archive.org/web/20220703182454/https://zakon.rada.gov.ua/laws/show/424-96-п)
        * [1997 [1]](https://web.archive.org/web/20220710133208/https://zakon.rada.gov.ua/laws/show/326-97-п)
        * [1997 [2]](https://web.archive.org/web/20240707083032/https://zakon.rada.gov.ua/laws/show/1547-96-п)
        * [1998](https://web.archive.org/web/20220516171244/https://zakon.rada.gov.ua/laws/show/1404-97-п>)
        * [1999 [1]](https://web.archive.org/web/20220721004702/https://zakon.rada.gov.ua/laws/show/1433-99-п>)
        * [1999 [2]](https://web.archive.org/web/20220701225902/https://zakon.rada.gov.ua/laws/show/558-99-п>)
        * [1999 [3]](https://web.archive.org/web/20220703131420/https://zakon.rada.gov.ua/laws/show/2070-98-п)
        * [2000 [1]](https://web.archive.org/web/20220416193413/https://zakon.rada.gov.ua/laws/show/1251-2000-п/)
        * [2000 [2]](https://web.archive.org/web/20220404231224/https://zakon.rada.gov.ua/laws/show/717-2000-п)
        * [2001 [1]](https://web.archive.org/web/20220312201133/https://zakon.rada.gov.ua/laws/show/138-2001-р/)
        * [2001 [2]](https://web.archive.org/web/20220404230657/https://zakon.rada.gov.ua/laws/show/210-2001-п)
        * [2002](https://web.archive.org/web/20220521085829/https://zakon.rada.gov.ua/laws/show/202-2002-р)
        * [2002 - 2003](https://web.archive.org/web/20220312195735/https://zakon.rada.gov.ua/laws/show/705-2002-р)
        * [2004](https://web.archive.org/web/20220404105708/https://zakon.rada.gov.ua/laws/show/773-2003-р)
        * [2005 [1]](https://web.archive.org/web/20220521235321/https://zakon.rada.gov.ua/laws/show/936-2004-р)
        * [2005 [2]](https://web.archive.org/web/20220611030516/https://zakon.rada.gov.ua/laws/show/133-2005-р)
        * [2006 [1]](https://web.archive.org/web/20240822140051/https://zakon.rada.gov.ua/laws/show/490-2005-р)
        * [2006 [2]](https://web.archive.org/web/20220312195751/https://zakon.rada.gov.ua/laws/show/562-2005-р/)
        * [2007](https://web.archive.org/web/20240823064327/https://zakon.rada.gov.ua/laws/show/612-2006-р)
        * [2008 [1]](https://web.archive.org/web/20240823064327/https://zakon.rada.gov.ua/laws/show/1059-2007-р)
        * [2008 [2]](https://web.archive.org/web/20240901160821/https://zakon.rada.gov.ua/laws/show/538-2008-р)
        * [2009](https://web.archive.org/web/20220312195619/https://zakon.rada.gov.ua/laws/show/1458-2008-р/)
        * [2010](https://web.archive.org/web/20240826001002/https://zakon.rada.gov.ua/laws/show/1412-2009-р)
        * [2011](https://web.archive.org/web/20220312200622/https://zakon.rada.gov.ua/laws/show/2130-2010-р/)
        * [2012](https://web.archive.org/web/20250119122439/https://zakon.rada.gov.ua/laws/show/1210-2011-р)
        * [2013](https://web.archive.org/web/20250119201324/https://zakon.rada.gov.ua/laws/show/1043-2012-р)
        * [2014](https://web.archive.org/web/20250421100048/https://zakon.rada.gov.ua/laws/show/920-2013-р)
        * [2015](https://web.archive.org/web/20240801225558/https://zakon.rada.gov.ua/laws/show/1084-2014-р)
        * [2016](https://web.archive.org/web/20221210054440/https://zakon.rada.gov.ua/laws/show/1155-2015-р)
        * [2017](https://web.archive.org/web/20240609025258/https://zakon.rada.gov.ua/laws/show/850-2016-р)
        * [2018](https://web.archive.org/web/20221210060148/https://zakon.rada.gov.ua/laws/show/1-2018-р)
        * [2019](https://web.archive.org/web/20220316200919/https://zakon.rada.gov.ua/laws/show/7-2019-р)
        * [2020](https://web.archive.org/web/20250423064733/https://zakon.rada.gov.ua/laws/show/995-2019-р)
        * [2021](https://web.archive.org/web/20250402142530/https://zakon.rada.gov.ua/laws/show/1191-2020-р)
        * [2022](https://web.archive.org/web/20250404010912/https://zakon.rada.gov.ua/laws/show/1004-2021-р)

    Special holidays References:
        * [1995](https://web.archive.org/web/20220713111605/https://zakon.rada.gov.ua/laws/show/13/95)
    """

    # Date format (see strftime() Format Codes)
    substituted_date_format = tr("%d.%m.%Y")
    # Day off (substituted from %s).
    substituted_label = tr("Вихідний день (перенесено з %s)")
    special_public_holidays = {
        1991: (
            (MAY, 3, MAY, 5),
            (MAY, 10, MAY, 12),
            (JUL, 15, JUL, 13),
        ),
        1992: (
            (JAN, 6, JAN, 4),
            (APR, 27, MAY, 16),
        ),
        1993: (
            (JAN, 8, JAN, 10),
            (AUG, 23, AUG, 21),
        ),
        1994: (MAR, 7, MAR, 5),
        1995: (
            # Presidential decree holiday.
            (JAN, 9, tr("Вихідний згідно указу Президента")),
            (MAY, 8, MAY, 6),
            (AUG, 25, AUG, 27),
            (NOV, 6, NOV, 4),
        ),
        1996: (
            (MAY, 3, MAY, 5),
            (MAY, 10, MAY, 12),
        ),
        1997: (
            (JAN, 2, DEC, 28, 1996),
            (JAN, 6, JAN, 4),
            (JAN, 8, JAN, 11),
            (APR, 29, APR, 19),
            (APR, 30, MAY, 17),
        ),
        1998: (JAN, 2, JAN, 4),
        1999: (
            (JAN, 8, JAN, 10),
            (APR, 12, APR, 24),
            (AUG, 23, AUG, 21),
        ),
        2000: (
            (MAY, 8, MAY, 6),
            (AUG, 25, AUG, 27),
        ),
        2001: (
            (MAR, 9, MAR, 11),
            (APR, 30, APR, 28),
            (MAY, 10, MAY, 5),
            (MAY, 11, MAY, 6),
            (JUN, 29, JUN, 23),
            (DEC, 31, DEC, 29),
        ),
        2002: (
            (MAY, 3, MAY, 11),
            (DEC, 30, DEC, 28),
            (DEC, 31, DEC, 29),
        ),
        2003: (JAN, 6, JAN, 4),
        2004: (
            (JAN, 2, JAN, 10),
            (JAN, 5, JAN, 17),
            (JAN, 6, JAN, 31),
            (AUG, 23, AUG, 21),
        ),
        2005: (
            (MAR, 7, MAR, 5),
            (MAY, 10, MAY, 14),
            (JUN, 27, JUN, 25),
        ),
        2006: (
            (JAN, 3, JAN, 21),
            (JAN, 4, FEB, 4),
            (JAN, 5, FEB, 18),
            (JAN, 6, MAR, 11),
            (MAY, 8, MAY, 6),
            (AUG, 25, SEP, 9),
        ),
        2007: (
            (JAN, 2, JAN, 20),
            (JAN, 3, JAN, 27),
            (JAN, 4, FEB, 10),
            (JAN, 5, FEB, 24),
            (MAR, 9, MAR, 3),
            (APR, 30, APR, 28),
            (JUN, 29, JUN, 16),
            (DEC, 31, DEC, 29),
        ),
        2008: (
            (JAN, 2, JAN, 12),
            (JAN, 3, JAN, 26),
            (JAN, 4, FEB, 9),
            (APR, 29, MAY, 17),
            (APR, 30, MAY, 31),
        ),
        2009: (
            (JAN, 2, JAN, 10),
            (JAN, 5, JAN, 24),
            (JAN, 6, FEB, 7),
        ),
        2010: (
            (JAN, 4, JAN, 30),
            (JAN, 5, FEB, 13),
            (JAN, 6, FEB, 27),
            (JAN, 8, MAR, 13),
            (AUG, 23, AUG, 21),
        ),
        2011: (
            (MAR, 7, MAR, 12),
            (JUN, 27, JUN, 25),
        ),
        2012: (
            (MAR, 9, MAR, 3),
            (APR, 30, APR, 28),
            (JUN, 29, JUL, 7),
            (DEC, 31, DEC, 29),
        ),
        2013: (
            (MAY, 3, MAY, 18),
            (MAY, 10, JUN, 1),
        ),
        2014: (
            (JAN, 2, JAN, 11),
            (JAN, 3, JAN, 25),
            (JAN, 6, FEB, 8),
        ),
        2015: (
            (JAN, 2, JAN, 17),
            (JAN, 8, JAN, 31),
            (JAN, 9, FEB, 14),
        ),
        2016: (
            (JAN, 8, JAN, 16),
            (MAR, 7, MAR, 12),
            (JUN, 27, JUL, 2),
        ),
        2017: (
            (MAY, 8, MAY, 13),
            (AUG, 25, AUG, 19),
        ),
        2018: (
            (MAR, 9, MAR, 3),
            (APR, 30, MAY, 5),
            (JUN, 29, JUN, 23),
            (DEC, 24, DEC, 22),
            (DEC, 31, DEC, 29),
        ),
        2019: (
            (APR, 30, MAY, 11),
            (DEC, 30, DEC, 21),
            (DEC, 31, DEC, 28),
        ),
        2020: (JAN, 6, JAN, 11),
        2021: (
            (JAN, 8, JAN, 16),
            (AUG, 23, AUG, 28),
            (OCT, 15, OCT, 23),
        ),
        2022: (MAR, 7, MAR, 12),
    }
