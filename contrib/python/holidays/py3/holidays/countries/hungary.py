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

from holidays.calendars.gregorian import JAN, MAR, APR, MAY, AUG, OCT, NOV, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Hungary(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Hungary holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Hungary>
        * <https://web.archive.org/web/20191002190510/http://hvg.hu:80/gazdasag/20170307_Megszavaztak_munkaszuneti_nap_lett_a_nagypentek>
        * <https://web.archive.org/web/20240307183735/http://www.tankonyvtar.hu/hu/tartalom/historia/92-10/ch01.html>
    """

    country = "HU"
    default_language = "hu"
    start_year = 1945
    supported_languages = ("en_US", "hu", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, HungaryStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Újév"))

        if self._year <= 1950 or self._year >= 1989:
            # National Day.
            name = tr("Nemzeti ünnep")
            self._add_holiday_mar_15(name)
            if self._year >= 1991:
                self._add_holiday_oct_23(name)

        if self._year >= 2017:
            # Good Friday.
            self._add_good_friday(tr("Nagypéntek"))

        # Easter.
        self._add_easter_sunday(tr("Húsvét"))

        if self._year != 1955:
            # Easter Monday.
            self._add_easter_monday(tr("Húsvét Hétfő"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Pünkösd"))

        if self._year <= 1952 or self._year >= 1992:
            # Whit Monday.
            self._add_whit_monday(tr("Pünkösdhétfő"))

        if self._year >= 1946:
            # Labor Day.
            name = tr("A Munka ünnepe")
            self._add_labor_day(name)
            if 1950 <= self._year <= 1953:
                self._add_labor_day_two(name)

        self._add_holiday_aug_20(
            # Bread Day.
            tr("A kenyér ünnepe")
            if 1950 <= self._year <= 1989
            # State Foundation Day.
            else tr("Az államalapítás ünnepe")
        )

        if self._year >= 1999:
            # All Saints' Day.
            self._add_all_saints_day(tr("Mindenszentek"))

        # Christmas Day.
        self._add_christmas_day(tr("Karácsony"))

        if self._year != 1955:
            # Second Day of Christmas.
            self._add_christmas_day_two(tr("Karácsony másnapja"))

        # Soviet era.
        if 1950 <= self._year <= 1989:
            # Proclamation of Soviet Republic Day.
            self._add_holiday_mar_21(tr("A Tanácsköztársaság kikiáltásának ünnepe"))

            # Liberation Day.
            self._add_holiday_apr_4(tr("A felszabadulás ünnepe"))

            if self._year not in {1956, 1989}:
                # Great October Socialist Revolution Day.
                self._add_holiday_nov_7(tr("A nagy októberi szocialista forradalom ünnepe"))


class HU(Hungary):
    pass


class HUN(Hungary):
    pass


class HungaryStaticHolidays:
    """Hungary special holidays.

    References:
        * [1991](https://archive.org/details/7-1990-xii-27-mum-rendelet)
        * [1992](https://web.archive.org/web/20250526085935/https://jogkodex.hu/jsz/1992_3_mum_rendelet_5937748)
        * [1993](https://web.archive.org/web/20250526090343/https://jogkodex.hu/jsz/1992_7_mum_rendelet_7815697)
        * [1994](https://web.archive.org/web/20250526090541/https://jogkodex.hu/jsz/1993_3_mum_rendelet_3120363)
        * [1997](https://web.archive.org/web/20250526090817/https://jogkodex.hu/jsz/1996_11_mum_rendelet_3554324)
        * [1998](https://web.archive.org/web/20250526091318/https://jogkodex.hu/jsz/1997_18_mum_rendelet_7493439)
        * [1999](https://web.archive.org/web/20250526093916/https://jogkodex.hu/jsz/1998_3_szcsm_rendelet_7336830)
        * [2001](https://archive.org/details/43-2000-xii-18-gm-rendelet)
        * [2002](https://web.archive.org/web/20250526071659/https://jogkodex.hu/jsz/2001_25_gm_rendelet_9200619)
        * [2003](https://web.archive.org/web/20250526071517/https://jogkodex.hu/jsz/2002_2_fmm_rendelet_1831209)
        * [2004](https://web.archive.org/web/20250526071310/https://jogkodex.hu/jsz/2003_9_fmm_rendelet_8666269)
        * [2005](https://web.archive.org/web/20250526071135/https://jogkodex.hu/jsz/2004_25_fmm_rendelet_4309634)
        * [2007](https://web.archive.org/web/20250526064108/https://jogkodex.hu/jsz/2006_4_szmm_rendelet_8628960)
        * [2008](https://web.archive.org/web/20250526051643/https://jogkodex.hu/jsz/2007_27_szmm_rendelet_3904252)
        * [2009](https://web.archive.org/web/20250526051816/https://jogkodex.hu/jsz/2008_16_szmm_rendelet_7668376)
        * [2010](https://web.archive.org/web/20250428204804/https://njt.hu/jogszabaly/2009-20-20-1X)
        * [2011](https://web.archive.org/web/20250428204915/https://njt.hu/jogszabaly/2010-7-20-2X)
        * [2012](https://web.archive.org/web/20250428204812/https://njt.hu/jogszabaly/2011-39-20-2X)
        * [2012-2013](https://web.archive.org/web/20230719163315/https://njt.hu/jogszabaly/2012-28-20-2X)
        * [2014](https://web.archive.org/web/20241104082745/https://njt.hu/jogszabaly/2013-33-20-2X)
        * [2015](https://web.archive.org/web/20241104081744/https://njt.hu/jogszabaly/2014-28-20-2X)
        * [2016](https://web.archive.org/web/20230719163025/https://njt.hu/jogszabaly/2015-18-20-2X)
        * [2018](https://web.archive.org/web/20250429080658/https://njt.hu/jogszabaly/2017-61-B0-15)
        * [2019](https://web.archive.org/web/20241211095342/https://njt.hu/jogszabaly/2018-6-20-53)
        * [2020](https://web.archive.org/web/20241104072826/https://njt.hu/jogszabaly/2019-7-20-53)
        * [2021](https://web.archive.org/web/20241102122816/https://njt.hu/jogszabaly/2020-14-20-7Q)
        * [2022](https://web.archive.org/web/20241107133627/https://njt.hu/jogszabaly/2021-23-20-7Q)
        * [2024](https://web.archive.org/web/20241105131832/https://njt.hu/jogszabaly/2023-15-20-8P)
        * [2025](https://web.archive.org/web/20241219165144/https://njt.hu/jogszabaly/2024-11-20-2X)
        * [2026](https://web.archive.org/web/20250526083742/https://jogkodex.hu/jsz/2025_10_ngm_rendelet_5591314)
    """

    # Substituted date format.
    substituted_date_format = tr("%Y. %m. %d.")
    # Day off (substituted from %s).
    substituted_label = tr("Pihenőnap (%s-től helyettesítve)")
    special_public_holidays = {
        1991: (AUG, 19, AUG, 17),
        1992: (
            (AUG, 21, AUG, 29),
            (DEC, 24, DEC, 19),
        ),
        1993: (DEC, 24, DEC, 18),
        1994: (MAR, 14, MAR, 12),
        1997: (
            (MAY, 2, APR, 26),
            (OCT, 24, OCT, 18),
            (DEC, 24, DEC, 20),
        ),
        1998: (
            (JAN, 2, JAN, 10),
            (AUG, 21, AUG, 15),
            (DEC, 24, DEC, 19),
        ),
        1999: (DEC, 24, DEC, 18),
        2001: (
            (MAR, 16, MAR, 10),
            (APR, 30, APR, 28),
            (OCT, 22, OCT, 20),
            (NOV, 2, OCT, 27),
            (DEC, 24, DEC, 22),
            (DEC, 31, DEC, 29),
        ),
        2002: (
            (AUG, 19, AUG, 10),
            (DEC, 24, DEC, 28),
        ),
        2003: (
            (MAY, 2, APR, 26),
            (OCT, 24, OCT, 18),
            (DEC, 24, DEC, 13),
        ),
        2004: (
            (JAN, 2, JAN, 10),
            (DEC, 24, DEC, 18),
        ),
        2005: (
            (MAR, 14, MAR, 19),
            (OCT, 31, NOV, 5),
        ),
        2007: (
            (MAR, 16, MAR, 10),
            (APR, 30, APR, 21),
            (OCT, 22, OCT, 20),
            (NOV, 2, OCT, 27),
            (DEC, 24, DEC, 22),
            (DEC, 31, DEC, 29),
        ),
        2008: (
            (MAY, 2, APR, 26),
            (OCT, 24, OCT, 18),
            (DEC, 24, DEC, 20),
        ),
        2009: (
            (JAN, 2, MAR, 28),
            (AUG, 21, AUG, 29),
            (DEC, 24, DEC, 19),
        ),
        2010: (DEC, 24, DEC, 11),
        2011: (
            (MAR, 14, MAR, 19),
            (OCT, 31, NOV, 5),
        ),
        2012: (
            (MAR, 16, MAR, 24),
            (APR, 30, APR, 21),
            (OCT, 22, OCT, 27),
            (NOV, 2, NOV, 10),
            (DEC, 24, DEC, 15),
            (DEC, 31, DEC, 1),
        ),
        2013: (
            (AUG, 19, AUG, 24),
            (DEC, 24, DEC, 7),
            (DEC, 27, DEC, 21),
        ),
        2014: (
            (MAY, 2, MAY, 10),
            (OCT, 24, OCT, 18),
            (DEC, 24, DEC, 13),
        ),
        2015: (
            (JAN, 2, JAN, 10),
            (AUG, 21, AUG, 8),
            (DEC, 24, DEC, 12),
        ),
        2016: (
            (MAR, 14, MAR, 5),
            (OCT, 31, OCT, 15),
        ),
        2018: (
            (MAR, 16, MAR, 10),
            (APR, 30, APR, 21),
            (OCT, 22, OCT, 13),
            (NOV, 2, NOV, 10),
            (DEC, 24, DEC, 1),
            (DEC, 31, DEC, 15),
        ),
        2019: (
            (AUG, 19, AUG, 10),
            (DEC, 24, DEC, 7),
            (DEC, 27, DEC, 14),
        ),
        2020: (
            (AUG, 21, AUG, 29),
            (DEC, 24, DEC, 12),
        ),
        2021: (DEC, 24, DEC, 11),
        2022: (
            (MAR, 14, MAR, 26),
            (OCT, 31, OCT, 15),
        ),
        2024: (
            (AUG, 19, AUG, 3),
            (DEC, 24, DEC, 7),
            (DEC, 27, DEC, 14),
        ),
        2025: (
            (MAY, 2, MAY, 17),
            (OCT, 24, OCT, 18),
            (DEC, 24, DEC, 13),
        ),
        2026: (
            (JAN, 2, JAN, 10),
            (AUG, 21, AUG, 8),
            (DEC, 24, DEC, 12),
        ),
    }
