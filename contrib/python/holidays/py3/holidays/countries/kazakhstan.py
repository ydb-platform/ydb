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
from holidays.calendars.gregorian import JAN, MAR, APR, MAY, JUL, AUG, SEP, OCT, DEC
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_WORKDAY


class Kazakhstan(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Kazakhstan holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Kazakhstan>
        * <https://web.archive.org/web/20241225060133/https://egov.kz/cms/en/articles/holidays-calend>
        * <https://web.archive.org/web/20240828112119/https://adilet.zan.kz/kaz/docs/Z010000267_/history>
        * <https://web.archive.org/web/20240918145208/https://adilet.zan.kz/kaz/docs/Z990000493_#z63>

    Islamic holidays:
        * [2025](https://web.archive.org/web/20250429084936/https://qazinform.com/news/first-day-of-ramadan-to-fall-on-march-1-2025-ca393f)
    """

    country = "KZ"
    default_language = "kk"
    # %s (estimated).
    estimated_label = tr("%s (бағаланған)")
    # %s (observed).
    observed_label = tr("%s (қайта белгіленген демалыс)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (қайта белгіленген демалыс, бағаланған)")
    supported_languages = ("en_US", "kk", "uk")
    # Kazakhstan declared its sovereignty on 25 October 1990.
    start_year = 1991

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
            self, cls=KazakhstanIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, KazakhstanStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        kwargs.setdefault("observed_since", 2002)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        name = tr("Жаңа жыл")
        dts_observed.add(self._add_new_years_day(name))
        dts_observed.add(self._add_new_years_day_two(name))

        if self._year >= 2006:
            # Orthodox Christmas.
            self._add_christmas_day(tr("Православиелік Рождество"))

        # International Women's Day.
        dts_observed.add(self._add_womens_day(tr("Халықаралық әйелдер күні")))

        if self._year >= 2002:
            # Nowruz Holiday.
            name = tr("Наурыз мейрамы")
            dts_observed.add(self._add_holiday_mar_22(name))
            if self._year >= 2010:
                dts_observed.add(self._add_holiday_mar_21(name))
                dts_observed.add(self._add_holiday_mar_23(name))

        # Kazakhstan's People Solidarity Holiday.
        dts_observed.add(self._add_labor_day(tr("Қазақстан халқының бірлігі мерекесі")))

        if self._year >= 2013:
            # Defender of the Fatherland Day.
            dts_observed.add(self._add_holiday_may_7(tr("Отан Қорғаушы күні")))

        # Victory Day.
        dt = self._add_world_war_two_victory_day(tr("Жеңіс күні"), is_western=False)
        if self._year != 2020:
            dts_observed.add(dt)

        if self._year >= 2009:
            # Capital Day.
            dts_observed.add(self._add_holiday_jul_6(tr("Астана күні")))

        if self._year >= 1996:
            dts_observed.add(
                # Constitution Day.
                self._add_holiday_aug_30(tr("Қазақстан Республикасының Конституциясы күні"))
            )

        if 1994 <= self._year <= 2008 or self._year >= 2022:
            # Republic Day.
            dts_observed.add(self._add_holiday_oct_25(tr("Республика күні")))

        if 2012 <= self._year <= 2021:
            dts_observed.add(
                # First President Day.
                self._add_holiday_dec_1(tr("Қазақстан Республикасының Тұңғыш Президенті күні"))
            )

        # Independence Day.
        name = tr("Тəуелсіздік күні")
        dts_observed.add(self._add_holiday_dec_16(name))
        if 2002 <= self._year <= 2021:
            dts_observed.add(self._add_holiday_dec_17(name))

        if self.observed:
            self._populate_observed(dts_observed)

        if self._year >= 2006:
            # Eid al-Adha.
            self._add_eid_al_adha_day(tr("Құрбан айт"))


class KZ(Kazakhstan):
    pass


class KAZ(Kazakhstan):
    pass


class KazakhstanIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2006, 2025)
    EID_AL_ADHA_DATES = {
        2006: (JAN, 10),
        2015: (SEP, 24),
        2016: (SEP, 12),
    }


class KazakhstanStaticHolidays:
    """Kazakhstan special holidays.

    References:
        * [2000](https://web.archive.org/web/20250428203427/https://adilet.zan.kz/kaz/docs/P000000642_)
        * 2001:
            * <https://web.archive.org/web/20200221115328/http://adilet.zan.kz:80/kaz/docs/P010000282_>
            * <https://web.archive.org/web/20250428203415/https://adilet.zan.kz/kaz/docs/P010000515_>
            * <https://web.archive.org/web/20250428203411/https://adilet.zan.kz/kaz/docs/P010001604_>
        * [2002](https://web.archive.org/web/20250428203447/https://adilet.zan.kz/kaz/docs/P020000466_)
        * 2003:
            * <https://web.archive.org/web/20250428203424/https://adilet.zan.kz/kaz/docs/P030000338_>
            * <https://web.archive.org/web/20240913114351/https://www.adilet.zan.kz/kaz/docs/P030001166_>
        * 2005:
            * <https://web.archive.org/web/20180101044854/http://adilet.zan.kz:80/kaz/docs/P050000142_>
            * <https://web.archive.org/web/20250428203454/https://adilet.zan.kz/kaz/docs/P050000751_>
            * <https://web.archive.org/web/20250428204009/https://adilet.zan.kz/kaz/docs/P050000949_>
        * 2006:
            * <https://web.archive.org/web/20250428204118/https://adilet.zan.kz/kaz/docs/P050001309_>
            * <https://web.archive.org/web/20250428204001/https://adilet.zan.kz/kaz/docs/P060000277_>
        * 2007:
            * <https://web.archive.org/web/20250428204133/https://adilet.zan.kz/kaz/docs/P070000148_>
            * <https://web.archive.org/web/20250428204131/https://adilet.zan.kz/kaz/docs/P070000165_>
            * <https://web.archive.org/web/20250428204025/https://adilet.zan.kz/kaz/docs/P070000713_>
            * <https://web.archive.org/web/20250428204150/https://adilet.zan.kz/kaz/docs/P070000925_>
            * <https://web.archive.org/web/20250428204033/https://adilet.zan.kz/kaz/docs/P070001113_>
        * [2008](https://web.archive.org/web/20250428204634/https://adilet.zan.kz/kaz/docs/P080000364_)
        * [2009](https://web.archive.org/web/20250428204627/https://adilet.zan.kz/kaz/docs/P090001936_)
        * 2010:
            * <https://web.archive.org/web/20250428204638/https://adilet.zan.kz/kaz/docs/P090002216_>
            * <https://web.archive.org/web/20250428204659/https://adilet.zan.kz/kaz/docs/P100000637_>
        * 2011:
            * <https://web.archive.org/web/20170602150014/http://adilet.zan.kz:80/kaz/docs/P1100000167>
            * <https://web.archive.org/web/20250428204646/https://adilet.zan.kz/kaz/docs/P1100000948>
        * 2012:
            * <https://web.archive.org/web/20250429081405/https://adilet.zan.kz/kaz/docs/P1200000268>
            * <https://web.archive.org/web/20250428204720/https://adilet.zan.kz/kaz/docs/P1200000458>
            * <https://web.archive.org/web/20250428204647/https://adilet.zan.kz/kaz/docs/P1200001538>
        * 2013:
            * <https://web.archive.org/web/20130424070807/http://adilet.zan.kz/kaz/docs/P1300000345>
            * <https://web.archive.org/web/20250428204654/https://adilet.zan.kz/kaz/docs/P1300001068>
            * <https://web.archive.org/web/20131213045017/http://adilet.zan.kz/kaz/docs/P1300001322>
        * [2014](https://web.archive.org/web/20200715173002/http://adilet.zan.kz/kaz/docs/P1400000365)
        * [2016](https://web.archive.org/web/20200225220136/http://adilet.zan.kz/kaz/docs/P1600000067)
        * [2017](https://web.archive.org/web/20200203183402/http://adilet.zan.kz/kaz/docs/P1700000005)
        * [2018](https://web.archive.org/web/20200226012057/http://adilet.zan.kz/kaz/docs/P1700000864)
        * [2019](https://web.archive.org/web/20200630083024/http://adilet.zan.kz/kaz/docs/P1800000888)
        * [2020](https://web.archive.org/web/20221023205506/https://adilet.zan.kz/kaz/docs/P1900000820)
        * [2021](https://web.archive.org/web/20250428204757/https://adilet.zan.kz/kaz/docs/P2000000930)
        * [2022](https://web.archive.org/web/20250428204753/https://adilet.zan.kz/kaz/docs/P2200000796)
        * [2023](https://web.archive.org/web/20230928205344/https://adilet.zan.kz/kaz/docs/P2300000326)
        * [2024](https://web.archive.org/web/20250108222408/https://adilet.zan.kz/kaz/docs/G24G0000109)
        * [2025](https://web.archive.org/web/20250428203311/https://adilet.zan.kz/kaz/docs/G24G0000436)
    """

    # Substituted date format.
    substituted_date_format = tr("%d.%m.%Y")
    # Day off (substituted from %s).
    substituted_label = tr("Демалыс күні (%s бастап ауыстырылды)")

    special_public_holidays = {
        2000: (MAY, 8, MAY, 6),
        2001: (
            (MAR, 9, MAR, 11),
            (MAR, 23, MAR, 25),
            (APR, 30, APR, 28),
            (DEC, 31, DEC, 29),
        ),
        2002: (MAY, 10, MAY, 12),
        2003: (
            (MAY, 2, MAY, 4),
            (DEC, 15, DEC, 13),
        ),
        2005: (
            (MAR, 7, MAR, 5),
            (MAR, 21, MAR, 19),
            (AUG, 29, AUG, 27),
            (OCT, 24, OCT, 22),
        ),
        2006: (
            (JAN, 11, JAN, 14),
            (MAY, 8, MAY, 6),
        ),
        2007: (
            (MAR, 9, MAR, 11),
            (MAR, 23, MAR, 25),
            (AUG, 31, SEP, 2),
            (OCT, 26, OCT, 28),
            (DEC, 31, DEC, 29),
        ),
        2008: (MAY, 2, MAY, 4),
        2009: (DEC, 18, DEC, 20),
        2010: (
            (JAN, 8, JAN, 10),
            (JUL, 5, JUL, 3),
        ),
        2011: (
            (MAR, 7, MAR, 5),
            (AUG, 29, AUG, 27),
        ),
        2012: (
            (MAR, 9, MAR, 11),
            (APR, 30, APR, 28),
            (DEC, 31, DEC, 29),
        ),
        2013: (
            (MAY, 10, MAY, 4),
            (OCT, 14, OCT, 12),
        ),
        2014: (
            (JAN, 3, DEC, 28, 2013),
            (MAY, 2, MAY, 4),
            (MAY, 8, MAY, 11),
        ),
        2016: (MAR, 7, MAR, 5),
        2017: (
            (MAR, 20, MAR, 18),
            (JUL, 7, JUL, 1),
        ),
        2018: (
            (MAR, 9, MAR, 3),
            (APR, 30, APR, 28),
            (MAY, 8, MAY, 5),
            (AUG, 31, AUG, 25),
            (DEC, 31, DEC, 29),
        ),
        2019: (MAY, 10, MAY, 4),
        2020: (
            (JAN, 3, JAN, 5),
            (DEC, 18, DEC, 20),
        ),
        2021: (JUL, 5, JUL, 3),
        2022: (
            (MAR, 7, MAR, 5),
            (AUG, 29, AUG, 27),
            (OCT, 24, OCT, 22),
        ),
        2023: (JUL, 7, JUL, 1),
        2024: (MAY, 8, MAY, 4),
        2025: (JAN, 3, JAN, 5),
    }

    special_public_holidays_observed = {
        # Victory Day.
        2020: (MAY, 8, tr("Жеңіс күні")),
    }
