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

from holidays.calendars.gregorian import _timedelta, FRI, SAT
from holidays.constants import OPTIONAL, PUBLIC, SCHOOL
from holidays.groups import HebrewCalendarHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    THU_TO_PREV_WED,
    FRI_TO_PREV_WED,
    FRI_TO_PREV_THU,
    SAT_TO_PREV_THU,
    SAT_TO_NEXT_SUN,
    SUN_TO_NEXT_MON,
)


class Israel(ObservedHolidayBase, HebrewCalendarHolidays):
    """Israel holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Israel>
        * <https://web.archive.org/web/20190923042619/https://www.knesset.gov.il/laws/special/heb/jerusalem_day_law.htm>
    """

    country = "IL"
    default_language = "he"
    # %s (observed).
    observed_label = tr("%s (נצפה)")
    supported_categories = (OPTIONAL, PUBLIC, SCHOOL)
    supported_languages = ("en_US", "he", "th", "uk")
    weekend = {FRI, SAT}
    start_year = 1948

    def __init__(self, *args, **kwargs):
        HebrewCalendarHolidays.__init__(self)
        kwargs.setdefault("observed_rule", FRI_TO_PREV_THU + SAT_TO_PREV_THU)
        super().__init__(*args, **kwargs)

    def _add_observed(self, dt, name, rule):
        is_observed, _ = super()._add_observed(dt, name, rule=rule)
        if not is_observed:
            self._add_holiday(name, dt)

    def _populate_public_holidays(self):
        # Rosh Hashanah (New Year).
        self._add_rosh_hashanah(tr("ראש השנה"), range(2))

        # Yom Kippur (Day of Atonement).
        self._add_yom_kippur(tr("יום כיפור"))

        # Sukkot (Feast of Tabernacles).
        self._add_sukkot(tr("סוכות"))
        # Simchat Torah / Shemini Atzeret.
        self._add_sukkot(tr("שמחת תורה/שמיני עצרת"), +7)

        # Pesach (Passover).
        self._add_passover(tr("פסח"))
        # Shvi'i shel Pesach (Seventh day of Passover)
        self._add_passover(tr("שביעי של פסח"), +6)

        rule = FRI_TO_PREV_THU + SAT_TO_PREV_THU
        if self._year >= 2004:
            rule += MON_TO_NEXT_TUE
        self._add_observed(
            self._hebrew_calendar.israel_independence_date(self._year),
            # Yom Ha-Atzmaut (Independence Day).
            tr("יום העצמאות"),
            rule,
        )

        # Shavuot.
        self._add_shavuot(tr("שבועות"))

    def _populate_optional_holidays(self):
        # Chol HaMoed Sukkot (Feast of Tabernacles holiday).
        self._add_sukkot(tr("חול המועד סוכות"), range(1, 6))

        if self._year >= 2008:
            # Sigd.
            self._add_yom_kippur(tr("סיגד"), +49)

        # Purim.
        self._add_purim(tr("פורים"))

        # Chol HaMoed Pesach (Passover holiday).
        self._add_passover(tr("חול המועד פסח"), range(1, 6))

        if self._year >= 1963:
            rule = THU_TO_PREV_WED + FRI_TO_PREV_WED
            if self._year >= 2004:
                rule += SUN_TO_NEXT_MON
            self._add_observed(
                _timedelta(self._hebrew_calendar.israel_independence_date(self._year), -1),
                # Yom Hazikaron (Fallen Soldiers and Victims of Terrorism Remembrance Day).
                tr("יום הזיכרון לחללי מערכות ישראל ונפגעי פעולות האיבה"),
                rule,
            )

        if self._year >= 1998:
            # Yom Yerushalayim (Jerusalem Day).
            self._add_lag_baomer(tr("יום ירושלים"), +10)

        self._add_observed(
            self._hebrew_calendar.tisha_bav_date(self._year),
            # Tisha B'Av (Tisha B'Av, fast).
            tr("תשעה באב"),
            SAT_TO_NEXT_SUN,
        )

    def _populate_school_holidays(self):
        # Chol HaMoed Sukkot (Feast of Tabernacles holiday).
        self._add_sukkot(tr("חול המועד סוכות"), range(1, 6))

        # Hanukkah.
        self._add_hanukkah(tr("חנוכה"), range(8))

        self._add_observed(
            _timedelta(self._hebrew_calendar.purim_date(self._year), -1),
            # Ta`anit Ester (Fast of Esther).
            tr("תענית אסתר"),
            SAT_TO_PREV_THU,
        )

        # Purim.
        self._add_purim(tr("פורים"))

        # Chol HaMoed Pesach (Passover holiday).
        self._add_passover(tr("חול המועד פסח"), range(1, 6))

        # Lag Ba'omer (Lag BaOmer).
        self._add_lag_baomer(tr('ל"ג בעומר'))


class IL(Israel):
    pass


class ISR(Israel):
    pass
