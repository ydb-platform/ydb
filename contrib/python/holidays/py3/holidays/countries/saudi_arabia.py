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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import JUN, SEP, NOV, THU, FRI, SAT, _timedelta
from holidays.groups import IslamicHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    THU_TO_PREV_WED,
    FRI_TO_PREV_THU,
    FRI_TO_NEXT_SAT,
    SAT_TO_NEXT_SUN,
    THU_FRI_TO_NEXT_WORKDAY,
    FRI_SAT_TO_NEXT_WORKDAY,
)


class SaudiArabia(ObservedHolidayBase, IslamicHolidays, StaticHolidays):
    """Saudi Arabia holidays.

    References:
        * <https://ar.wikipedia.org/wiki/قائمة_العطل_الرسمية_في_السعودية>
        * <https://web.archive.org/web/20240610223551/http://laboreducation.hrsd.gov.sa/en/labor-education/322>
        * <https://web.archive.org/web/20250329052253/https://english.alarabiya.net/News/gulf/2022/01/27/Saudi-Arabia-to-commemorate-Founding-Day-on-Feb-22-annually-Royal-order>
        * [2015 (1436 AH) Dhu al-Hijjah begin on September 15](https://web.archive.org/web/20250430191246/https://qna.org.qa/en/news/news-details?id=saudi-arabia-eid-aladha-to-start-on-september-24&date=14/09/2015)
    """

    country = "SA"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # %s (observed).
    observed_label = tr("%s (ملاحظة)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (المقدرة، ملاحظة)")
    supported_languages = ("ar", "en_US")
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = False, **kwargs):
        """Saudi Arabia has traditionally used the Umm al-Qura calendar
            for administrative purposes.

        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        IslamicHolidays.__init__(
            self, cls=SaudiArabiaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, SaudiArabiaStaticHolidays)
        kwargs.setdefault("observed_rule", FRI_TO_PREV_THU + SAT_TO_NEXT_SUN)
        super().__init__(*args, **kwargs)

    def _add_islamic_observed(self, dt: date) -> None:
        # Observed days are added to make up for any days falling on a weekend.
        if not self.observed:
            return None
        observed_rule = THU_FRI_TO_NEXT_WORKDAY if self._year <= 2012 else FRI_SAT_TO_NEXT_WORKDAY
        for i in range(4):
            self._add_observed(_timedelta(dt, -i), name=self[dt], rule=observed_rule)

    def _get_weekend(self, dt: date) -> set[int]:
        # Weekend used to be THU, FRI before June 28th, 2013.
        return {FRI, SAT} if dt >= date(2013, JUN, 28) else {THU, FRI}

    def _populate_public_holidays(self):
        self._observed_rule = (
            THU_TO_PREV_WED + FRI_TO_NEXT_SAT
            if self._year <= 2012
            else FRI_TO_PREV_THU + SAT_TO_NEXT_SUN
        )

        # Eid al-Fitr Holiday.
        eid_al_fitr_name = tr("عطلة عيد الفطر")
        eid_al_fitr_dates = self._add_eid_al_fitr_day(eid_al_fitr_name)
        self._add_eid_al_fitr_day_two(eid_al_fitr_name)
        self._add_eid_al_fitr_day_three(eid_al_fitr_name)

        for dt in eid_al_fitr_dates:
            if self._islamic_calendar._is_long_ramadan(dt):
                # Add 30 Ramadan.
                self._add_holiday(eid_al_fitr_name, _timedelta(dt, -1))
                self._add_islamic_observed(_timedelta(dt, +2))
            else:
                # Add 4 Shawwal.
                self._add_islamic_observed(self._add_holiday(eid_al_fitr_name, _timedelta(dt, +3)))

        # Arafat Day.
        self._add_arafah_day(tr("يوم عرفة"))

        # Eid al-Adha Holiday.
        name = tr("عطلة عيد الأضحى")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        for dt in self._add_eid_al_adha_day_three(name):
            self._add_islamic_observed(dt)

        if self._year >= 2022:
            # Founding Day.
            self._add_observed(self._add_holiday_feb_22(tr("يوم التأسيسي")))

        if self._year >= 2005:
            # National Day.
            self._add_observed(self._add_holiday_sep_23(tr("اليوم الوطني")))


class SA(SaudiArabia):
    pass


class SAU(SaudiArabia):
    pass


class SaudiArabiaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_ADHA_DATES = {
        2015: (SEP, 24),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2025)


class SaudiArabiaStaticHolidays:
    special_public_holidays = {
        # Celebrate the country's win against Argentina in the World Cup.
        2022: (NOV, 23, tr("يوم وطني")),
    }
