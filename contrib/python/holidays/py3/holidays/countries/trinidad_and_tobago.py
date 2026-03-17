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

from __future__ import annotations

from gettext import gettext as tr
from typing import TYPE_CHECKING

from holidays.calendars import _CustomHinduHolidays, _CustomIslamicHolidays
from holidays.calendars.gregorian import MAR, APR, JUN, JUL, OCT, NOV
from holidays.constants import OPTIONAL, PUBLIC
from holidays.groups import (
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SUN_TO_NEXT_WORKDAY,
    SAT_SUN_TO_NEXT_WORKDAY,
    WORKDAY_TO_NEXT_WORKDAY,
)

if TYPE_CHECKING:
    from datetime import date


class TrinidadAndTobago(
    ObservedHolidayBase,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
):
    """Trinidad and Tobago holidays.

    References:
        * <https://web.archive.org/web/20250429121623/https://otp.tt/trinidad-and-tobago/national-holidays-and-awards/>
        * <https://web.archive.org/web/20250429120744/https://laws.gov.tt/ttdll-web2/revision/list?offset=380&q=&currentid=322>
        * <https://web.archive.org/web/20250429120707/https://www.guardian.co.tt/article-6.2.448640.3e9e366923>
        * <https://web.archive.org/web/20250429120548/https://www.guardian.co.tt/article-6.2.410311.3fe66fb00f>
        * <https://web.archive.org/web/20250418212357/https://www.nalis.gov.tt/resources/tt-content-guide/labour-day/>
        * <https://web.archive.org/web/20250429120701/https://www.facebook.com/groups/191766699268/posts/10160832951274269/>
        * <https://web.archive.org/web/20250429142054/https://www.facebook.com/plugins/post.php?href=https://www.facebook.com/100064996051675/posts/1101755938667598/>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Trinidad_and_Tobago>
        * <https://en.wikipedia.org/wiki/Trinidad_and_Tobago_Carnival>
        * <https://en.wikipedia.org/wiki/Indian_Arrival_Day#Trinidad_and_Tobago>
        * <https://web.archive.org/web/20250429120402/https://www.timeanddate.com/holidays/trinidad/eid-al-fitr>
        * <https://web.archive.org/web/20250429120140/https://www.timeanddate.com/holidays/trinidad/diwali>
    """

    country = "TT"
    default_language = "en_TT"
    supported_categories = (OPTIONAL, PUBLIC)
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    # %s (observed).
    observed_label = tr("%s (observed)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observed, estimated)")
    supported_languages = ("en_TT", "en_US")
    # Trinidad and Tobago gained independence on August 31, 1962.
    start_year = 1963

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=TrinidadAndTobagoHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=TrinidadAndTobagoIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_observed(self, dts: set[date], *, multiple: bool = False) -> None:
        for dt in sorted(dts):
            self._add_observed(
                dt,
                rule=WORKDAY_TO_NEXT_WORKDAY + SAT_SUN_TO_NEXT_WORKDAY
                if len(self.get_list(dt)) > 1
                else SUN_TO_NEXT_WORKDAY,
            )

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("New Year's Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        if self._year >= 1996:
            # Spiritual Baptist Liberation Day.
            dts_observed.add(self._add_holiday_mar_30(tr("Spiritual Baptist Liberation Day")))

            # Indian Arrival Day.
            dts_observed.add(self._add_holiday_may_30(tr("Indian Arrival Day")))

        # Corpus Christi.
        dts_observed.add(self._add_corpus_christi_day(tr("Corpus Christi")))

        if self._year >= 1973:
            # Labor Day.
            dts_observed.add(self._add_holiday_jun_19(tr("Labour Day")))

        if self._year >= 1985:
            # African Emancipation Day.
            dts_observed.add(self._add_holiday_aug_1(tr("African Emancipation Day")))

        # Independence Day.
        dts_observed.add(self._add_holiday_aug_31(tr("Independence Day")))

        if self._year >= 1976:
            # Republic Day.
            dts_observed.add(self._add_holiday_sep_24(tr("Republic Day")))

        # Diwali.
        dts_observed.add(self._add_diwali(tr("Divali")))

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Christmas Day")))

        # Boxing Day.
        dts_observed.add(self._add_christmas_day_two(tr("Boxing Day")))

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Eid-Ul-Fitr")))

        if self.observed:
            self._populate_observed(dts_observed)

    def _populate_optional_holidays(self):
        # Carnival Monday.
        self._add_carnival_monday(tr("Carnival Monday"))

        # Carnival Tuesday.
        self._add_carnival_tuesday(tr("Carnival Tuesday"))


class TT(TrinidadAndTobago):
    pass


class TTO(TrinidadAndTobago):
    pass


class TrinidadAndTobagoHinduHolidays(_CustomHinduHolidays):
    DIWALI_DATES = {
        2012: (NOV, 13),
        2013: (NOV, 4),
        2014: (OCT, 23),
        2015: (NOV, 11),
        2016: (OCT, 29),
        2017: (OCT, 19),
        2018: (NOV, 7),
        2019: (OCT, 27),
        2020: (NOV, 14),
        2021: (NOV, 4),
        2022: (OCT, 24),
        2023: (NOV, 12),
        2024: (OCT, 31),
        2025: (OCT, 20),
    }


class TrinidadAndTobagoIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2012, 2025)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2017: (JUN, 26),
        2019: (JUN, 5),
        2023: (APR, 22),
        2025: (MAR, 31),
    }
