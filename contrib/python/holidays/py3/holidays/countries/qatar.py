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
from holidays.calendars.gregorian import JAN, JUL, AUG, SEP, OCT, NOV, DEC, THU, FRI, SAT
from holidays.constants import BANK, PUBLIC
from holidays.groups import InternationalHolidays, IslamicHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Qatar(HolidayBase, InternationalHolidays, IslamicHolidays, StaticHolidays):
    """Qatar holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Qatar>
        * [National Sports Day](https://web.archive.org/web/20250417141518/https://hukoomi.gov.qa/en/national-sport-day)
        * [Qatar National Day](https://web.archive.org/web/20240522081644/https://www.qatar.qa/en/qatar/history-of-qatar-qatar-national-day-committee/)
        * [Weekend](https://web.archive.org/web/20240930093123/https://www.arabnews.com/node/234601)
    """

    country = "QA"
    default_language = "ar_QA"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    start_year = 1971
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("ar_QA", "en_US")
    weekend = {FRI, SAT}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=QatarIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, QatarStaticHolidays)
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # Qatar switches from THU-FRI to FRI-SAT on Aug 1, 2003.
        return {FRI, SAT} if dt >= date(2003, AUG, 1) else {THU, FRI}

    def _populate_public_holidays(self):
        if self._year >= 2012:
            # National Sports Day.
            self._add_holiday_2nd_tue_of_feb(tr("اليوم الوطني للرياضة"))

        if self._year >= 2007:
            # Qatar National Day.
            self._add_holiday_dec_18(tr("اليوم الوطني لقطر"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)

    def _populate_bank_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("رأس السنة الميلادية"))

        if self._year >= 2010:
            # March Bank Holiday.
            self._add_holiday_1st_sun_of_mar(tr("عطلة البنك"))


class QA(Qatar):
    pass


class QAT(Qatar):
    pass


class QatarIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20260107122007/https://www.timeanddate.com/holidays/qatar/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2005, 2025)
    EID_AL_ADHA_DATES = {
        2008: (DEC, 9),
        2009: (NOV, 28),
        2010: (NOV, 15),
        2016: (SEP, 10),
        2017: (AUG, 31),
        2018: (AUG, 22),
    }

    # https://web.archive.org/web/20241207022523/https://www.timeanddate.com/holidays/qatar/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2005, 2025)
    EID_AL_FITR_DATES = {
        2005: (NOV, 4),
        2006: (OCT, 24),
        2008: (OCT, 2),
        2009: (SEP, 21),
        2011: (AUG, 31),
        2015: (JUL, 18),
    }


class QatarStaticHolidays:
    """Qatar special holidays.

    References:
        * [New Year's Holiday](https://web.archive.org/web/20250119073018/https://www.expatica.com/qa/lifestyle/holidays/qatar-public-holidays-74585/)
    """

    special_public_holidays = {
        # New Year's Holiday.
        2025: (JAN, 2, tr("عطلة رأس السنة")),
    }
