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
from holidays.calendars.gregorian import AUG, THU, FRI, SAT, _timedelta
from holidays.constants import PUBLIC, SCHOOL, WORKDAY
from holidays.groups import InternationalHolidays, IslamicHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, FRI_TO_NEXT_WORKDAY


class Yemen(ObservedHolidayBase, InternationalHolidays, IslamicHolidays):
    """Yemen holidays.

    References:
        * [Presidential Decree Law No. (8) of 1996](https://web.archive.org/web/20230905142654/http://yemen-nic.info/db/laws_ye/detail.php?ID=11476)
        * [Presidential Decree Law No. (2) of 2000](https://web.archive.org/web/20140714152707/http://www.presidentsaleh.gov.ye/showlaws.php?_lwbkno=3&_lwptno=2&_lwnmid=4)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Yemen>
        * <https://ar.wikipedia.org/wiki/قائمة_العطل_الرسمية_في_اليمن>
        * <https://web.archive.org/web/20160806072136/http://marebpress.net/news_details.php?lang=arabic&sid=55120>
        * <https://web.archive.org/web/20250505071425/https://yemenembassy.it/festivities/>
        * <https://web.archive.org/web/20250113111659/https://www.timeanddate.com/holidays/yemen/2025?hol=9>
        * <https://web.archive.org/web/20250103115023/http://www.yemenpost.net/Detail123456789.aspx?ID=3&SubID=7132&MainCat=3>
    """

    country = "YE"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # %s (observed).
    observed_label = tr("%s (ملاحظة)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (المقدرة، ملاحظة)")
    # The Republic of Yemen was declared on 22 May 1990.
    start_year = 1991
    supported_categories = (PUBLIC, SCHOOL, WORKDAY)
    supported_languages = ("ar", "en_US")
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
            self, cls=YemenIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_rule", FRI_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        # Yemen switches from THU-FRI to FRI-SAT on Aug 15, 2013.
        return {FRI, SAT} if dt >= date(2013, AUG, 15) else {THU, FRI}

    def _populate_public_holidays(self):
        dts_observed = set()

        # Hijri New Year.
        dts_observed.update(self._add_islamic_new_year_day(tr("عيد رأس السنة الهجرية")))

        if self._year <= 1999:
            # Prophet's Birthday.
            dts_observed.update(self._add_mawlid_day(tr("المولد النبوي")))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        dts_observed.update(self._add_eid_al_fitr_day(name))
        dts_observed.update(self._add_eid_al_fitr_day_two(name))
        dts_observed.update(self._add_eid_al_fitr_day_three(name))
        ramadan_29_holidays = self._add_holiday_29_ramadan(name)
        dts_observed.update(ramadan_29_holidays)
        for dt in ramadan_29_holidays:
            if _timedelta(dt, +1) not in self:
                dts_observed.update(self._add_eid_al_fitr_eve(name))

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        dts_observed.update(self._add_arafah_day(name))
        dts_observed.update(self._add_eid_al_adha_day(name))
        dts_observed.update(self._add_eid_al_adha_day_two(name))
        dts_observed.update(self._add_eid_al_adha_day_three(name))
        dts_observed.update(self._add_eid_al_adha_day_four(name))

        # Labor Day.
        dts_observed.add(self._add_labor_day(tr("عيد العمال")))

        # Unity Day.
        dts_observed.add(self._add_holiday_may_22(tr("اليوم الوطني للجمهورية اليمنية")))

        if self._year <= 1999:
            # Victory Day.
            dts_observed.add(self._add_holiday_jul_7(tr("ذكرى 7 يوليو")))

        # Revolution Day.
        dts_observed.add(self._add_holiday_sep_26(tr("ثورة 26 سبتمبر المجيدة")))

        # Liberation Day.
        dts_observed.add(self._add_holiday_oct_14(tr("ثورة 14 أكتوبر المجيدة")))

        # Independence Day.
        dts_observed.add(self._add_holiday_nov_30(tr("عيد الجلاء")))

        if self.observed:
            self._populate_observed(dts_observed)

    def _populate_school_holidays(self):
        if self._year >= 2013:
            # Teacher's Day.
            self._add_holiday_may_5(tr("عيد المعلم"))

    def _populate_workday_holidays(self):
        if self._year >= 2000:
            # Prophet's Birthday.
            self._add_mawlid_day(tr("المولد النبوي"))

            # Isra' and Mi'raj.
            self._add_isra_and_miraj_day(tr("ذكرى الإسراء والمعراج"))

            # Victory Day.
            self._add_holiday_jul_7(tr("ذكرى 7 يوليو"))


class YE(Yemen):
    pass


class YEM(Yemen):
    pass


class YemenIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250115070635/https://www.timeanddate.com/holidays/yemen/eid-al-adha-first-day

    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2020, 2024)

    # https://web.archive.org/web/20250218061345/https://www.timeanddate.com/holidays/yemen/eid-al-fitr-first-day
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2020, 2025)

    # https://web.archive.org/web/20241011200213/https://www.timeanddate.com/holidays/yemen/muharram-new-year
    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2020, 2024)
    HIJRI_NEW_YEAR_DATES = {
        2021: (AUG, 10),
    }

    # https://web.archive.org/web/20241010083000/https://www.timeanddate.com/holidays/yemen/isra-miraj
    ISRA_AND_MIRAJ_DATES_CONFIRMED_YEARS = (2023, 2025)

    # https://web.archive.org/web/20241010222331/https://www.timeanddate.com/holidays/yemen/prophet-birthday
    MAWLID_DATES_CONFIRMED_YEARS = (2020, 2024)

    # https://web.archive.org/web/20250119111122/https://www.timeanddate.com/holidays/yemen/ramadan-begins
    RAMADAN_BEGINNING_DATES_CONFIRMED_YEARS = (2023, 2025)
