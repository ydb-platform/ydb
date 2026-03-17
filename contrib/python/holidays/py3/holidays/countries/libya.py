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
from holidays.calendars.gregorian import JAN, FEB, MAR, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import InternationalHolidays, IslamicHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Libya(HolidayBase, InternationalHolidays, IslamicHolidays, StaticHolidays):
    """Libya holidays.

    References:
        * [Law No. 4 of 1987](https://web.archive.org/web/20250629084625/https://lawsociety.ly/legislation/قانون-رقم-4-لسنة-1987-م-بشأن-العطلات-الرسمية/)
        * [Law No. 5 of 2012](https://web.archive.org/web/20250629084558/https://lawsociety.ly/legislation/القانون-رقم-5-لسنة-2012-م-بشأن-العطلات-الرسم/)
        * [National Environmental Sanitation Day](https://web.archive.org/web/20250629084547/https://lawsociety.ly/legislation/قرار-رقم-414-لسنة-2021-م-باعتبار-يوم-14-أغسطس-يو/)
    """

    country = "LY"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    start_year = 1988
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("ar", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=LibyaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=LibyaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        if self._year <= 2011:
            # People's Authority Day.
            self._add_holiday_mar_2(tr("عید إعلان سلطة الشعب"))

            # American Forces Evacuation Day.
            self._add_holiday_jun_11(tr("عيد إجلاء القوات الأمريكية"))

            # Glorious July Revolution Day.
            self._add_holiday_jul_23(tr("عيد ثورة يوليو المجيدة"))

            # Great Al-Fateh Revolution Day.
            self._add_holiday_sep_1(tr("عيد الفاتح العظيم"))
        else:
            # Anniversary of the February 17 Revolution.
            self._add_holiday_feb_17(tr("ثورة 17 فبراير"))

            # Labor Day.
            self._add_labor_day(tr("عيد العمال"))

            if self._year >= 2022:
                # National Environmental Sanitation Day.
                self._add_holiday_aug_14(tr("يوم وطني للإصحاح البيئي"))

            # Martyrs' Day.
            self._add_holiday_sep_16(tr("يوم الشهيد"))

            # Liberation Day.
            self._add_holiday_oct_23(tr("يوم التحرير"))

            # Independence Day.
            self._add_holiday_dec_24(tr("عيد الاستقلال"))

            # Islamic New Year.
            self._add_islamic_new_year_day(tr("عيد رأس السنة الهجرية"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("ذكرى المولد النبوي الشريف"))

        # Eid al-Fitr.
        name = tr("عيد الفطر")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Day of Arafah.
        self._add_arafah_day(tr("يوم عرفة"))

        # Eid al-Adha.
        name = tr("عيد الأضحى")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)

    def _populate_workday_holidays(self):
        if self._year <= 2011:
            # Syrian Revolution Day.
            self._add_holiday_mar_8(tr("عيد ثورة سوريا"))

            # Anniversary of the Arab League.
            self._add_holiday_mar_22(tr("ذكرى إنشاء الجامعة العربية"))

            # British Forces Evacuation Day.
            self._add_holiday_mar_28(tr("عيد إجلاء القوات البريطانية"))

            # Italian Forces Evacuation Day.
            self._add_holiday_oct_7(tr("عيد إجلاء الطليان"))

            # Islamic New Year.
            self._add_islamic_new_year_day(tr("عيد رأس السنة الهجرية"))

            # Ashura.
            self._add_ashura_day(tr("عاشوراء"))

            # Isra and Mi'raj.
            self._add_isra_and_miraj_day(tr("ذكرى الإسراء والمعراج"))

            # Night of Forgiveness.
            self._add_imam_mahdi_birthday_day(tr("ليلة النصف من شعبان"))


class LY(Libya):
    pass


class LBY(Libya):
    pass


class LibyaIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20240908234803/https://www.timeanddate.com/holidays/libya/eid-al-adha
    # https://web.archive.org/web/20250629084537/https://lawsociety.ly/legislation/قرار-رقم-773-لسنة-2017-م-بشأن-تحديد-عطلة-عيد-ال/
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2012, 2025)
    EID_AL_ADHA_DATES = {
        2014: (OCT, 5),
        2018: (AUG, 22),
    }

    # https://web.archive.org/web/20241012125707/https://www.timeanddate.com/holidays/libya/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2012, 2025)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2025: (MAR, 31),
    }

    # https://web.archive.org/web/20250418094505/https://www.timeanddate.com/holidays/libya/muharram-new-year
    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2012, 2025)
    HIJRI_NEW_YEAR_DATES = {
        2013: (NOV, 5),
        2015: (OCT, 15),
        2016: (OCT, 3),
        2017: (SEP, 22),
        2018: (SEP, 12),
        2021: (AUG, 10),
    }

    # https://web.archive.org/web/20241213175353/https://www.timeanddate.com/holidays/libya/prophet-birthday
    # https://web.archive.org/web/20250629084607/https://lawsociety.ly/legislation/قرار-رقم-1299-لسنة-2019-م-بشأن-عطلة-ذكرى-المولد/
    MAWLID_DATES_CONFIRMED_YEARS = (2012, 2024)
    MAWLID_DATES = {
        2012: (FEB, 5),
        2014: (JAN, 14),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2018: (NOV, 21),
        2021: (OCT, 19),
    }


class LibyaStaticHolidays:
    """Libya special holidays.

    References:
        * <https://web.archive.org/web/20250629084721/https://lawsociety.ly/legislation/قرار-رقم-555-لسنة-2023-م-بمنح-إجازة-رسمية/>
    """

    special_public_holidays = {
        # Public Holiday.
        2023: (DEC, 10, tr("عطلة رسمية"))
    }
