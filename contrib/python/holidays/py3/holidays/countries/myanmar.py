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
from holidays.calendars.gregorian import JAN, MAR, NOV, DEC
from holidays.groups import (
    BurmeseCalendarHolidays,
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.holiday_base import HolidayBase


class Myanmar(
    HolidayBase,
    BurmeseCalendarHolidays,
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Myanmar holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Myanmar>
        * <https://my.wikipedia.org/wiki/မြန်မာနိုင်ငံရှိ_အားလပ်ရက်များ>
        * [Algorithm, Program and Calculation of Myanmar Calendar](https://web.archive.org/web/20250510011425/http://cool-emerald.blogspot.com/2013/06/algorithm-program-and-calculation-of.html)
        * [2025 International and Chinese New Year](https://web.archive.org/web/20240709104600/https://www.moi.gov.mm/news/58594)
        * [Armed Forces Day](https://web.archive.org/web/20250522010622/https://burma.irrawaddy.com/on-this-day/2019/03/27/187691.html)
        * [2021-2024](https://web.archive.org/web/20241112125406/https://evisa.moip.gov.mm/home/publicholiday)
        * [2013-2025 (Webarchive)](https://web.archive.org/web/20250711015428/http://www.myanmarembassy.sg/contact-us/holidays/)
    """

    country = "MM"
    default_language = "my"
    # %s (estimated).
    estimated_label = tr("%s (ခန့်မှန်း)")
    supported_languages = ("en_US", "my", "th")
    # Myanmar gained its independence from British rule on January 4, 1948.
    start_year = 1948

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In Myanmar, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        BurmeseCalendarHolidays.__init__(self)
        ChineseCalendarHolidays.__init__(self)
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self,
            cls=MyanmarIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        StaticHolidays.__init__(self, cls=MyanmarStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        if self._year >= 2025:
            # New Year's Day.
            self._add_new_years_day(tr("နိုင်ငံတကာနှစ်သစ်ကူးနေ့"))

        # Independence Day.
        self._add_holiday_jan_4(tr("လွတ်လပ်ရေးနေ့"))

        # Union Day.
        self._add_holiday_feb_12(tr("ပြည်ထောင်စုနေ့"))

        # Peasants' Day.
        name = tr("တောင်သူလယ်သမားနေ့")
        if self._year >= 1965:
            self._add_holiday_mar_2(name)
        elif self._year >= 1963:
            self._add_holiday_jan_1(name)

        self._add_holiday_mar_27(
            # Armed Forces Day.
            tr("တပ်မတော်နေ့")
            if self._year >= 1955
            # Revolution Day.
            else tr("တော်လှန်ရေးနေ့")
        )

        # May Day.
        self._add_labor_day(tr("မေဒေးနေ့"))

        # Martyrs' Day.
        self._add_holiday_jul_19(tr("အာဇာနည်နေ့"))

        # Christmas Day.
        self._add_christmas_day(tr("ခရစ္စမတ်နေ့"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("အီဒုလ်အဿွဟာနေ့"))

        if self._year >= 2025:
            # Chinese New Year.
            self._add_chinese_new_years_day(tr("တရုတ်နှစ်သစ်ကူးနေ့"))

        # Myanmar New Year.
        name = tr("မြန်မာနှစ်သစ်ကူး ရုံးပိတ်ရက်များ")
        if self._year >= 2024:
            self._add_myanmar_new_year(name, extra_days_after=8)
        elif self._year >= 2022:
            self._add_myanmar_new_year(name, extra_days_before=4)
        elif 2007 <= self._year <= 2016:
            self._add_myanmar_new_year(name, extra_days_before=1, extra_days_after=8)
        else:
            self._add_myanmar_new_year(name)

        # Full Moon Day of Tabaung.
        self._add_tabaung_full_moon_day(tr("တပေါင်းလပြည့်နေ့"))

        # Full Moon Day of Kason.
        self._add_kason_full_moon_day(tr("ကဆုန်လပြည့်နေ့"))

        # Full Moon Day of Waso.
        self._add_waso_full_moon_day(tr("ဝါဆိုလပြည့်နေ့"))

        # Thadingyut Holidays.
        name = tr("သီတင်းကျွတ်ပိတ်ရက်များ")
        self._add_thadingyut_full_moon_eve(name)
        self._add_thadingyut_full_moon_day(name)
        self._add_thadingyut_full_moon_day_two(name)

        # Diwali.
        self._add_myanmar_diwali(tr("ဒီပါဝလီနေ့"))

        # Full Moon Day of Tazaungmon.
        self._add_tazaungmon_full_moon_day(tr("တန်ဆောင်တိုင်လပြည့်နေ့"))

        # National Day.
        self._add_myanmar_national_day(tr("အမျိုးသားနေ့"))

        # Karen New Year.
        self._add_karen_new_year(tr("ကရင်နှစ်သစ်ကူးနေ့"))


class MM(Myanmar):
    pass


class MMR(Myanmar):
    pass


class MyanmarIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2020, 2025)


class MyanmarStaticHolidays:
    """Myanmar special holidays.

    References:
        * [2025](https://web.archive.org/web/20241221214023/https://www.gnlm.com.mm/govt-designates-continuous-public-days-for-2024-2025-to-ease-travel-and-extend-leisure/)
    """

    # Substituted date format.
    substituted_date_format = tr("%d-%m-%Y")

    # Day off (substituted from %s).
    substituted_label = tr("အလုပ်ပိတ်ရက် (%s မှ ပြန်လဲထားသည်)")

    special_public_holidays = {
        2024: (DEC, 31, JAN, 11, 2025),
        2025: (
            (MAR, 12, MAR, 22),
            (MAR, 14, MAR, 29),
            (NOV, 3, NOV, 8),
            (DEC, 26, JAN, 3, 2026),
        ),
    }
