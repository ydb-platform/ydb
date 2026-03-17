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

from holidays.calendars.balinese_saka import _BalineseSakaLunar
from holidays.calendars.buddhist import _BuddhistLunisolar, _CustomBuddhistHolidays
from holidays.calendars.burmese import _BurmeseLunisolar
from holidays.calendars.chinese import _ChineseLunisolar, _CustomChineseHolidays
from holidays.calendars.custom import _CustomCalendar
from holidays.calendars.gregorian import GREGORIAN_CALENDAR
from holidays.calendars.hebrew import _HebrewLunisolar
from holidays.calendars.hindu import _CustomHinduHolidays, _HinduLunisolar
from holidays.calendars.islamic import _CustomIslamicHolidays, _IslamicLunar
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.calendars.julian_revised import JULIAN_REVISED_CALENDAR
from holidays.calendars.mongolian import _CustomMongolianHolidays, _MongolianLunisolar
from holidays.calendars.persian import _Persian
from holidays.calendars.sinhala import _SinhalaLunar, _CustomSinhalaHolidays
from holidays.calendars.thai import _ThaiLunisolar, KHMER_CALENDAR, THAI_CALENDAR
from holidays.calendars.tibetan import _TibetanLunisolar, _CustomTibetanHolidays
