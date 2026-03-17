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
from holidays.calendars.gregorian import APR
from holidays.groups import InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Tajikistan(HolidayBase, InternationalHolidays, IslamicHolidays):
    """Tajikistan holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Tajikistan>
        * [Law #3 from Nov 3, 1995](https://web.archive.org/web/20250823162418/http://www.portali-huquqi.tj/publicadliya/view_qonunhoview.php?showdetail=&asosi_id=155&language=tj)
        * [Law #753 from Aug 2, 2011](https://web.archive.org/web/20250823162543/http://www.portali-huquqi.tj/publicadliya/view_qonunhoview.php?showdetail=&asosi_id=12997&language=tj)
        * [Independence Day](https://web.archive.org/web/20250324203223/https://tnu.tj/index.php/en/9-september-independence-day-of-the-republic-of-tajikistan/)
    """

    country = "TJ"
    default_language = "tg"
    # %s (estimated).
    estimated_label = tr("%s (таҳминан)")
    start_year = 1992
    supported_languages = ("en_US", "ru", "tg")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=TajikistanIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Соли Нав"))

        # Mother's Day.
        self._add_holiday_mar_8(tr("Рӯзи Модар"))

        # International Nowruz Day.
        name = tr("Иди байналмилалии Наврӯз")
        self._add_holiday_mar_21(name)
        self._add_holiday_mar_22(name)

        # Established by Law #13 from May 3, 2002.
        if self._year >= 2003:
            self._add_holiday_mar_23(name)

        # Established by Law #146 from Dec 28, 2005.
        if self._year >= 2006:
            self._add_holiday_mar_24(name)

        # Abolished by Law #1390 from Feb 24, 2017.
        if self._year <= 2016:
            # International Workers' Solidarity Day.
            self._add_labor_day(tr("Рӯзи байналхалқии якдилии меҳнаткашон"))

        self._add_world_war_two_victory_day(
            # Victory Day.
            tr("Рӯзи Ғалаба дар Ҷанги Бузурги Ватанӣ"),
            is_western=False,
        )

        # Established by Law #628 from May 22, 1998.
        if self._year >= 1998:
            # Day of National Unity.
            self._add_holiday_jun_27(tr("Рӯзи Ваҳдати миллӣ"))

        # Independence Day.
        self._add_holiday_sep_9(tr("Рӯзи Истиқлолияти давлатии Ҷумҳурии Тоҷикистон"))

        if self._year >= 1994:
            # Constitution Day.
            self._add_holiday_nov_6(tr("Рӯзи Конститутсияи Ҷумҳурии Тоҷикистон"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Рӯзи иди Рамазон"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Рӯзи иди Қурбон"))


class TJ(Tajikistan):
    pass


class TJK(Tajikistan):
    pass


class TajikistanIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20240911001624/https://www.timeanddate.com/holidays/tajikistan/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (1992, 2025)
    EID_AL_FITR_DATES = {
        2023: (APR, 22),
    }

    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (1992, 2025)
