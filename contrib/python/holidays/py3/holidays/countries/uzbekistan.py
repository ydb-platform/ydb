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
from holidays.calendars.gregorian import JAN, MAR, APR, MAY, JUN, JUL, AUG, SEP, DEC
from holidays.groups import InternationalHolidays, IslamicHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_WORKDAY


class Uzbekistan(ObservedHolidayBase, InternationalHolidays, IslamicHolidays, StaticHolidays):
    """Uzbekistan holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Uzbekistan>
        * [Labor Code 21.12.1995](https://web.archive.org/web/20250422170827/https://lex.uz/docs/-142859)
        * [Labor Code 28.10.2022](https://web.archive.org/web/20250424164306/https://lex.uz/docs/-6257288)
    """

    country = "UZ"
    default_language = "uz"
    # %s (estimated).
    estimated_label = tr("%s (taxminiy)")
    # %s (observed).
    observed_label = tr("%s (ko‘chirilgan)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (ko‘chirilgan, taxminiy)")
    supported_languages = ("en_US", "uk", "uz")
    start_year = 1992

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=UzbekistanIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, UzbekistanStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _is_observed(self, dt: date) -> bool:
        # Commencement of the new Labor Code.
        return dt >= date(2023, APR, 30)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("Yangi yil")))

        # Women's Day.
        dts_observed.add(self._add_womens_day(tr("Xotin-qizlar kuni")))

        # Nowruz.
        dts_observed.add(self._add_holiday_mar_21(tr("Navro‘z bayrami")))

        dts_observed.add(
            self._add_world_war_two_victory_day(
                # Day of Memory and Honor.
                tr("Xotira va qadrlash kuni")
                if self._year >= 1999
                # Victory Day.
                else tr("G‘alaba kuni"),
                is_western=False,
            )
        )

        # Independence Day.
        dts_observed.add(self._add_holiday_sep_1(tr("Mustaqillik kuni")))

        if self._year >= 1997:
            # Teachers and Instructors Day.
            dts_observed.add(self._add_holiday_oct_1(tr("O‘qituvchi va murabbiylar kuni")))

        if self._year >= 1993:
            dts_observed.add(
                # Constitution Day.
                self._add_holiday_dec_8(tr("O‘zbekiston Respublikasi Konstitutsiyasi kuni"))
            )

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Ro‘za hayit")))

        # Eid al-Adha.
        dts_observed.update(self._add_eid_al_adha_day(tr("Qurbon hayit")))

        if self.observed and self._year >= 2023:
            self._populate_observed(dts_observed)


class UZ(Uzbekistan):
    pass


class UZB(Uzbekistan):
    pass


class UzbekistanIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2006, 2025)
    EID_AL_ADHA_DATES = {
        2006: ((JAN, 10), (DEC, 30)),
        2007: (DEC, 19),
        2015: (SEP, 24),
        2016: (SEP, 12),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2006, 2025)
    EID_AL_FITR_DATES = {
        2009: (SEP, 21),
        2011: (AUG, 31),
        2013: (AUG, 9),
        2015: (JUL, 18),
        2017: (JUN, 26),
        2019: (JUN, 5),
    }


class UzbekistanStaticHolidays:
    """Uzbekistan special holidays.

    References:
        * [2018](https://web.archive.org/web/20250610012705/https://lex.uz/ru/docs/3479237)
        * [2019](https://web.archive.org/web/20250521124231/https://lex.uz/docs/-4054698)
        * [2020](https://web.archive.org/web/20250522053834/https://lex.uz/docs/-4595638)
        * [2021](https://web.archive.org/web/20250522053822/https://lex.uz/docs/-5137156)
        * [2022](https://web.archive.org/web/20250522053831/https://lex.uz/docs/-5784917)
        * [2023](https://web.archive.org/web/20250522053822/https://lex.uz/docs/-6324664)
        * [2024](https://web.archive.org/web/20250522053835/https://lex.uz/docs/-6704048)
        * [2025](https://web.archive.org/web/20250422135040/https://lex.uz/docs/-7275687)
        * [2025-2026](https://web.archive.org/web/20251227000438/https://lex.uz/docs/-7938920)
    """

    # Date format (see strftime() Format Codes)
    substituted_date_format = tr("%d/%m %Y")
    # Day off (substituted from %s).
    substituted_label = tr("Dam olish kuni (%s dan ko‘chirilgan)")

    # Additional day off by Presidential decree.
    additional_day_off = tr("Prezidentining farmoni bilan qo‘shimcha dam olish kuni")
    special_public_holidays = {
        2018: (
            (JAN, 2, additional_day_off),
            (JAN, 3, JAN, 6),
            (MAR, 19, MAR, 17),
            (MAR, 22, MAR, 24),
            (MAR, 20, additional_day_off),
            (AUG, 23, AUG, 25),
            (AUG, 24, AUG, 26),
            (AUG, 31, additional_day_off),
            (SEP, 3, SEP, 8),
            (SEP, 4, SEP, 15),
            (DEC, 31, DEC, 29),
        ),
        2019: (
            (JAN, 2, additional_day_off),
            (JAN, 3, JAN, 5),
            (MAR, 22, additional_day_off),
            (JUN, 6, JUN, 1),
            (SEP, 2, additional_day_off),
            (SEP, 3, SEP, 7),
            (DEC, 31, DEC, 28),
        ),
        2020: (
            (JAN, 2, JAN, 4),
            (MAR, 23, additional_day_off),
            (AUG, 31, AUG, 29),
        ),
        2021: (
            (MAR, 22, MAR, 27),
            (MAY, 14, additional_day_off),
            (JUL, 21, JUL, 17),
            (JUL, 22, JUL, 24),
            (SEP, 2, additional_day_off),
            (SEP, 3, additional_day_off),
            (DEC, 31, additional_day_off),
        ),
        2022: (
            (JAN, 3, additional_day_off),
            (JAN, 4, JAN, 8),
            (MAR, 22, additional_day_off),
            (MAR, 23, additional_day_off),
            (MAY, 3, additional_day_off),
            (MAY, 4, MAY, 7),
            (JUL, 11, additional_day_off),
            (JUL, 12, JUL, 16),
            (SEP, 2, additional_day_off),
        ),
        2023: (
            (JAN, 2, additional_day_off),
            (JAN, 3, JAN, 7),
            (MAR, 20, MAR, 11),
            (MAR, 22, MAR, 25),
            (APR, 24, additional_day_off),
            (JUN, 29, additional_day_off),
            (JUN, 30, additional_day_off),
        ),
        2024: (
            (JAN, 2, JAN, 6),
            (MAR, 22, additional_day_off),
            (APR, 11, additional_day_off),
            (APR, 12, APR, 13),
            (JUN, 18, additional_day_off),
            (SEP, 3, additional_day_off),
            (DEC, 30, DEC, 14),
            (DEC, 31, additional_day_off),
        ),
        2025: (
            (JAN, 2, JAN, 4),
            (DEC, 31, additional_day_off),
        ),
        2026: (
            (JAN, 2, additional_day_off),
            (MAY, 28, additional_day_off),
            (MAY, 29, additional_day_off),
            (AUG, 31, additional_day_off),
            (DEC, 31, DEC, 12),
        ),
    }
