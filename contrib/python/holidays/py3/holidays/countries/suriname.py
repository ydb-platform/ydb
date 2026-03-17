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

from holidays import HolidayBase
from holidays.calendars import _CustomHinduHolidays, _CustomIslamicHolidays
from holidays.calendars.gregorian import MAR, APR, JUN, JUL, AUG, SEP, OCT, NOV
from holidays.groups import (
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
)


class Suriname(
    HolidayBase,
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
):
    """Suriname holidays.

    References:
        * [Besluit Vrije Dagen 1971](https://web.archive.org/web/20250411201210/https://www.sris.sr/wp-content/uploads/2022/10/Besluit-Vrije-dagen-GB-1971-no-78.pdf)
        * https://web.archive.org/web/20240416223555/https://gov.sr/wp-content/uploads/2022/09/arbeidswet.pdf
        * [S.B. 2007 no. 98](https://web.archive.org/web/20250411204836/https://www.sris.sr/wp-content/uploads/2021/10/SB-2007-no-98-besluit-vrije-dagen.pdf)
        * [S.B. 2012 no. 21](https://web.archive.org/web/20250411204754/https://www.sris.sr/wp-content/uploads/2021/10/SB-2012-no-21-besluit-vrije-dagen.pdf)
        * [S.B. 2021 no. 27](https://web.archive.org/web/20250411210516/https://www.sris.sr/wp-content/uploads/2021/03/S.B.-2021-no.-27-houdende-nadere-wijziging-van-het-Besluit-Vrije-Dagen-1971.pdf)
        * [S.B. 2024 no. 167](https://web.archive.org/web/20250411211129/https://www.sris.sr/wp-content/uploads/2025/01/S.B.-2024-no.-167-Algemene-termijnenwet.pdf)

    Note:
        The oldest decree available online that underpins the public holidays defined here
        for Suriname is Besluit Vrije Dagen 1971 of April 22, 1971.

        The S.B. 2024 no. 167 law only applies to prolongations of terms specified in contracts
        and other similar legal documents, and does not apply to labor agreements (and days off).
    """

    country = "SR"
    default_language = "nl"
    # %s (estimated).
    estimated_label = tr("%s (geschat)")
    start_year = 1972
    supported_languages = ("en_US", "nl")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChineseCalendarHolidays.__init__(self)
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=SurinameHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=SurinameIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nieuwjaarsdag"))

        if 1981 <= self._year <= 1992 or 2012 <= self._year <= 2020:
            # Day of Liberation and Renewal.
            self._add_holiday_feb_25(tr("Dag van Bevrijding en Vernieuwing"))

        # Good Friday.
        self._add_good_friday(tr("Goede Vrijdag"))

        # Easter Monday.
        self._add_easter_monday(tr("Tweede Paasdag"))

        if self._year <= 1975:
            # Birthday of H.M. the Queen.
            self._add_holiday_apr_30(tr("Verjaardag van H.M. de Koningin"))

        # Labor Day.
        self._add_labor_day(tr("Dag van de Arbeid"))

        self._add_holiday_jul_1(
            # Day of Freedoms.
            tr("Keti Koti Dey") if 2008 <= self._year <= 2024 else tr("Dag der Vrijheden")
        )

        if self._year >= 2007:
            # Indigenous People Day.
            self._add_holiday_aug_9(tr("Dag der Inheemsen"))

        if self._year >= 2012:
            # Day of the Maroons.
            self._add_holiday_oct_10(tr("Dag der Marrons"))

        if self._year >= 1976:
            self._add_holiday_nov_25(
                # Independence Day.
                tr("Onafhankelijkheidsdag")
                if self._year >= 2008
                # Republic Day.
                else tr("Dag van de Republiek")
            )

        # Christmas Day.
        self._add_christmas_day(tr("Eerste Kerstdag"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Tweede Kerstdag"))

        # Holi.
        self._add_holi(tr("Holi-Phagwa"))

        if self._year >= 2012:
            # Diwali.
            self._add_diwali(tr("Divali"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Ied-Ul-Fitre"))

        if self._year >= 2012:
            # Eid al-Adha.
            self._add_eid_al_adha_day(tr("Ied-Ul-Adha"))

        if self._year >= 2022:
            # Chinese New Year.
            self._add_chinese_new_years_day(tr("Chinees Nieuwjaar"))


class SR(Suriname):
    pass


class SUR(Suriname):
    pass


class SurinameHinduHolidays(_CustomHinduHolidays):
    # https://web.archive.org/web/20241104221047/https://www.timeanddate.com/holidays/suriname/holi-phagwa
    HOLI_DATES = {
        2015: (MAR, 6),
        2016: (MAR, 23),
        2017: (MAR, 12),
        2018: (MAR, 2),
        2019: (MAR, 21),
        2020: (MAR, 9),
        2021: (MAR, 28),
        2022: (MAR, 18),
        2023: (MAR, 7),
        2024: (MAR, 25),
        2025: (MAR, 14),
        2026: (MAR, 3),
        2027: (MAR, 22),
        2028: (MAR, 11),
        2029: (MAR, 28),
        2030: (MAR, 19),
    }

    # https://web.archive.org/web/20241105043434/https://www.timeanddate.com/holidays/suriname/diwali
    DIWALI_DATES = {
        2014: (OCT, 23),
        2015: (NOV, 11),
        2016: (OCT, 30),
        2017: (OCT, 19),
        2018: (NOV, 7),
        2019: (OCT, 27),
        2020: (NOV, 14),
        2021: (NOV, 4),
        2022: (OCT, 24),
        2023: (NOV, 12),
        2024: (OCT, 31),
        2025: (OCT, 20),
        2026: (NOV, 8),
        2027: (OCT, 28),
        2028: (OCT, 17),
        2029: (NOV, 5),
        2030: (OCT, 25),
    }


class SurinameIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20241107062349/https://www.timeanddate.com/holidays/suriname/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2014, 2025)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2019: (JUN, 5),
        2023: (APR, 22),
        2025: (MAR, 31),
    }

    # https://web.archive.org/web/20241113121535/https://www.timeanddate.com/holidays/suriname/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2014, 2025)
    EID_AL_ADHA_DATES = {
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2019: (AUG, 12),
        2023: (JUN, 29),
        2025: (JUN, 7),
    }
