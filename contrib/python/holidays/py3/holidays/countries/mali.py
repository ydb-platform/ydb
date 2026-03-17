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
from holidays.calendars.gregorian import JAN, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Mali(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Mali holidays.

    References:
        * [Ordinance no. 54 of 1960](https://web.archive.org/web/20250619193041/https://sgg-mali.ml/JO/1960/mali-jo-1960-71.pdf)
        * [Law No. 28 of 1964](https://web.archive.org/web/20250619190638/https://sgg-mali.ml/JO/1964/mali-jo-1964-183.pdf)
        * [Ordinance no. 16 of 1992](https://web.archive.org/web/20250611221453/https://sgg-mali.ml/JO/1992/mali-jo-1992-08.pdf)
        * [Law No. 40 of 2005](https://web.archive.org/web/20250603120027/https://sgg-mali.ml/JO/2005/mali-jo-2005-25.pdf)
    """

    country = "ML"
    default_language = "fr"
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    start_year = 1961
    supported_languages = ("en_US", "fr")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=MaliIslamicHolidays, show_estimated=islamic_show_estimated
        )
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Jour de l'An"))

        if self._year >= 1965:
            # Armed Forces Day.
            self._add_holiday_jan_20(tr("Journée de l'Armée"))

        if self._year >= 1992:
            # Martyrs' Day.
            self._add_holiday_mar_26(tr("Journée du 26 mars"))

        if self._year >= 2006:
            # Easter Monday.
            self._add_easter_monday(tr("Lundi de Pâques"))

        # Labor Day.
        self._add_labor_day(tr("Fête du Travail"))

        if self._year >= 1965:
            # Africa Day.
            self._add_africa_day(tr("Journée de l'Afrique"))

        # National Day of the Republic of Mali.
        self._add_holiday_sep_22(tr("Fête Nationale de la République du Mali"))

        # Christmas Day.
        self._add_christmas_day(tr("Fête de Noël"))

        self._add_mawlid_day(
            # Prophet's Birthday.
            tr("Journée du Maouloud (Naissance du Prophète)")
            if self._year >= 2006
            # Prophet's Birthday.
            else tr("Journée du Mawloud")
        )

        if self._year >= 2006:
            # Prophet's Baptism.
            self._add_prophet_baptism_day(tr("Journée du Maouloud (Baptême du Prophète)"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Journée de la Fête du Ramadan"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Journée de la Tabaski"))


class ML(Mali):
    pass


class MLI(Mali):
    pass


class MaliIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250619042440/https://www.timeanddate.com/holidays/mali/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_ADHA_DATES = {
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
        2021: (JUL, 21),
        2024: (JUN, 17),
        2025: (JUN, 7),
    }

    # https://web.archive.org/web/20250318001443/https://www.timeanddate.com/holidays/mali/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_FITR_DATES = {
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2019: (JUN, 3),
        2020: (MAY, 23),
        2021: (MAY, 12),
        2022: (MAY, 1),
        2024: (APR, 9),
    }

    # https://web.archive.org/web/20240424165412/https://www.timeanddate.com/holidays/mali/prophet-birthday
    MAWLID_DATES_CONFIRMED_YEARS = (2015, 2024)
    MAWLID_DATES = {
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2018: (NOV, 21),
        2019: (NOV, 10),
        2021: (OCT, 19),
        2022: (OCT, 9),
        2023: (SEP, 28),
        2024: (SEP, 16),
    }
