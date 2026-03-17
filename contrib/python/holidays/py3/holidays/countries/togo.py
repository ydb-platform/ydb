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
from holidays.calendars.gregorian import MAY, JUN, JUL, AUG, SEP, OCT, NOV
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Togo(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Togo holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Togo>
        * <https://web.archive.org/web/20250507123116/https://www.timeanddate.com/holidays/togo/>
        * <https://web.archive.org/web/20250507123425/https://www.goethe.de/ins/tg/fr/ueb/fer.html>
        * <https://web.archive.org/web/20250507123619/https://www.republiquetogolaise.com/politique/2309-3579-23-septembre-le-togo-rend-hommage-a-ses-martyrs>
        * <https://web.archive.org/web/20250507123840/https://islam.zmo.de/s/westafrica/item/25841#?xywh=-405,-94,2376,1868>
        * <https://en.wikipedia.org/wiki/1986_Togolese_coup_attempt>
        * <https://web.archive.org/web/20250507131725/https://www.rfi.fr/fr/afrique/20140112-togo-le-13-janvier-est-plus-jour-fete>
        * <https://web.archive.org/web/20250507124528/https://islam.zmo.de/s/afrique_ouest/item/25800#?xywh=-1641,0,4303,2363>
        * <https://web.archive.org/web/20231202052731/https://www.holidayscalendar.com/event/anniversary-of-the-failed-attack-on-lome/>

    Ramadan start dates:
        * [2015](https://web.archive.org/web/20150825071241/https://www.republicoftogo.com/toutes-les-rubriques/societe/le-mois-du-jeune-debute-le-18-juin)
        * [2016](https://web.archive.org/web/20250507125256/https://www.republicoftogo.com/toutes-les-rubriques/societe/le-ramadan-debute-le-6-juin)
        * [2017](https://web.archive.org/web/20250507125353/https://www.tf1info.fr/societe/le-ramadan-2017-1438-commence-le-samedi-27-mai-comment-la-date-du-debut-du-jeune-est-elle-fixee-1512235.html)
        * [2018](https://web.archive.org/web/20250507125622/https://www.republicoftogo.com/toutes-les-rubriques/societe/debut-du-ramadan-demain)
        * [2019](https://web.archive.org/web/20250507130404/https://www.republiquetogolaise.com/social/0605-3100-la-communaute-musulmane-du-togo-entame-ce-lundi-le-jeune-du-mois-de-ramadan)
        * [2020](https://web.archive.org/web/20250507125837/https://www.republiquetogolaise.com/culture/2404-4278-debut-du-mois-de-ramadan)
        * [2021](https://web.archive.org/web/20250507130643/https://www.republiquetogolaise.com/culture/1304-5393-debut-du-mois-de-ramadan)
        * [2022](https://web.archive.org/web/20250507130847/https://www.republiquetogolaise.com/culture/0304-6745-debut-du-mois-de-ramadan)
        * [2023](https://web.archive.org/web/20250507131022/https://www.republiquetogolaise.com/culture/2303-7864-debut-du-mois-de-ramadan)
        * [2024](https://web.archive.org/web/20250507131215/https://www.republiquetogolaise.com/culture/1103-9017-debut-du-mois-de-ramadan)
        * [2025](https://web.archive.org/web/20250507131439/https://www.republiquetogolaise.com/culture/2802-10360-le-jeune-de-ramadan-debute-le-1er-mars)
    """

    country = "TG"
    default_language = "fr"
    supported_categories = (PUBLIC, WORKDAY)
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    supported_languages = ("en_US", "fr")
    # Togo gained independence on April 27, 1960.
    start_year = 1961

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=TogoIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Jour de l'an"))

        if 1967 <= self._year <= 2013:
            # Liberation Day.
            self._add_holiday_jan_13(tr("Fête de la libération nationale"))

        # Easter Monday.
        self._add_easter_monday(tr("Lundi de Pâques"))

        # Independence Day.
        self._add_holiday_apr_27(tr("Fête de l'indépendance"))

        # Labor Day.
        self._add_holiday_may_1(tr("Fête du travail"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Fête de l'Ascension"))

        # Whit Monday.
        self._add_whit_monday(tr("Lundi de Pentecôte"))

        # Martyrs' Day.
        self._add_holiday_jun_21(tr("Fête des Martyrs"))

        # Assumption Day.
        self._add_holiday_aug_15(tr("Assomption"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Toussaint"))

        # Christmas Day.
        self._add_christmas_day(tr("Noël"))

        # First Day of Ramadan.
        self._add_ramadan_beginning_day(tr("Ramadan"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("l'Aïd El-Fitr"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Tabaski"))

    def _populate_workday_holidays(self):
        if self._year >= 1987:
            # Anniversary of the Failed Attack on Lomé.
            self._add_holiday_sep_24(tr("Anniversaire de l'attentat manqué contre Lomé"))


class TG(Togo):
    pass


class TGO(Togo):
    pass


class TogoIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2010, 2025)
    EID_AL_ADHA_DATES = {
        2010: (NOV, 17),
        2011: (NOV, 7),
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2010, 2025)
    EID_AL_FITR_DATES = {
        2011: (AUG, 31),
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2019: (JUN, 5),
    }

    RAMADAN_BEGINNING_DATES_CONFIRMED_YEARS = (2015, 2025)
    RAMADAN_BEGINNING_DATES = {
        2018: (MAY, 17),
    }
