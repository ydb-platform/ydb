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
from holidays.calendars.gregorian import JUN, JUL, SEP
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class CentralAfricanRepublic(
    HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays
):
    """Central African Republic holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Central_African_Republic>
        * [PUBLIC HOLIDAYS](https://web.archive.org/web/20171215122602/http://www.ais-asecna.org/pdf/gen/gen-2-1/04gen2-1-01.pdf)
        * <https://web.archive.org/web/20250122135347/https://corbeaunews-centrafrique.org/lindependance-et-la-fete-nationale-deux-dates-distinctes-pour-la-republique-centrafricaine/>
        * <https://web.archive.org/web/20250715092147/https://www.rfi.fr/fr/afrique/20160912-rca-fete-tabaski-est-desormais-jour-ferie>
        * <https://web.archive.org/web/20250715093233/https://www.aa.com.tr/fr/culture-et-arts/lofficialisation-des-fêtes-musulmanes-en-rca-nouvelle-donne-pour-la-cohésion-sociale/45806>
    """

    country = "CF"
    default_language = "fr"
    supported_languages = ("en_US", "fr")
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    # December 1, 1958: Autonomy within the French Community.
    start_year = 1959

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
            self, cls=CentralAfricanRepublicIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Jour de l'an"))

        if self._year >= 1960:
            # Barthélemy Boganda Day.
            self._add_holiday_mar_29(tr("Journée Barthélemy Boganda"))

        # Easter Monday.
        self._add_easter_monday(tr("Lundi de Pâques"))

        # Labor Day.
        self._add_labor_day(tr("Fête du Travail"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension"))

        # Whit Monday.
        self._add_whit_monday(tr("Lundi de Pentecôte"))

        if self._year >= 2007:
            # General Prayer Day.
            self._add_holiday_jun_30(tr("Journée de prière générale"))

        if self._year >= 1960:
            # Independence Day.
            self._add_holiday_aug_13(tr("Jour de l'indépendance"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assomption"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Toussaint"))

        # National Day.
        name = tr("Fête nationale")
        if self._year in {1977, 1978}:
            self._add_holiday_dec_4(name)
        else:
            self._add_holiday_dec_1(name)

        # Christmas Day.
        self._add_christmas_day(tr("Jour de Noël"))

        if self._year >= 2015:
            # Eid al-Fitr.
            self._add_eid_al_fitr_day(tr("Aïd al-Fitr"))

            # Eid al-Adha.
            self._add_eid_al_adha_day(tr("Aïd al-Adha"))


class CF(CentralAfricanRepublic):
    pass


class CAF(CentralAfricanRepublic):
    pass


class CentralAfricanRepublicIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_ADHA_DATES = {
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_FITR_DATES = {
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
    }
