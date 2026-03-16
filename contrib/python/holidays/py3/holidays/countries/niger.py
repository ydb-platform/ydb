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

from holidays import OPTIONAL, PUBLIC
from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import (
    JAN,
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
    SUN,
)
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Niger(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Niger holidays.

    References:
        * [Law No. 59-22 of December 24, 1959](https://web.archive.org/web/20241106023958/https://www.impots.gouv.ne/media/loi/1960.pdf)
        * <https://web.archive.org/web/20110721063839/http://www.ais-asecna.org/pdf/gen/gen-2-1/12gen2-1-01.pdf>
        * <https://web.archive.org/web/20250531032502/https://wageindicator.org/documents/decentworkcheck/africa/niger-french-2021.pdf>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Niger>
        * <https://web.archive.org/web/20250531032604/https://wageindicator.org/fr-ne/droit-du-travail/les-conges-et-les-jours-de-repos>
        * <https://web.archive.org/web/20250206194155/https://www.rivermate.com/guides/niger/leave>
        * [Eid al-Adha](https://web.archive.org/web/20250117013558/https://www.timeanddate.com/holidays/niger/eid-al-adha)
        * [Eid al-Fitr](https://web.archive.org/web/20250114130118/https://www.timeanddate.com/holidays/niger/eid-al-fitr)
        * [Laylat al-Qadr](https://web.archive.org/web/20250531033137/https://www.timeanddate.com/holidays/niger/laylat-al-qadr)
        * [Islamic New Year](https://web.archive.org/web/20240723135601/https://www.timeanddate.com/holidays/niger/muharram-new-year)
        * [Prophet's Birthday](https://web.archive.org/web/20250124122731/https://www.timeanddate.com/holidays/niger/prophet-birthday)

    Notes:
        After Law No. 97-020 of June 20, 1997 establishing public holidays came into
        effect, holidays that fell on the mandatory weekly rest day (Sunday) were
        observed on the next Monday.
    """

    country = "NE"
    default_language = "fr_NE"
    supported_languages = ("en_US", "fr_NE")
    supported_categories = (OPTIONAL, PUBLIC)
    # %s (observed).
    observed_label = tr("%s (observé)")
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observé, estimé)")
    # Law No. 59-22.
    start_year = 1960
    weekend = {SUN}

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
            self, cls=NigerIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 1998)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("Jour de l'An")))

        # Easter Monday.
        self._add_easter_monday(tr("Lundi de Pâques"))

        if self._year >= 1995:
            # National Concord Day.
            self._add_observed(self._add_holiday_apr_24(tr("Fête nationale de la Concorde")))

        # International Labor Day.
        self._add_observed(self._add_labor_day(tr("Journée internationale du travail")))

        if self._year >= 2024:
            # Anniversary of the CNSP Coup.
            self._add_observed(self._add_holiday_jul_26(tr("Anniversaire du coup d'État du CNSP")))

        self._add_observed(
            self._add_holiday_aug_3(
                # Anniversary of the Proclamation of Independence.
                tr("L'anniversaire de la proclamation de l'indépendance")
                if self._year >= 1961
                # Independence Day.
                else tr("Jour de l'indépendance")
            )
        )

        # National Day.
        self._add_observed(self._add_holiday_dec_18(tr("Fête nationale")))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Noël")))

        # Islamic New Year.
        for dt in self._add_islamic_new_year_day(tr("Jour de l'An musulman")):
            self._add_observed(dt)

        # Prophet's Birthday.
        for dt in self._add_mawlid_day(tr("Mouloud")):
            self._add_observed(dt)

        # Laylat al-Qadr.
        for dt in self._add_laylat_al_qadr_day(tr("Laylat al-Qadr")):
            self._add_observed(dt)

        # Eid al-Fitr.
        for dt in self._add_eid_al_fitr_day(tr("Korité")):
            self._add_observed(dt)

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Tabaski"))

        # Day after Eid al-Adha.
        for dt in self._add_eid_al_adha_day_two(tr("Lendemain de la Tabaski")):
            self._add_observed(dt)

    def _populate_optional_holidays(self):
        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension"))

        # Whit Monday.
        self._add_whit_monday(tr("Lundi de Pentecôte"))

        # Assumption Day.
        self._add_observed(self._add_assumption_of_mary_day(tr("Assomption")))

        # All Saints' Day.
        self._add_observed(self._add_all_saints_day(tr("Toussaint")))


class NE(Niger):
    pass


class NER(Niger):
    pass


class NigerIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (1998, 2025)
    EID_AL_ADHA_DATES = {
        1998: (APR, 8),
        1999: (MAR, 28),
        2001: (MAR, 6),
        2002: (FEB, 23),
        2003: (FEB, 12),
        2004: (FEB, 2),
        2008: (DEC, 9),
        2009: (NOV, 28),
        2010: (NOV, 17),
        2011: (NOV, 7),
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
        2022: (JUL, 10),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (1998, 2025)
    EID_AL_FITR_DATES = {
        1998: (JAN, 30),
        1999: (JAN, 19),
        2000: ((JAN, 8), (DEC, 28)),
        2001: (DEC, 17),
        2002: (DEC, 6),
        2003: (NOV, 26),
        2005: (NOV, 4),
        2006: (OCT, 24),
        2008: (OCT, 2),
        2009: (SEP, 21),
        2011: (AUG, 31),
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2020: (MAY, 23),
        2021: (MAY, 12),
        2022: (MAY, 1),
        2024: (APR, 9),
    }

    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (1998, 2025)
    HIJRI_NEW_YEAR_DATES = {
        1998: (APR, 28),
        2003: (MAR, 5),
        2004: (FEB, 22),
        2010: (DEC, 8),
        2011: (NOV, 27),
        2013: (NOV, 5),
        2015: (OCT, 15),
        2016: (OCT, 3),
        2017: (SEP, 22),
        2018: (SEP, 12),
        2020: (AUG, 21),
        2021: (AUG, 10),
        2024: (JUL, 6),
        2025: (JUN, 27),
    }

    LAYLAT_AL_QADR_DATES_CONFIRMED_YEARS = (1998, 2025)
    LAYLAT_AL_QADR_DATES = {
        1998: (JAN, 26),
        1999: (JAN, 15),
        2000: ((JAN, 4), (DEC, 24)),
        2001: (DEC, 13),
        2003: (NOV, 22),
        2004: (NOV, 11),
        2005: (OCT, 31),
        2008: (SEP, 28),
        2014: (JUL, 25),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (1998, 2025)
    MAWLID_DATES = {
        1998: (JUL, 7),
        2000: (JUN, 15),
        2003: (MAY, 14),
        2004: (MAY, 2),
        2006: (APR, 11),
        2011: (FEB, 16),
        2012: (FEB, 5),
        2014: (JAN, 14),
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2018: (NOV, 21),
        2019: (NOV, 10),
        2024: (SEP, 16),
        2025: (SEP, 5),
    }
