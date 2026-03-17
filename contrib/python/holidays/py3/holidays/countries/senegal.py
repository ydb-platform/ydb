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
from holidays.calendars.gregorian import APR, MAY, JUN, JUL, AUG, SEP, OCT, DEC
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Senegal(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Senegal holidays.
    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Senegal>
        * [Law 63-51](https://web.archive.org/web/20250604134724/https://www.dri.gouv.sn/sites/default/files/LOI/1963/63_51.pdf)
        * [Law 74-52](https://web.archive.org/web/20250608105243/https://www.dri.gouv.sn/sites/default/files/an-documents/LOI%20N1974%2052%20DU%204%20NOVEMBRE%201974.pdf)
        * [Law 83-54](https://web.archive.org/web/20250608105419/https://www.dri.gouv.sn/sites/default/files/LOI/1983/comP4%20loi%20decentralisation%20et%20travail/LOI%20N%20198354%20DU%2018%20FEVRIER%201983/LOI%20N%20198354%20DU%2018%20FEVRIER%201983.pdf)
        * [Law 2013-06](https://web.archive.org/web/20250604215148/https://natlex.ilo.org/dyn/natlex2/natlex2/files/download/97261/SEN-97261.pdf)
    """

    country = "SN"
    default_language = "fr_SN"
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    # %s (observed).
    observed_label = tr("%s (observé)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observé, estimé)")
    # First official source with holidays dates (Law 63-51).
    start_year = 1964
    supported_languages = ("en_US", "fr_SN")

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
            self, cls=SenegalIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, SenegalStaticHolidays)
        # Law 74-52.
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 1975)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        if self._year >= 1983:
            # Ashura.
            self._add_ashura_day(tr("Tamxarit"))

        if self._year >= 2014:
            # Grand Magal of Touba.
            self._add_grand_magal_of_touba(tr("Grand Magal de Touba"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("Journée du Maouloud"))

        # Eid al-Fitr.
        for dt in self._add_eid_al_fitr_day(tr("Journée de la Korité")):
            self._add_observed(dt)

        # Eid al-Adha.
        for dt in self._add_eid_al_adha_day(tr("Journée de la Tabaski")):
            self._add_observed(dt)

        # New Year's Day.
        self._add_new_years_day(tr("Jour de l'an"))

        if 1983 <= self._year <= 1989:
            # Senegambia Confederation Day.
            self._add_holiday_feb_1(tr("Fête de la Confédération de la Sénégambie"))

        # Independence Day.
        name = tr("Fête de l'Indépendance")
        if self._year >= 1975:
            self._add_holiday_apr_4(name)
        else:
            self._add_holiday_jul_14(name)

        # Easter Monday.
        self._add_easter_monday(tr("Lundi de Pâques"))

        # Labor Day.
        self._add_labor_day(tr("Fête du Travail"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Jeudi de l'Ascension"))

        # Whit Monday.
        self._add_whit_monday(tr("Lundi de Pentecôte"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assomption"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Toussaint"))

        # Christmas Day.
        self._add_christmas_day(tr("Noël"))


class SN(Senegal):
    pass


class SEN(Senegal):
    pass


class SenegalIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20250609044648/https://www.timeanddate.com/holidays/senegal/tamkharit
    ASHURA_DATES_CONFIRMED_YEARS = (2020, 2024)
    ASHURA_DATES = {
        2024: (JUL, 17),
    }

    # https://web.archive.org/web/20250609061446/https://www.timeanddate.com/holidays/senegal/grand-magal-de-touba
    GRAND_MAGAL_OF_TOUBA_DATES_CONFIRMED_YEARS = (2020, 2024)
    GRAND_MAGAL_OF_TOUBA_DATES = {
        2020: (OCT, 6),
        2021: (SEP, 26),
        2022: (SEP, 15),
        2023: (SEP, 4),
        2024: (AUG, 23),
    }

    # https://web.archive.org/web/20250609091559/https://www.timeanddate.com/holidays/senegal/maouloud
    MAWLID_DATES_CONFIRMED_YEARS = (2020, 2024)

    # https://web.archive.org/web/20250609092813/https://www.timeanddate.com/holidays/senegal/korite
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2020, 2025)
    EID_AL_FITR_DATES = {
        2021: (MAY, 12),
        2022: (MAY, 1),
        2023: (APR, 22),
    }

    # https://web.archive.org/web/20250609102658/https://www.timeanddate.com/holidays/senegal/tabaski
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2020, 2024)
    EID_AL_ADHA_DATES = {
        2021: (JUL, 21),
        2022: (JUL, 10),
        2023: (JUN, 29),
        2024: (JUN, 17),
    }


class SenegalStaticHolidays:
    """Senegal special holidays.

    References:
        * [29th October, 2018 Public holiday](https://web.archive.org/web/20250608105852/https://www.juriafrica.com/lex/decret-2018-1942-26-octobre-2018-48947.htm)
        * [26th December, 2022 Public holiday](https://web.archive.org/web/20250608135605/https://primature.sn/publications/conseil-des-ministres/conseil-des-ministres-du-22-decembre-2022)
    """

    # Public holiday.
    name = tr("Jour férié")
    special_public_holidays = {
        2018: (OCT, 29, name),
        2022: (DEC, 26, name),
    }
