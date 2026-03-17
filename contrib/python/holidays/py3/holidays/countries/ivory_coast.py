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

from holidays.calendars.gregorian import FEB, OCT
from holidays.groups import (
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class IvoryCoast(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays, StaticHolidays
):
    """Ivory Coast holidays.

    References:
        * [Decree No. 96-205](https://web.archive.org/web/20240701120937/http://www.droit-afrique.com/uploads/RCI-Decret-1996-205-jours-feries.pdf)
        * Decree No. 2011-371:
            * [page 1](https://web.archive.org/web/20180617165811/http://www.cgeci.org/cgeci/docs/documents/Doc-ferie-1.pdf)
            * [page 2](https://web.archive.org/web/20180826205106/http://www.cgeci.org/cgeci/docs/documents/Doc-ferie-2.pdf)
        * <https://fr.wikipedia.org/wiki/Fêtes_et_jours_fériés_en_Côte_d'Ivoire>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Ivory_Coast>
        * <https://web.archive.org/web/20250408210549/https://www.timeanddate.com/holidays/ivory-coast/>
        * [National Peace Day](https://en.wikipedia.org/wiki/Ivory_Coast#Independence)

    Note:
        The oldest decree available online that underpins the public holidays defined here
        for the Ivory Coast is Decree no. 96-205 of March 7, 1996.

        In Islamic calendar, days begin at sunset. The naming convention "day after" refers
        to the daylight hours following the night of the celebration, which is technically
        the same Gregorian calendar day.

        According to Decree no. 2011-371 of 4 November 2011, if Eid al-Fitr, Eid al-Adha
        or Christmas Day falls on a Sunday, the following Monday is also a holiday.
    """

    country = "CI"
    default_language = "fr"
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    # Day after the %s.
    observed_label = tr("Lendemain de la %s")
    # Day after the %s (estimated).
    observed_estimated_label = tr("Lendemain de la %s (estimé)")
    start_year = 1997
    supported_languages = ("en_CI", "en_US", "fr")

    def __init__(self, *args, islamic_show_estimated: bool = False, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        # The observed dates for the Ivory Coast's islamic holidays have been verified against
        # local references (COSIM) and align with the default Umm al-Qura calculations.
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, IvoryCoastStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("1er janvier"))

        # Easter Monday.
        self._add_easter_monday(tr("Lundi de Pâques"))

        # Labor Day.
        self._add_observed(self._add_labor_day(tr("Fête du travail")))

        # Ascension Day.
        self._add_ascension_thursday(tr("Jour de l'Ascension"))

        # Whit Monday.
        self._add_whit_monday(tr("Lundi de Pentecôte"))

        # Independence Day.
        self._add_observed(self._add_holiday_aug_7(tr("Fête Nationale")))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Fête de l'Assomption"))

        # National Peace Day.
        self._add_holiday_nov_15(tr("Journée Nationale de la Paix"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Fête de la Toussaint"))

        if self._year <= 2000:
            self._add_holiday_dec_7(
                # Anniversary of death of President Felix Houphouet-Boigny.
                tr("Anniversaire du décès du Président Felix Houphouet-Boigny")
            )

        # Christmas Day.
        dt = self._add_christmas_day(tr("Fête de Noël"))
        if self._year >= 2011:
            self._add_observed(dt)

        # Day after Prophet's Birthday.
        self._add_mawlid_day(tr("Lendemain de l'Anniversaire de la Naissance du Prophète Mahomet"))

        # Day after Night of Power.
        self._add_laylat_al_qadr_day(tr("Lendemain de la Nuit du Destin"))

        # Eid al-Fitr.
        for dt in self._add_eid_al_fitr_day(tr("Fête de fin du Ramadan")):
            if self._year >= 2012:
                self._add_observed(dt)

        # Eid al-Adha.
        for dt in self._add_eid_al_adha_day(tr("Fête de la Tabaski")):
            if self._year >= 2012:
                self._add_observed(dt)


class CI(IvoryCoast):
    pass


class CIV(IvoryCoast):
    pass


class IvoryCoastStaticHolidays:
    """Ivory Coast special holidays.

    References:
        * [2010 Presidential Election](https://web.archive.org/web/20250427185110/https://www.gouv.ci/_actualite-article.php?d=4.&recordID=1255&p=366)
        * [2024 AFCON](https://web.archive.org/web/20250429075000/https://apanews.net/public-holiday-as-cote-divoire-wins-afcon-trophy/)
    """

    special_public_holidays = {
        # Public holiday for Presidential election preparation.
        2010: (OCT, 29, tr("Jour férié pour la préparation de l'élection présidentielle")),
        # 2024 African Cup of Nations Victory.
        2024: (FEB, 12, tr("Victoire à la Coupe d'Afrique des Nations 2024")),
    }
