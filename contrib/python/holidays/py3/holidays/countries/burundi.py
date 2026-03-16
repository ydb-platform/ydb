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
from holidays.calendars.gregorian import JUN, JUL, SEP, OCT
from holidays.groups import ChristianHolidays, IslamicHolidays, InternationalHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Burundi(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Burundi holidays.

    References:
        * [Décret n° 100/150 du 7 Juin 2021](https://web.archive.org/web/20240514031141/https://www.presidence.gov.bi/wp-content/uploads/2021/06/Liste-et-regime-des-jours-feries.pdf)
        * [Republic of Burundi's Ministry of Foreign Affairs Public Holidays List](https://web.archive.org/web/20250820080112/https://www.mae.gov.bi/en/official-holidays/)
        * <https://web.archive.org/web/20250820095115/https://www.timeanddate.com/holidays/burundi/2025>

    When it appears inappropriate for a public holiday listed in Article 1 to be observed because
    it coincides with a Sunday, it shall be postponed to the following working day, which shall
    accordingly be considered a public holiday and observed as such.
    """

    country = "BI"
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    default_language = "fr_BI"
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observé, estimé)")
    # %s (observed).
    observed_label = tr("%s (observé)")
    start_year = 1962
    supported_languages = ("en_US", "fr_BI")

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
            self, cls=BurundiIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("Jour de l'an")))

        if self._year >= 1992:
            # Unity Day.
            dts_observed.add(self._add_holiday_feb_5(tr("Fête de l'Unité")))

        if self._year >= 1995:
            dts_observed.add(
                self._add_holiday_apr_6(
                    # Commemoration of the Assassination of President Cyprien Ntaryamira.
                    tr("Commémoration de l'Assassinat du Président Cyprien Ntaryamira")
                )
            )

        # International Labor Day.
        dts_observed.add(self._add_labor_day(tr("Fête Internationale du Travail")))

        # Ascension Day.
        self._add_ascension_thursday(tr("Jour de l'Ascension"))

        if self._year >= 2022:
            dts_observed.add(
                self._add_holiday_jun_8(
                    # National Day of Patriotism and Commemoration of the Death of
                    # President Pierre Nkurunziza.
                    tr(
                        "Journée Nationale du Patriotisme et Commémoration de la Mort "
                        "du Président Pierre Nkurunziza"
                    )
                )
            )

        # Independence Day.
        dts_observed.add(self._add_holiday_jul_1(tr("Anniversaire de l'Indépendance")))

        # Assumption Day.
        dts_observed.add(self._add_assumption_of_mary_day(tr("Assomption")))

        dts_observed.add(
            self._add_holiday_oct_13(
                # Commemoration of the Assassination of National Hero, Prince Louis Rwagasore.
                tr("Commémoration de l'Assassinat du Héros National, le Prince Louis Rwagasore")
            )
        )
        if self._year >= 1994:
            dts_observed.add(
                self._add_holiday_oct_21(
                    # Commemoration of the Assassination of President Melchior Ndadaye.
                    tr("Commémoration de l'Assassinat du Président Melchior Ndadaye")
                )
            )

        # All Saints' Day.
        dts_observed.add(self._add_all_saints_day(tr("Toussaint")))

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Noël")))

        # Eid al-Fitr.
        dts_observed.update(self._add_eid_al_fitr_day(tr("Aid-El-Fithr")))

        # Eid al-Adha.
        dts_observed.update(self._add_eid_al_adha_day(tr("Aid-El-Adha")))

        if self.observed:
            self._populate_observed(dts_observed)


class BI(Burundi):
    pass


class BDI(Burundi):
    pass


class BurundiIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2014, 2025)
    EID_AL_ADHA_DATES = {
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2014, 2025)
    EID_AL_FITR_DATES = {
        2014: (JUL, 29),
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
    }
