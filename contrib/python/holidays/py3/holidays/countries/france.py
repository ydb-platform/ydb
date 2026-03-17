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

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class France(HolidayBase, ChristianHolidays, InternationalHolidays):
    """France holidays.

    References:
        * <https://fr.wikipedia.org/wiki/Fêtes_et_jours_fériés_en_France>
        * <https://web.archive.org/web/20250424031328/https://www.service-public.fr/particuliers/vosdroits/F2405>
        * [Arrêté du 29 germinal an X](https://web.archive.org/web/20250327095254/https://journals.openedition.org/rdr/439)
        * [Loi du 6 juillet 1880](https://archive.org/details/jorf-18800707-185)
        * [Loi du 8 mars 1886](https://archive.org/details/jorf-18860309-67)
        * [Loi du 9 décembre 1905](https://web.archive.org/web/20250617084843/https://mjp.univ-perp.fr/france/1905laicite.htm)
        * [Loi du 24 octobre 1922](https://web.archive.org/web/20250618030815/https://archives.defense.gouv.fr/content/download/102094/992430/Loi%20du%2024%20octobre%201922%20fixant%20la%20journée%20commémorative%20du%2011%20novembre.pdf)
        * [Loi n°46-828 du 26 avril 1946](https://archive.org/details/jorf-19460428-100)
        * [Loi n°46-934 du 7 mai 1946](https://archive.org/details/jorf-19460508-107)
        * [Loi n°47-778 du 30 avril 1947 ](https://archive.org/details/jorf-19470501-104)
        * [Loi n°48-746 du 29 avril 1948](https://archive.org/details/jorf-19480430-104)
        * [Loi n°53-225 du 20 mars 1953](https://archive.org/details/jorf-19530321-69)
        * [Décret n°59-533 du 11 avril 1959](https://archive.org/details/jorf-19590415-88)
        * [Loi n° 61-814 du 29 juillet 1961](https://web.archive.org/web/20240715174837/https://www.legifrance.gouv.fr/loda/id/JORFTEXT000000684031)
        * [Loi n°81-893 du 2 octobre 1981](https://archive.org/details/jorf-19811003-232)
        * [Loi n° 83-550 du 30 juin 1983](https://archive.org/details/jorf-19830701-151)
        * [Décret n°83-1003 du 23 novembre 1983](https://archive.org/details/jorf-19831124-272_202506)
        * [Loi n° 2004-626 du 30 juin 2004](https://web.archive.org/web/20220830045136/https://www.legifrance.gouv.fr/loda/id/JORFTEXT000000622485/)
        * [Loi n° 2008-351 du 16 avril 2008](https://web.archive.org/web/20220602124233/https://www.legifrance.gouv.fr/loda/id/JORFTEXT000018656009/)
        * [Décret n° 2012-553 du 23 avril 2012](https://web.archive.org/web/20231104120733/https://www.legifrance.gouv.fr/jorf/id/JORFTEXT000025743756)
        * <https://web.archive.org/web/20240719134144/https://www.moselle.gouv.fr/contenu/telechargement/5830/42693/file/fiche+commerce+MAJ.pdf>
        * <https://web.archive.org/web/20190812110746/http://histoire.assemblee.pf/articles.php?id=567>
        * <https://web.archive.org/web/20190812043635/http://histoire.assemblee.pf/articles.php?id=568>
        * <https://web.archive.org/web/20250618073111/https://www.tahitipresse.pf/2025/05/polynesie-29-juin-ou-20-novembre/>
        * <https://web.archive.org/web/20250618061809/https://la1ere.franceinfo.fr/nouvellecaledonie/commemorations-differentes-24-septembre-752087.html>

    Some provinces have specific holidays, only those are included in the
    PROVINCES, because these provinces have different administrative status,
    which makes it difficult to enumerate.
    """

    country = "FR"
    default_language = "fr"
    # Arrêté du 29 germinal an X (April 19th, 1802),
    # Reaffirmed by Article 42 de la loi du 9 décembre 1905.
    start_year = 1803
    # fmt: off
    subdivisions: tuple[str, ...] = (
        "57",   # Moselle.
        "6AE",  # Alsace.
        "971",  # Guadeloupe.
        "972",  # Martinique.
        "973",  # Guyane.
        "974",  # La Réunion.
        "976",  # Mayotte.
        "BL",   # Saint-Barthélemy.
        "MF",   # Saint-Martin.
        "NC",   # Nouvelle-Calédonie,
        "PF",   # Polynésie Française.
        "PM",   # Saint-Pierre-et-Miquelon.
        "TF",   # Terres australes françaises.
        "WF",   # Wallis-et-Futuna.
    )
    # fmt: on
    subdivisions_aliases = {
        "Moselle": "57",
        "Alsace": "6AE",
        "GP": "971",
        "GUA": "971",
        "Guadeloupe": "971",
        "MQ": "972",
        "Martinique": "972",
        "GY": "973",
        "Guyane": "973",
        "RE": "974",
        "LRE": "974",
        "La Réunion": "974",
        "YT": "976",
        "MAY": "976",
        "Mayotte": "976",
        "Saint-Barthélemy": "BL",
        "Saint-Martin": "MF",
        "Nouvelle-Calédonie": "NC",
        "Polynésie Française": "PF",
        "Saint-Pierre-et-Miquelon": "PM",
        "Terres australes françaises": "TF",
        "Wallis-et-Futuna": "WF",
    }
    supported_languages = ("en_US", "fr", "th", "uk")
    _deprecated_subdivisions = (
        "Alsace-Moselle",
        "GES",
        "Métropole",
        "Saint-Barthélémy",
    )

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Established on March 28th, 1810.
        if self._year >= 1811:
            # New Year's Day.
            self._add_new_years_day(tr("Jour de l'an"))

        # Established on March 8th, 1886.
        if self._year >= 1886:
            # Easter Monday.
            self._add_easter_monday(tr("Lundi de Pâques"))

            # Removed on June 30th, 2004.
            # Readded on April 16th, 2008.
            if self._year not in {2005, 2006, 2007}:
                # Whit Monday.
                self._add_whit_monday(tr("Lundi de Pentecôte"))

        # Made unofficial public holiday with no name on April 23rd, 1919.
        # Included in official list by Vichy France with new name on April 24th, 1941.
        # Confirmed for 1946 with no name on April 26th, 1946.
        # Added annually from 1947 onwards with no name on April 30th, 1947.
        # Got its current name on April 29th, 1948.
        if self._year >= 1919:
            if self._year >= 1948:
                # Labor Day.
                name = tr("Fête du Travail")
            elif 1941 <= self._year <= 1945:
                # Labor and Social Concord Day.
                name = tr("Fête du Travail et de la Concorde sociale")
            else:
                # May Day.
                name = tr("1er mai")
            self._add_labor_day(name)

        # Commemorated on May 7th, 1946 as 1st Sunday after May 7th.
        # Upgraded to Public Holiday on May 8th directly on March 20th, 1953.
        # Removed from 1960 onwards per April 11th, 1959 decree.
        # Readded on October 2nd, 1981.
        if 1953 <= self._year <= 1959 or self._year >= 1982:
            # Victory Day.
            self._add_world_war_two_victory_day(tr("Fête de la Victoire"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension"))

        # Established on July 6th, 1880.
        if self._year >= 1880:
            # National Day.
            self._add_holiday_jul_14(tr("Fête nationale"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assomption"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Toussaint"))

        # Established on October 24th, 1922.
        if self._year >= 1922:
            # Armistice Day.
            self._add_remembrance_day(tr("Armistice"))

        # Christmas Day.
        self._add_christmas_day(tr("Noël"))

        if self.subdiv == "Alsace-Moselle":
            self._populate_subdiv_6ae_public_holidays()
        elif self.subdiv == "Saint-Barthélémy":
            self._populate_subdiv_bl_public_holidays()

    # Moselle.
    def _populate_subdiv_57_public_holidays(self):
        # Established on August 16th, 1892.
        if self._year >= 1893:
            # Good Friday.
            self._add_good_friday(tr("Vendredi saint"))

        # Established on August 16th, 1892.
        if self._year >= 1892:
            # Saint Stephen's Day.
            self._add_christmas_day_two(tr("Saint Étienne"))

    # Alsace.
    def _populate_subdiv_6ae_public_holidays(self):
        # Established on August 16th, 1892.
        if self._year >= 1893:
            # Good Friday.
            self._add_good_friday(tr("Vendredi saint"))

        # Established on August 16th, 1892.
        if self._year >= 1892:
            # Saint Stephen's Day.
            self._add_christmas_day_two(tr("Saint Étienne"))

    # Guadeloupe.
    def _populate_subdiv_971_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Vendredi saint"))

        # Mi-Careme.
        self._add_holiday_24_days_prior_easter(tr("Mi-Carême"))

        # Provision for Public Holidays decreed on June 30th, 1983.
        # Date for each DOM declared on November 23rd, 1983.
        # Victor Schoelcher Day is based on June 30th, 1983 provision.
        if self._year >= 1984:
            # Abolition of Slavery.
            self._add_holiday_may_27(tr("Abolition de l'esclavage"))

            # Victor Schoelcher Day.
            self._add_holiday_jul_21(tr("Fête de Victor Schoelcher"))

    # Martinique.
    def _populate_subdiv_972_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Vendredi saint"))

        # Provision for Public Holidays decreed on June 30th, 1983.
        # Date for each DOM declared on November 23rd, 1983.
        # Victor Schoelcher Day is based on June 30th, 1983 provision.
        if self._year >= 1984:
            # Abolition of Slavery.
            self._add_holiday_may_22(tr("Abolition de l'esclavage"))

            # Victor Schoelcher Day.
            self._add_holiday_jul_21(tr("Fête de Victor Schoelcher"))

    # Guyane.
    def _populate_subdiv_973_public_holidays(self):
        # Provision for Public Holidays decreed on June 30th, 1983.
        # Date for each DOM declared on November 23rd, 1983.
        if self._year >= 1984:
            # Abolition of Slavery.
            self._add_holiday_jun_10(tr("Abolition de l'esclavage"))

    # Reunion.
    def _populate_subdiv_974_public_holidays(self):
        # Provision for Public Holidays decreed on June 30th, 1983.
        # Date for each DOM declared on November 23rd, 1983.
        if self._year >= 1983:
            # Abolition of Slavery.
            self._add_holiday_dec_20(tr("Abolition de l'esclavage"))

    # Mayotte.
    def _populate_subdiv_976_public_holidays(self):
        # Provision for Public Holidays decreed on June 30th, 1983.
        # Date for each DOM declared on November 23rd, 1983.
        if self._year >= 1984:
            # Abolition of Slavery.
            self._add_holiday_apr_27(tr("Abolition de l'esclavage"))

    # Saint Barthelemy.
    def _populate_subdiv_bl_public_holidays(self):
        # Provision for Public Holidays decreed on June 30th, 1983.
        # Date for each DOM declared on November 23rd, 1983.
        # Amended to include Saint-Barthélemy and Saint-Martin subdivision on April 23rd, 2012.
        if self._year >= 2012:
            # Abolition of Slavery.
            self._add_holiday_oct_9(tr("Abolition de l'esclavage"))

    # Saint Martin.
    def _populate_subdiv_mf_public_holidays(self):
        # Provision for Public Holidays decreed on June 30th, 1983.
        # Date for each DOM declared on November 23rd, 1983.
        # Amended to include Saint-Barthélemy and Saint-Martin subdivision on April 23rd, 2012.
        # Victor Schoelcher Day is based on June 30th, 1983 provision.
        if self._year >= 2012:
            # Abolition of Slavery.
            self._add_holiday_may_28(tr("Abolition de l'esclavage"))

            # Victor Schoelcher Day.
            self._add_holiday_jul_21(tr("Fête de Victor Schoelcher"))

    # New Caledonia.
    def _populate_subdiv_nc_public_holidays(self):
        # First observed in 1953.
        # Renamed in 2004.
        if self._year >= 1953:
            self._add_holiday_sep_24(
                # Citizenship Day.
                tr("Fête de la Citoyenneté")
                if self._year >= 2004
                # Annexation Day.
                else tr("Fête de la prise de possession")
            )

    # French Polynesia.
    def _populate_subdiv_pf_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Vendredi saint"))

        # Established on February 1st, 1978.
        if self._year >= 1978:
            # Missionary Day.
            self._add_holiday_mar_5(tr("Arrivée de l'Évangile"))

        # Established on May 30th, 1985.
        # Replaced by Matāri'i on April 30th, 2024.
        if 1985 <= self._year <= 2024:
            # Internal Autonomy Day.
            self._add_holiday_jun_29(tr("Fête de l'autonomie"))

        # Established on April 30th, 2024.
        if self._year >= 2025:
            # Matāri'i.
            self._add_holiday_nov_20(tr("Matāri'i"))

    #  Wallis and Futuna.
    def _populate_subdiv_wf_public_holidays(self):
        # While it's not clear when these holidays were added,
        # they're likely added after local autonomy was granted on July 29th, 1961.
        if self._year >= 1962:
            # Feast of Saint Peter Chanel.
            self._add_holiday_apr_28(tr("Saint Pierre Chanel"))

            # Saints Peter and Paul Day.
            self._add_saints_peter_and_paul_day(tr("Saints Pierre et Paul"))

            # Territory Day.
            self._add_holiday_jul_29(tr("Fête du Territoire"))


class FR(France):
    """FR is also used by dateutil (Friday), so be careful with this one."""

    pass


class FRA(France):
    pass
