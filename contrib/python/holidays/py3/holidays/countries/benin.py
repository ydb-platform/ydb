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

from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class Benin(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Benin holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Benin>
        * [Order 71-14](https://web.archive.org/web/20240727075244/https://sgg.gouv.bj/doc/ordonnance-1971-14/)
        * [Order 76-35](https://web.archive.org/web/20240726042853/https://sgg.gouv.bj/doc/ordonnance-1976-35/)
        * [Order 79-33](https://web.archive.org/web/20240725115717/https://sgg.gouv.bj/doc/ordonnance-1979-33/)
        * [Law 90-019](https://web.archive.org/web/20250527021602/https://sgg.gouv.bj/doc/loi-90-019/)
        * [Law 97-031](https://web.archive.org/web/20250527022039/https://sgg.gouv.bj/doc/loi-97-031/)
        * [Law 2024-32](https://web.archive.org/web/20250321041430/https://sgg.gouv.bj/doc/loi-2024-32/)

    Official dates:
        * [2015](https://web.archive.org/web/20211130121041/https://beninconsulate.co.ke/index.php/news-events/7-public-holidays-2015)
        * [2017](https://web.archive.org/web/20211130124252/https://beninconsulate.co.ke/index.php/news-events/8-public-holidays-2017)
        * [2019](https://web.archive.org/web/20231003144018/https://beninconsulate.co.ke/index.php/news-events)
        * [2024](https://web.archive.org/web/20241211032923/https://beninconsulate.co.ke/index.php/news-events)
        * [2025](https://web.archive.org/web/20250317141957/https://beninconsulate.co.ke/index.php/news-events?start=1)

    Certain holidays dates:
        * 2022:
            * [All Saints' Day](https://web.archive.org/web/20241010101230/https://travail.gouv.bj/page/communiques/la-journee-du-mardi-1er-novembre-2022-declaree-feriee-chomee-et-payee)
            * [Christmas Day](https://web.archive.org/web/20240814200022/https://travail.gouv.bj/page/communiques/la-journee-du-dimanche-25-decembre-2022-declaree-feriee-chomee-et-payee-a-loccasion-de-la-celebration-de-la-fete-de-noel-lire-le-communique-du-ministre-du-travail-et-de-la-fonction-publique)
        * 2023:
            * [New Year's Day](https://web.archive.org/web/20240724084416/https://travail.gouv.bj/page/communiques/la-journee-du-dimanche-1er-janvier-2023-declaree-feriee-chomee-et-payee-a-loccasion-de-la-celebration-de-la-fete-du-nouvel-an)
            * [Vodoun Festival](https://web.archive.org/web/20240724084406/https://travail.gouv.bj/page/communiques/la-journee-du-mardi-10-janvier-2023-est-declaree-feriee-chomee-et-payee-a-loccasion-de-la-celebration-de-la-fete-annuelle-des-religions-traditionnelles-2301071350-956)
            * [Eid al-Fitr](https://web.archive.org/web/20240723004550/https://travail.gouv.bj/page/communiques/celebration-le-vendredi-21-avril-2023-de-la-fete-du-ramadan)
            * [Easter Monday](https://web.archive.org/web/20241010103541/https://travail.gouv.bj/page/communiques/la-journee-du-lundi-10-avril-2023-est-declaree-feriee-chomee-et-payee-en-raison-de-la-fete-de-paques)
            * [Whit Monday](https://web.archive.org/web/20240911163554/https://travail.gouv.bj/page/communiques/la-fete-de-pentecote)
            * [Labor Day](https://web.archive.org/web/20240814205702/https://travail.gouv.bj/page/communiques/la-journee-du-lundi-1er-mai-2023-declaree-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national-a-loccasion-de-la-celebration-de-la-fete-du-travail)
            * [Assumption Day](https://web.archive.org/web/20241214182235/https://travail.gouv.bj/page/communiques/la-journee-du-mardi-15-aout-2023-declaree-feriee-chomee-et-payee-en-raison-de-la-fete-de-lassomption)
            * [Independence Day](https://web.archive.org/web/20240723004533/https://travail.gouv.bj/page/communiques/la-journee-du-mardi-1er-aout-2023-declaree-feriee-chomee-et-payee-a-l-occasion-de-la-fete-nationale)
            * [Eid al-Adha](https://web.archive.org/web/20250422035008/https://travail.gouv.bj/page/communiques/communique-du-ministere-du-travail-et-de-la-fonction-publique-2306262022-744)
        * 2024:
            * [Vodoun Festival](https://web.archive.org/web/20240723004435/https://travail.gouv.bj/page/communiques/la-journee-du-mercredi-10-janvier-2024-est-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
            * [Eid al-Fitr](https://web.archive.org/web/20241010114534/https://travail.gouv.bj/page/communiques/la-journee-du-mercredi-10-avril-2024-jour-du-ramadan-est-declaree-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
            * [Easter Monday](https://web.archive.org/web/20241106004344/https://travail.gouv.bj/page/communiques/la-journee-du-lundi-1er-avril-2024-lundi-de-paques-est-chomee)
            * [Assumption Day](https://web.archive.org/web/20250215044724/https://travail.gouv.bj/page/communiques/la-journee-du-dimanche-15-aout-jour-de-lassomption-est-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
            * [Eid al-Adha](https://web.archive.org/web/20241214200230/https://travail.gouv.bj/page/communiques/jour-de-la-tabaski)
            * [Labor Day](https://web.archive.org/web/20250418224409/https://www.travail.gouv.bj/page/communiques/la-journee-du-mercredi-1er-mai-2024-journee-de-la-fete-du-travail-est-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
            * [Ascension Day](https://web.archive.org/web/20241214204011/https://travail.gouv.bj/page/communiques/le-jeudi-09-mai-2024-jour-de-lascension-est-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
            * [Whit Monday](https://web.archive.org/web/20241010113401/https://travail.gouv.bj/page/communiques/le-lundi-20-mai-2024-lundi-de-pentecote-est-ferie-chome-et-paye-sur-toute-letendue-du-territoire-national)
            * [Prophet's Birthday](https://web.archive.org/web/20250215034616/https://travail.gouv.bj/page/communiques/la-journee-du-dimanche-15-septembre-2024-jour-de-maoloud-est-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
            * [Christmas Day](https://web.archive.org/web/20250122081816/https://travail.gouv.bj/page/communiques/la-journee-du-mercredi-25-decembre-2024-est-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
        * 2025:
            * [New Year's Day](https://web.archive.org/web/20250422023225/https://travail.gouv.bj/page/communiques/la-journee-du-mercredi-1er-janvier-2025-fete-du-nouvel-an-est-feriee-chomee-et-payee-sur-toute-letendue-du-territoire-national)
            * [Vodoun Festival](https://web.archive.org/web/20250422034718/https://travail.gouv.bj/page/communiques/les-journees-du-jeudi-9-janvier-2024-et-vendredi-10-janvier-2024-sont-feriees-chomees-et-payees-sur-toute-letendue-du-territoire-national)
    """

    country = "BJ"
    default_language = "fr_BJ"
    # %s (estimated).
    estimated_label = tr("%s (estimé)")
    # Order 76-35 of June 30, 1976.
    start_year = 1977
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("en_US", "fr_BJ")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Fête du Nouvel An"))

        # Established in 1998 by Law 97-031.
        # Changed in 2025 by Law 2024-32.
        if self._year >= 1998:
            # Vodoun Festival.
            name = tr("Fête annuelle des religions traditionnelles")
            if self._year >= 2025:
                self._add_holiday_1_day_prior_2nd_fri_of_jan(name)
                self._add_holiday_2nd_fri_of_jan(name)
            else:
                self._add_holiday_jan_10(name)

        # Established in 1980 by Order 79-33.
        # Abolished in 1991 by Law 90-019.
        if 1980 <= self._year <= 1990:
            # Martyrs' Day.
            self._add_holiday_jan_16(tr("Journée des Martyrs"))

            # Youth Day.
            self._add_holiday_apr_1(tr("Journée de la Jeunesse Béninoise"))

        # Changed in 1991 by Law 90-019.
        if self._year >= 1991:
            # Easter Monday.
            self._add_easter_monday(tr("Lundi de Pâques"))
        else:
            # Easter Sunday.
            self._add_easter_sunday(tr("Jour de Pâques"))

        # Labor Day.
        self._add_labor_day(tr("Fête du Travail"))

        # Established in 1991 by Law 90-019.
        if self._year >= 1991:
            # Ascension Day.
            self._add_ascension_thursday(tr("Jour de l'Ascension"))

        if self._year >= 1991:
            # Whit Monday.
            self._add_whit_monday(tr("Lundi de Pentecôte"))
        else:
            # Whit Sunday.
            self._add_whit_sunday(tr("Jour de Pentecôte"))

        # Established in 1990 by Law 90-019.
        if self._year >= 1990:
            # Assumption Day.
            self._add_assumption_of_mary_day(tr("Jour de l'Assomption"))

        # Established in 1977 by Order 71-14.
        # Abolished in 1990 by Law 90-019.
        if self._year <= 1989:
            # Armed Forces Day.
            self._add_holiday_oct_26(tr("Fête des Forces Armées Populaires du Bénin"))

        # Established in 1990 by Law 90-019.
        if self._year >= 1990:
            # All Saints' Day.
            self._add_all_saints_day(tr("La Toussaint"))

        # National Day.
        name = tr("Fête Nationale")
        if self._year >= 1991:
            self._add_holiday_aug_1(name)
        else:
            self._add_holiday_nov_30(name)

        # Christmas Day.
        self._add_christmas_day(tr("Jour de Noël"))

        # Established in 1977 by Order 71-14.
        # Abolished in 1990 by Law 90-019.
        if self._year <= 1989:
            # Production Day.
            self._add_holiday_dec_31(tr("Fête de la Production"))

        # Islamic holidays.

        # Established in 1990 by Law 90-019.
        if self._year >= 1990:
            # Prophet's Birthday.
            self._add_mawlid_day(tr("Journée Maouloud"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Jour du Ramadan"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Jour de la Tabaski"))

    def _populate_workday_holidays(self):
        if self._year >= 1990:
            # Remembrance Day.
            self._add_holiday_jan_16(tr("Journée de Souvenir"))

            # People's Sovereignty Day.
            self._add_holiday_feb_28(tr("Journée de la Souveraineté de Peuple"))

            # Women's Day.
            self._add_holiday_mar_8(tr("Journée de la Femme"))


class BJ(Benin):
    pass


class BEN(Benin):
    pass
