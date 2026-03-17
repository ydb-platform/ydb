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

from holidays.constants import OPTIONAL, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Haiti(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Haiti holidays.

    References:
        * <https://web.archive.org/web/20250214180331/https://www.wipo.int/wipolex/en/legislation/details/7837>
        * <https://web.archive.org/web/20241206114430/https://haitiwonderland.com/haiti/histoire/jours-feries-en-haiti/27>
        * <https://web.archive.org/web/20241226203305/https://www.haiti-reference.info/pages/plan/generalites/calendrier-dhaiti/>
    """

    country = "HT"
    supported_categories = (OPTIONAL, PUBLIC)
    default_language = "fr_HT"
    supported_languages = ("en_US", "es", "fr_HT", "ht")
    # 1987 Constitution.
    start_year = 1987

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # ARTICLE 275.1 : National Holidays.
        # Both Battle of Vertieres Day and Armed Forces Day are on Nov 18, declared discretely.

        # National Independence Day.
        self._add_holiday_jan_1(tr("Fête de l'Indépendance Nationale"))

        # Ancestry Day.
        self._add_holiday_jan_2(tr("Jour des Aïeux"))

        # Agriculture and Labor Day.
        self._add_labor_day(tr("Fête de l'Agriculture et du Travail"))

        # Flag Day and University Day.
        self._add_holiday_may_18(tr("Fête du Drapeau et de l'Université"))

        # Commemoration of the Battle of Vertieres.
        self._add_holiday_nov_18(tr("Commémoration de la Bataille de Vertières"))

        # Armed Forces Day.
        self._add_holiday_nov_18(tr("Jour des Forces Armées"))

        # Other Common Holidays.

        # New Year's Day.
        self._add_new_years_day(tr("Nouvel An"))

        # Carnival.
        self._add_carnival_sunday(tr("Carnaval"))

        # Shrove Monday.
        self._add_carnival_monday(tr("Lundi Gras"))

        # Fat Tuesday.
        self._add_carnival_tuesday(tr("Mardi Gras"))

        # Good Friday.
        self._add_good_friday(tr("Vendredi Saint"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Pâques"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fête-Dieu"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assomption de Marie"))

        # Death of Dessalines.
        self._add_holiday_oct_17(tr("Mort de Dessalines"))

        # All Saints' Day.
        self._add_all_saints_day(tr("La Toussaint"))

        # Day of the Dead.
        self._add_all_souls_day(tr("Fête des Morts"))

        # Christmas Day.
        self._add_christmas_day(tr("Noël"))

    def _populate_optional_holidays(self):
        # Ash Wednesday.
        self._add_ash_wednesday(tr("Mercredi des Cendres"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jeudi Saint"))

        # National Sovereignty Day.
        self._add_holiday_may_23(tr("Fête de la Souveraineté Nationale"))

        # Feast of Lady of Perpetual Help, Patroness of Haiti.
        self._add_holiday_jun_27(tr("Fête de Notre-Dame du Perpétuel Secours, patronne d'Haiti"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension"))

        # Birth Anniversary of Jean-Jacques Dessalines.
        self._add_holiday_sep_20(tr("Anniversaire de Naissance de Jean-Jacques Dessalines"))

        # Discovery Day.
        self._add_holiday_dec_5(tr("Jour de la Découverte"))


class HT(Haiti):
    pass


class HTI(Haiti):
    pass
