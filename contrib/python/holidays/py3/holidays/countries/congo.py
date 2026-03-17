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


class Congo(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Congo holidays.

    References:
        * [Loi N° 2-94](https://web.archive.org/web/20241003070938/http://mokili.free.fr/jours_feries.php)
        * [Loi N° 18-2010](https://web.archive.org/web/20250427173832/https://www.finances.gouv.cg/sites/default/files/documents/n¯18-2010%20du%2027%20novembre%202010.PDF)

    Cross-Checked With:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Republic_of_the_Congo>
    """

    country = "CG"
    default_language = "fr"
    supported_languages = ("en_US", "fr")
    # Loi N° 2-94 of 1 March 1994.
    start_year = 1994

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Jour de l'An"))

        # Easter Monday.
        self._add_easter_monday(tr("Lundi de Pâques"))

        # Labor Day.
        self._add_labor_day(tr("Fête du Travail"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Ascension"))

        # Whit Monday.
        self._add_whit_monday(tr("Lundi de Pentecôte"))

        # Reconciliation Day.
        self._add_holiday_jun_10(tr("Fête de la Réconciliation"))

        # National Day.
        self._add_holiday_aug_15(tr("Fête Nationale"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Toussaint"))

        if self._year >= 2010:
            # Republic Day.
            self._add_holiday_nov_28(tr("Jour de la République"))

        # Christmas Day.
        self._add_christmas_day(tr("Noël"))


class CG(Congo):
    pass


class COG(Congo):
    pass
