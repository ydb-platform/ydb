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

from holidays.constants import BANK, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class SanMarino(HolidayBase, ChristianHolidays, InternationalHolidays):
    """San Marino holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_San_Marino>
        * [Bank holiday calendar (Italian)](https://web.archive.org/web/20250627182751/https://www.bcsm.sm/funzioni/funzioni-statutarie/sistema-dei-pagamenti/calendario-festività)
        * [Bank holiday calendar (English)](https://web.archive.org/web/20250627182741/https://www.bcsm.sm/en/functions/statutory-functions/payment-system/bank-holiday-calendar)
        * [Law No. 7 of Feb. 17, 1961](https://web.archive.org/web/20250122105136/https://www.consigliograndeegenerale.sm/on-line/home/archivio-leggi-decreti-e-regolamenti/documento17018406.html)
    """

    country = "SM"
    default_language = "it"
    # Law No. 7 of Feb. 17, 1961.
    start_year = 1962
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("en_US", "it", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self) -> None:
        # New Year's Day.
        self._add_new_years_day(tr("Capodanno"))

        # Epiphany.
        self._add_epiphany_day(tr("Epifania"))

        self._add_holiday_feb_5(
            # Anniversary of the Liberation of the Republic and Feast of Saint Agatha.
            tr("Anniversario della Liberazione della Repubblica e Festa di Sant'Agata")
        )

        # Anniversary of the Arengo.
        self._add_holiday_mar_25(tr("Anniversario dell'Arengo"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Pasqua"))

        # Easter Monday.
        self._add_easter_monday(tr("Lunedì dell'angelo"))

        # Investiture of Captains Regent.
        name = tr("Investitura Capitani Reggenti")
        self._add_holiday_apr_1(name)
        self._add_holiday_oct_1(name)

        # Workers' Day.
        self._add_labor_day(tr("Festa dei lavoratori"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Corpus Domini"))

        self._add_holiday_jul_28(
            # Anniversary of the Fall of Fascism and Freedom Day.
            tr("Anniversario della Caduta del Fascismo e Festa della Libertà")
        )

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assunzione della B.V. Maria"))

        self._add_holiday_sep_3(
            # Saint Marinus' Day, Anniversary of the Founding of the Republic.
            tr("San Marino, Anniversario di Fondazione della Repubblica")
        )

        # All Saints' Day.
        self._add_all_saints_day(tr("Tutti i Santi"))

        # Commemoration of the Dead.
        self._add_all_souls_day(tr("Commemorazione dei defunti"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Immacolata Concezione"))

        # Christmas Day.
        self._add_christmas_day(tr("Natale"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Santo Stefano"))

    def _populate_bank_holidays(self):
        # Christmas Eve.
        self._add_christmas_eve(tr("Vigilia di Natale"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Ultimo dell'anno"))


class SM(SanMarino):
    pass


class SMR(SanMarino):
    pass
