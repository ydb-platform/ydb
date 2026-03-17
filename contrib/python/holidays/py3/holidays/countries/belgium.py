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


class Belgium(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Belgium holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Belgium>
        * <https://web.archive.org/web/20250331001402/https://www.belgium.be/nl/over_belgie/land/belgie_in_een_notendop/feestdagen>
        * <https://nl.wikipedia.org/wiki/Feestdagen_in_België>
        * <https://web.archive.org/web/20240816004739/https://www.nbb.be/en/about-national-bank/national-bank-belgium/public-holidays>
    """

    country = "BE"
    default_language = "nl"
    supported_categories = (BANK, PUBLIC)
    supported_languages = ("de", "en_US", "fr", "nl", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nieuwjaar"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Pasen"))

        # Easter Monday.
        self._add_easter_monday(tr("Paasmaandag"))

        # Labor Day.
        self._add_labor_day(tr("Dag van de Arbeid"))

        # Ascension Day.
        self._add_ascension_thursday(tr("O. L. H. Hemelvaart"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Pinksteren"))

        # Whit Monday.
        self._add_whit_monday(tr("Pinkstermaandag"))

        # National Day.
        self._add_holiday_jul_21(tr("Nationale feestdag"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("O. L. V. Hemelvaart"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Armistice Day.
        self._add_remembrance_day(tr("Wapenstilstand"))

        # Christmas Day.
        self._add_christmas_day(tr("Kerstmis"))

    def _populate_bank_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Goede Vrijdag"))

        # Friday after Ascension Day.
        self._add_holiday_40_days_past_easter(tr("Vrijdag na O. L. H. Hemelvaart"))

        # Bank Holiday.
        self._add_christmas_day_two(tr("Banksluitingsdag"))


class BE(Belgium):
    pass


class BEL(Belgium):
    pass
