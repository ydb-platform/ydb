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


class Greenland(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Greenland holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Greenland>
        * [Greenlandic names source](https://web.archive.org/web/20241214072442/https://nalunaarutit.gl/groenlandsk-lovgivning/2008/bkg-26-2008?sc_lang=kl-GL)
        * [Translation source](https://web.archive.org/web/20250422014548/https://www.norden.org/en/info-norden/public-holidays-greenland)
    """

    country = "GL"
    default_language = "kl"
    supported_categories = (OPTIONAL, PUBLIC)
    supported_languages = ("da", "en_US", "fi", "is", "kl", "no", "sv", "uk")
    # Greenland Home Rule Act 1978.
    start_year = 1979

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Ukiortaaq"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Sisamanngortoq illernartoq"))

        # Good Friday.
        self._add_good_friday(tr("Tallimanngorneq tannaartoq"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Poorskip ullua"))

        # Easter Monday.
        self._add_easter_monday(tr("Poorskip-aappaa"))

        # Great Prayer Day.
        self._add_holiday_26_days_past_easter(tr("Ulloq qinuffiusoq"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Qilaliarfik"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Piinsip ullua"))

        # Whit Monday.
        self._add_whit_monday(tr("Piinsip-aappaa"))

        # Christmas Day.
        self._add_christmas_day(tr("Juullip ullua"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Juullip-aappaa"))

    def _populate_optional_holidays(self):
        # Epiphany.
        self._add_epiphany_day(tr("Kunngit pingasut ulluat"))

        # International Workers' Day.
        self._add_labor_day(tr("Sulisartut ulluat"))

        if self._year >= 1983:
            # National Day.
            self._add_holiday_jun_21(tr("Ullortuneq"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Juulliaraq"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Ukiortaami"))


class GL(Greenland):
    pass


class GRL(Greenland):
    pass
