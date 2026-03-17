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

from holidays.constants import HALF_DAY, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class FaroeIslands(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Faroe Islands holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Faroe_Islands>
        * [Decree no. 52 of 23 April 2003](https://web.archive.org/web/20241204233745/https://logir.fo/Kunngerd/52-fra-23-04-2003-um-flagging-fra-landsins-bygningum)
        * [2007](https://web.archive.org/web/20250125193236/https://www.framtak.com/info/holidays.html))
        * [2024](https://web.archive.org/web/20250615193008/https://visitfaroeislands.com/en/plan-your-stay/get-ready-for-your-trip/general-facts/public-holidays)
        * [2025](https://web.archive.org/web/20250615193215/https://guidetofaroeislands.fo/travel-information/faroe-islands-holiday/)
    """

    country = "FO"
    default_language = "fo"
    # Denmark granted home rule to the Faroe Islands on 30 March 1948.
    start_year = 1949
    supported_categories = (HALF_DAY, PUBLIC)
    supported_languages = ("da", "en_US", "fo", "is", "no", "sv")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nýggjársdagur"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Skírhósdagur"))

        # Good Friday.
        self._add_good_friday(tr("Langifríggjadagur"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Páskadagur"))

        # Easter Monday.
        self._add_easter_monday(tr("Annar páskadagur"))

        # Great Prayer Day.
        self._add_holiday_26_days_past_easter(tr("Dýri biðidagur"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Kristi himmalsferðardagur"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Hvítusunnudagur"))

        # Whit Monday.
        self._add_whit_monday(tr("Annar hvítusunnudagur"))

        # Saint Olaf's Day.
        self._add_holiday_jul_29(tr("Ólavsøkudagur"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Jólaaftan"))

        # Christmas Day.
        self._add_christmas_day(tr("Jóladagur"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Annar jóladagur"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Nýggjársaftan"))

    def _populate_half_day_holidays(self):
        # National Flag Day.
        self._add_holiday_apr_25(tr("Flaggdagur"))

        # Constitution Day.
        self._add_holiday_jun_5(tr("Grundlógardagur"))

        # Saint Olaf's Eve.
        self._add_holiday_jul_28(tr("Ólavsøkuaftan"))


class FO(FaroeIslands):
    pass


class FRO(FaroeIslands):
    pass
