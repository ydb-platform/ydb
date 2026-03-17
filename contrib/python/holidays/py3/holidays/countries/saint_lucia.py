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
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class SaintLucia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Saint Lucia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Saint_Lucia>
        * <https://web.archive.org/web/20250415223705/https://www.timeanddate.com/holidays/saint-lucia/>
        * <https://web.archive.org/web/20241201164546/https://archive.stlucia.gov.lc/saint_lucia/public_holidays.htm>
        * <https://web.archive.org/web/20220227084500/https://archive.stlucia.gov.lc/stluciasilver/national_holidays.htm>
        * <https://web.archive.org/web/20160314100648/http://www.stluciachamber.org/uploadedImages/contentImg/file/List%20of%20Holidays%20for%202015%20(1).pdf>
    """

    country = "LC"
    default_language = "en_LC"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_LC", "en_US")
    start_year = 1979

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")), rule=SUN_TO_NEXT_TUE)

        # New Year's Holiday.
        self._add_observed(self._add_new_years_day_two(tr("New Year's Holiday")))

        # Independence Day.
        self._add_observed(self._add_holiday_feb_22(tr("Independence Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Labor Day.
        self._add_observed(self._add_labor_day(tr("Labour Day")))

        # Whit Monday.
        self._add_whit_monday(tr("Whit Monday"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Corpus Christi"))

        # Emancipation Day.
        self._add_observed(self._add_holiday_aug_1(tr("Emancipation Day")))

        # Thanksgiving Day.
        self._add_holiday_1st_mon_of_oct(tr("Thanksgiving Day"))

        # National Day.
        self._add_observed(self._add_holiday_dec_13(tr("National Day")))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Christmas Day")), rule=SUN_TO_NEXT_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two(tr("Boxing Day")))


class LC(SaintLucia):
    pass


class LCA(SaintLucia):
    pass
