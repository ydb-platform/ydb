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
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_MON_TUE,
)


class CookIslands(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Cook Islands holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Cook_Islands>
        * [Public Holidays Amendment Act 1970-71](https://web.archive.org/web/20250530101809/https://cookislandslaws.gov.ck/#/Laws-as-Made?id=414&name=Public%20Holidays%20Amendment%20Act%201970-71%20No%2029)
        * [Public Holidays Act 1999 (Law as Made)](https://web.archive.org/web/20250530120928/https://cookislandslaws.gov.ck/#/Laws-as-Made?id=1241&name=Public%20Holidays%20Act%201999%20No%2017)
        * [Public Holidays (Aitutaki Gospel Day) Order 1989](https://web.archive.org/web/20250530121249/https://parliament.gov.ck/wp-content/uploads/2022/07/Public-Holidays-Aitutaki-Gospel-Day-Order-1990-No.-9.pdf)
        * [Public Holidays Act 1999 (Consolidated Law)](https://web.archive.org/web/20250530101809/https://cookislandslaws.gov.ck/#/InternalConsolidatedLaws?legalId=LOCI.PHA99&parent_id=Public%20Holidays%20Act%201999)
        * [Gospel Days](https://web.archive.org/web/20110928152006/http://www.cookislands.org.uk/gospelday.html)
        * [Public Holidays (Ra o te Ui Ariki) Amendment Act 2011](https://web.archive.org/web/20250530101809/https://cookislandslaws.gov.ck/#/Laws-as-Made?id=1553&name=Public%20Holidays%20(Ra%20o%20te%20Ui%20Ariki)%20Amendment%20Act%202011%20No%204)
        * [Public Holidays (Ra o te Ui Ariki) Amendment 2013](https://web.archive.org/web/20250530122050/https://parliament.gov.ck/wp-content/uploads/2020/02/Public-Holidays-Ra-o-te-Ui-Ariki-No.-4.pdf)
        * [2004](https://web.archive.org/web/20090326004400/http://www.cook-islands.gov.ck/view_release.php?release_id=429)
        * [2008](https://web.archive.org/web/20090326003301/http://www.stats.gov.ck/NewsEvents/PublicHolidays.pdf)
    """

    country = "CK"
    default_language = "en_CK"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_CK", "en_US")
    # Public Holiday Act 1999.
    start_year = 2000

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        self._add_observed(
            # New Year's Day.
            self._add_new_years_day(tr("New Year's Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Day after New Year's Day.
            self._add_new_years_day_two(tr("Day after New Year's Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        # Anzac Day.
        self._add_anzac_day(tr("Anzac Day"))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Sovereign's Birthday.
        self._add_holiday_1st_mon_of_jun(tr("Sovereign's Birthday"))

        if self._year >= 2012:
            # Day of the House of Ariki.
            name = tr("Ra o te Ui Ariki")
            if self._year >= 2013:
                self._add_holiday_1st_fri_of_jul(name)
            else:
                self._add_holiday_jun_6(name)

        # Constitution Day.
        self._add_observed(self._add_holiday_aug_4(tr("Constitution Day")))

        # Cook Islands Gospel Day.
        self._add_observed(self._add_holiday_oct_26(tr("Cook Islands Gospel Day")))

        self._add_observed(
            # Christmas Day.
            self._add_christmas_day(tr("Christmas Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        self._add_observed(
            # Boxing Day.
            self._add_christmas_day_two(tr("Boxing Day")),
            rule=SAT_SUN_TO_NEXT_MON_TUE,
        )

        if self._year <= 2011:
            # Penrhyn Gospel Day.
            self._add_observed(self._add_holiday_mar_13(tr("Penrhyn Gospel Day")))

            # Palmerston Gospel Day.
            self._add_observed(self._add_holiday_may_25(tr("Palmerston Gospel Day")))

            # Mangaia Gospel Day.
            self._add_observed(self._add_holiday_jun_15(tr("Mangaia Gospel Day")))

            # Atiu Gospel Day.
            self._add_observed(self._add_holiday_jul_20(tr("Atiu Gospel Day")))

            # Mitiaro Gospel Day.
            self._add_observed(self._add_holiday_jul_21(tr("Mitiaro Gospel Day")))

            self._add_observed(
                # Mauke Gospel Day.
                self._add_holiday_jul_23(tr("Mauke Gospel Day")),
                rule=SAT_TO_NEXT_TUE if self._year in {2005, 2011} else None,
            )

            # Rarotonga Gospel Day.
            self._add_observed(self._add_holiday_jul_25(tr("Rarotonga Gospel Day")))

            # Manihiki Gospel Day.
            self._add_observed(self._add_holiday_aug_8(tr("Manihiki Gospel Day")))

            # Rakahanga Gospel Day.
            self._add_observed(self._add_holiday_aug_15(tr("Rakahanga Gospel Day")))

            # Aitutaki Gospel Day.
            self._add_observed(self._add_holiday_oct_27(tr("Aitutaki Gospel Day")))

            # Pukapuka Gospel Day.
            self._add_observed(self._add_holiday_dec_8(tr("Pukapuka Gospel Day")))


class CK(CookIslands):
    pass


class COK(CookIslands):
    pass
