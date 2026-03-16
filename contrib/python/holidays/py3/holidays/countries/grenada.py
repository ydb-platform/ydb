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

from holidays.calendars.gregorian import FEB, MAR, JUN, JUL, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON


class Grenada(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Grenada holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Grenada>
        * [Bank Holidays Act](https://web.archive.org/web/20250504095840/https://laws.gov.gd/index.php/chapters/b-23-39/1042-cap25-bank-holidays-act-2/viewdocument/1042)
        * <https://web.archive.org/web/20250213100825/https://www.timeanddate.com/holidays/grenada/>
        * [National Heroes' Day](https://web.archive.org/web/20250504095924/https://www.gov.gd/component/edocman/grenadas-first-national-heroes-day-and-october-19th-commemoration-events/viewdocument/576)
        * [Emancipation Day](https://web.archive.org/web/20250504100015/https://nowgrenada.com/2025/01/gnrc-chairman-supports-1-august-as-national-holiday/)

    Checked With:
        * [2010](https://web.archive.org/web/20100108053101/http://www.gov.gd/holiday_events.html)
        * [2011](https://web.archive.org/web/20111007094915/http://www.gov.gd/holiday_events.html)
        * [2012](https://web.archive.org/web/20120623100105/http://www.gov.gd/holiday_events.html)
        * [2013](https://web.archive.org/web/20130805181426/http://www.gov.gd/holiday_events.html)
        * [2014](https://web.archive.org/web/20140424191452/http://www.gov.gd/holiday_events.html)
        * [2015](https://web.archive.org/web/20150701045051/http://www.gov.gd/holiday_events.html)
        * [2016](https://web.archive.org/web/20160518030722/http://www.gov.gd/holiday_events.html)
        * [2017](https://web.archive.org/web/20170505034606/http://www.gov.gd/holiday_events.html)
        * [2018](https://web.archive.org/web/20180424025139/http://www.gov.gd/holiday_events.html)
        * [2023](https://web.archive.org/web/20230329094946/https://www.gov.gd/pdf/Public%20Holidays%202023.pdf)
        * [2025](https://web.archive.org/web/20250130143152/https://www.gov.gd/component/edocman/grenadas-public-holidays-2025/viewdocument/1386)
    """

    country = "GD"
    default_language = "en_GD"
    # %s (observed).
    observed_label = tr("%s (observed)")
    # First available revision of Bank Holidays Act.
    start_year = 2000
    supported_languages = ("en_GD", "en_US")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, GrenadaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Independence Day.
        self._add_observed(self._add_holiday_feb_7(tr("Independence Day")))

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
        name = tr("Emancipation Day")
        if self._year >= 2025:
            self._add_observed(self._add_holiday_aug_1(name))
        else:
            self._add_holiday_1st_mon_of_aug(name)

        # Carnival Monday.
        self._add_holiday_2nd_mon_of_aug(tr("Carnival Monday"))

        # Carnival Tuesday.
        self._add_holiday_1_day_past_2nd_mon_of_aug(tr("Carnival Tuesday"))

        if self._year >= 2023:
            # National Heroes' Day.
            self._add_observed(self._add_holiday_oct_19(tr("National Heroes' Day")))

        # Thanksgiving Day.
        self._add_observed(self._add_holiday_oct_25(tr("Thanksgiving Day")))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Christmas Day")))

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two(tr("Boxing Day")))


class GD(Grenada):
    pass


class GRD(Grenada):
    pass


class GrenadaStaticHolidays:
    """Grenada special holidays.

    References:
        * [2011](https://web.archive.org/web/20250504100149/https://laws.gov.gd/index.php/s-r-o/37-sr-o-35-of-2011-bank-holiday/viewdocument/37)
        * [2013](https://web.archive.org/web/20250504100219/https://laws.gov.gd/index.php/s-r-o/93-sro-6-of-2013-bank-holiday/viewdocument/93)
        * [2016](https://web.archive.org/web/20250504100408/https://laws.gov.gd/index.php/s-r-o/289-sr-o-61-of-2016-bank-holiday/viewdocument/289)
        * [2018](https://web.archive.org/web/20250504100436/https://laws.gov.gd/index.php/s-r-o/348-sr-o-10-of-2018-bank-holiday-proclamation/viewdocument/348)
        * [24 June 2022](https://web.archive.org/web/20250504100516/https://laws.gov.gd/index.php/s-r-o/576-sr-o-33-of-2022-bank-holiday-proclamation-2022/viewdocument/576)
        * [27 December 2022](https://web.archive.org/web/20250504100538/https://laws.gov.gd/index.php/s-r-o/586-sr-o-43-of-2022-bank-holiday-proclamation-2022/viewdocument/586)
        * [CARICOM's 50th Anniversary 2023](https://web.archive.org/web/20250504100603/https://laws.gov.gd/index.php/s-r-o/612-sr-o-22-of-2023-bank-holiday-proclamation-2023/viewdocument/612)
    """

    # Bank Holiday.
    bank_holiday = tr("Bank Holiday")

    special_public_holidays = {
        2011: (DEC, 27, bank_holiday),
        2013: (FEB, 22, bank_holiday),
        2016: (DEC, 27, bank_holiday),
        2018: (MAR, 14, bank_holiday),
        2022: (
            (JUN, 24, bank_holiday),
            (DEC, 27, bank_holiday),
        ),
        # CARICOM's 50th Anniversary.
        2023: (JUL, 4, tr("CARICOM's 50th Anniversary")),
    }
