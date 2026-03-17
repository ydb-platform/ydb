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
from holidays.groups import InternationalHolidays, MongolianCalendarHolidays
from holidays.holiday_base import HolidayBase


class Mongolia(HolidayBase, InternationalHolidays, MongolianCalendarHolidays):
    """Mongolia holidays.

    References:
        * [Law on Public holidays and days of observation](https://web.archive.org/web/20250327062440/https://legalinfo.mn/mn/detail/399)
        * [Labor Law](https://web.archive.org/web/20250421093230/https://legalinfo.mn/mn/detail?lawId=16230709635751)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Mongolia>
        * [Mongolian lunar calendar](https://web.archive.org/web/20230412171012/https://www.math.mcgill.ca/gantumur/cal/index_mn.html)
        * <https://web.archive.org/web/20250613054645/https://www.timeanddate.com/holidays/mongolia/>
        * <https://web.archive.org/web/20250429110535/https://investmongolia.gov.mn/mongolia-at-a-glance/>
    """

    country = "MN"
    default_language = "mn"
    # %s (estimated).
    estimated_label = tr("%s (урьдчилсан)")
    start_year = 2004
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("en_US", "mn")

    def __init__(self, *args, **kwargs):
        InternationalHolidays.__init__(self)
        MongolianCalendarHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Шинэ жил"))

        # Lunar New Year.
        name = tr("Цагаан сар")
        self._add_tsagaan_sar(name)
        self._add_tsagaan_sar_day_2(name)
        # Expanded on November 28th, 2013.
        if self._year >= 2014:
            self._add_tsagaan_sar_day_3(name)

        # International Women's Day.
        self._add_womens_day(tr("Олон улсын эмэгтэйчүүдийн өдөр"))

        # Children's Day.
        self._add_childrens_day(tr("Хүүхдийн баяр"))

        # Established on December 20th, 2019.
        if self._year >= 2020:
            # The Buddha's Birthday.
            self._add_buddha_day(tr("Бурхан багшийн Их дүйчин өдөр"))

        # National Festival and People's Revolution Anniversary.
        name = tr("Үндэсний их баяр наадам, Ардын хувьсгалын ойн баяр")
        # Expanded to July 10 on July 7th, 2023.
        if self._year >= 2023:
            self._add_holiday_jul_10(name)
        self._add_holiday_jul_11(name)
        self._add_holiday_jul_12(name)
        self._add_holiday_jul_13(name)
        # Expanded to July 14-15 on July 1st, 2014.
        if self._year >= 2014:
            self._add_holiday_jul_14(name)
            self._add_holiday_jul_15(name)

        # Established on November 8th, 2012.
        if self._year >= 2012:
            # Genghis Khan's Birthday.
            self._add_genghis_khan_day(tr("Их Эзэн Чингис хааны өдөр"))

        # Repealed on November 8th, 2012.
        # Re-established again on November 18th, 2016.
        if self._year <= 2011 or self._year >= 2016:
            self._add_holiday_nov_26(
                # Republic Day.
                tr("Бүгд Найрамдах Улс тунхагласан өдөр")
                if self._year >= 2016
                # Republic Holiday.
                else tr("Бүгд Найрамдах Улс тунхагласны баяр")
            )

        # Established on August 16th, 2007.
        # Renamed and considered a Public Holiday on December 23rd, 2011.
        if self._year >= 2011:
            self._add_holiday_dec_29(
                # National Freedom and Independence Day.
                tr("Үндэсний эрх чөлөө, тусгаар тогтнолоо сэргээсний баярын өдөр")
            )

    def _populate_workday_holidays(self):
        """
        NOTE:
        The following observances are currently unimplemented
        due to unclear week-based scheduling rules:

        1. National Literary, Cultural and Book Days (Үндэсний бичиг соёл, номын өдрүүд):
        - Observed on Saturday and Sunday of the 3rd week of May and September.
        - Officially added on July 2nd, 2021.

        2. Environmental Protection Days (Байгаль орчныг хамгаалах өдрүүд):
        - Observed during the 4th week of September.

        These are defined using "week of the month" logic, but it is unclear whether weeks start
        from the 1st of the month or the first full week. We previously contacted
        contact@mecc.gov.mn for clarification but received no response.

        See:
        https://github.com/vacanza/holidays/issues/2672
        """

        # Constitution Day.
        self._add_holiday_jan_13(tr("Монгол Улсын Үндсэн хуулийн өдөр"))

        # Established on December 9th, 2004.
        if self._year >= 2005:
            # Patriots' Day.
            self._add_holiday_mar_1(tr("Эх орончдын өдөр"))

        # Renamed on February 9th, 2011.
        self._add_holiday_mar_18(
            # Military Day.
            tr("Монгол цэргийн өдөр")
            if self._year >= 2011
            # Armed Forces Day.
            else tr("Зэвсэгт хүчний өдөр")
        )

        # Health Protection Day.
        self._add_holiday_apr_7(tr("Эрүүл мэндийг хамгаалах өдөр"))

        # Intellectual Property Protection Day.
        self._add_holiday_apr_26(tr("Оюуны өмчийг хамгаалах өдөр"))

        # Family Day.
        self._add_holiday_may_15(tr("Гэр бүлийн өдөр"))

        # Established on June 11th, 2009.
        if self._year >= 2009:
            # National Flag Day.
            self._add_holiday_jul_10(tr("Монгол Улсын төрийн далбааны өдөр"))

        # Youth Day.
        self._add_holiday_aug_25(tr("Залуучуудын өдөр"))

        if self._year >= 2007:
            # New Harvest Days.
            self._add_multiday_holiday(self._add_holiday_sep_5(tr("Шинэ ургацын өдрүүд")), 45)

        # Memorial Day of Political Victims.
        self._add_holiday_sep_10(tr("Улс төрийн хэлмэгдэгсдийн дурсгалын өдөр"))

        # Elders' Day.
        self._add_holiday_oct_1(tr("Ахмадын өдөр"))

        # Renamed on July 7th, 2021.
        self._add_holiday_oct_29(
            # Capital City Day.
            tr("Монгол Улсын нийслэл хотын өдөр")
            if self._year >= 2021
            # Capital Day.
            else tr("Монгол Улсын Нийслэлийн өдөр")
        )

        # Established on November 8th, 2012 after the old one was repealed.
        # Made a Public Holiday again on November 8th, 2016.
        if 2012 <= self._year <= 2015:
            # Republic Day.
            self._add_holiday_nov_26(tr("Бүгд Найрамдах Улс тунхагласан өдөр"))

        # Democracy and Human Rights Day.
        self._add_holiday_dec_10(tr("Ардчилал, хүний эрхийн өдөр"))

        # Established on August 16th, 2007.
        # Renamed and considered a Public Holiday on December 23rd, 2011.
        if 2007 <= self._year <= 2010:
            # National Freedom Day.
            self._add_holiday_dec_29(tr("Үндэсний эрх чөлөөний өдөр"))


class MN(Mongolia):
    pass


class MNG(Mongolia):
    pass
