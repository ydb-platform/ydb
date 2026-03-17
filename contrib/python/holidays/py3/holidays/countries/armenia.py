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

from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Armenia(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Armenia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Armenia>
        * [Law HO-37 of Mar 16, 1996](https://web.archive.org/web/20260122214757/https://www.arlis.am/hy/acts/259)
        * [Law HO-117 of Dec 22, 2000](https://web.archive.org/web/20250717094812/https://www.arlis.am/hy/acts/801)
        * [Law HO-118 of Dec 22, 2000](https://web.archive.org/web/20250717090032/https://www.arlis.am/hy/acts/802)
        * [Law HO-200 of Jun 24, 2001](https://web.archive.org/web/20250427132425/http://www.parliament.am/legislation.php?sel=show&ID=1274&lang=arm)
        * [Law HO-228 of Oct 11, 2001](https://web.archive.org/web/20250427175053/http://www.parliament.am/legislation.php?ID=1622&lang=arm&sel=show)
        * [Law HO-335 of Jun 04, 2002](https://web.archive.org/web/20251210090018/http://www.parliament.am/legislation.php?sel=show&ID=1623&lang=arm)
        * [Law HO-466 of Dec 19, 2002](https://web.archive.org/web/20251209213043/http://www.parliament.am/legislation.php?sel=show&ID=1624&lang=arm)
        * [Law HO-80 of May 2, 2005](https://web.archive.org/web/20241211151029/http://www.parliament.am/legislation.php?sel=show&ID=2284&lang=arm)
        * [Law HO-105 of Jun 16, 2005](https://web.archive.org/web/20250508000307/http://www.parliament.am/legislation.php?lang=arm&sel=show&ID=2326)
        * [Law HO-100 of Jun 21, 2006](https://web.archive.org/web/20250428015807/http://www.parliament.am/legislation.php?ID=2648&lang=arm&sel=show)
        * [Law HO-221 of Dec 16, 2009](https://web.archive.org/web/20250501031654/http://www.parliament.am/legislation.php?ID=3737&lang=arm&sel=show)
        * [Law HO-134 of Oct 22, 2010](https://web.archive.org/web/20231201154954/http://www.parliament.am/legislation.php?sel=show&ID=3886&lang=arm)
        * [Law HO-127 of May 6, 2011](https://web.archive.org/web/20231201160900/http://www.parliament.am/legislation.php?sel=show&ID=4177&lang=arm)
        * [Law HO-142 of May 21, 2011](https://web.archive.org/web/20251210013331/http://www.parliament.am/legislation.php?sel=show&ID=4198&lang=arm)
        * [Law HO-15 of Apr 15, 2015](https://web.archive.org/web/20250510074755/http://parliament.am/legislation.php?ID=5221&lang=arm&sel=show)
        * [Law HO-187 of Dec 30, 2015](https://web.archive.org/web/20250510003259/http://parliament.am/legislation.php?sel=show&ID=5390&lang=arm)
        * [Law HO-67 of Mar 23, 2017](https://web.archive.org/web/20250428212817/http://www.parliament.am/legislation.php?sel=show&ID=5795&lang=arm)
        * [Law HO-14-N of Apr 19, 2019](https://web.archive.org/web/20231201160233/http://www.parliament.am/legislation.php?sel=show&ID=6551&lang=arm)
        * [Law HO-83-N of Jul 3, 2019](https://web.archive.org/web/20231201160446/http://www.parliament.am/legislation.php?sel=show&ID=6640&lang=arm)
        * [Law HO-191-N of Nov 13, 2019](https://web.archive.org/web/20231201163636/http://www.parliament.am/legislation.php?sel=show&ID=6751&lang=arm)
        * [Law HO-82-N of Mar 15, 2021](https://web.archive.org/web/20231201153032/http://www.parliament.am/legislation.php?sel=show&ID=7506&lang=arm)
        * [Law HO-191-N of May 18, 2021](https://web.archive.org/web/20250505174605/http://www.parliament.am/legislation.php?lang=arm&sel=show&ID=7622)
        * [Law HO-362-N of Nov 29, 2021](https://web.archive.org/web/20250427183056/http://www.parliament.am/legislation.php?sel=show&ID=7828&lang=arm)
        * [Law HO-147-N of May 2, 2024](https://web.archive.org/web/20250427204734/http://www.parliament.am/legislation.php?ID=9191&lang=arm&sel=show)
        * [Law HO-370-N of Nov 7, 2024](https://web.archive.org/web/20250505152132/http://www.parliament.am/legislation.php?sel=show&ID=9424&lang=arm)
        * [Law HO-1-N of Jan 20, 2026](https://web.archive.org/web/20260122152327/http://www.parliament.am/legislation.php?sel=show&ID=10146&lang=arm)
    """

    country = "AM"
    default_language = "hy"
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("en_US", "hy")
    start_year = 1991
    subdivisions = (
        "AG",  # Aragac̣otn.
        "AR",  # Ararat.
        "AV",  # Armavir.
        "ER",  # Erevan.
        "GR",  # Geġark'unik'.
        "KT",  # Kotayk'.
        "LO",  # Loṙi.
        "SH",  # Širak.
        "SU",  # Syunik'.
        "TV",  # Tavuš.
        "VD",  # Vayoć Jor.
    )
    subdivisions_aliases = {
        "Aragac̣otn": "AG",
        "Ararat": "AR",
        "Armavir": "AV",
        "Erevan": "ER",
        "Geġark'unik'": "GR",
        "Kotayk'": "KT",
        "Loṙi": "LO",
        "Širak": "SH",
        "Syunik'": "SU",
        "Tavuš": "TV",
        "Vayoć Jor": "VD",
    }

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, ArmeniaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("Նոր տարվա օր")
        self._add_new_years_day(name)
        self._add_new_years_day_two(name)

        # Christmas and Epiphany.
        self._add_holiday_jan_6(tr("Սուրբ Ծնունդ եւ Հայտնություն"))

        # Established by Law HO-221 of Dec 16, 2009.
        # Abolished by Law HO-362-N of Nov 29, 2021.
        if 2010 <= self._year <= 2021:
            # Christmas Holidays.
            name = tr("նախածննդյան տոներ")
            self._add_holiday_jan_3(name)
            self._add_holiday_jan_4(name)
            self._add_holiday_jan_5(name)

            # Day of Remembrance of the Dead.
            self._add_holiday_jan_7(tr("Մեռելոց հիշատակի օր"))

        # Established by Law HO-1-N of Jan 20, 2026.
        if self._year >= 2026:
            # Day of Remembrance for the Fallen Defenders of the Fatherland.
            self._add_holiday_jan_27(tr("Հայրենիքի պաշտպանության համար զոհվածների հիշատակի օր"))

        # Made non-working by Law HO-466 of Dec 19, 2002.
        if self._year >= 2003:
            # Army Day.
            self._add_holiday_jan_28(tr("Բանակի օր"))

        # Established by Law HO-118 of Dec 22, 2000.
        # Renamed by Law HO-200 of Jun 24, 2001.
        if self._year >= 2001:
            self._add_womens_day(
                # Women's Day.
                tr("Կանանց տոն")
                if self._year >= 2002
                # International Women's Day.
                else tr("Կանանց միջազգային օր")
            )

        # Made working by Law HO-200 of Jun 24, 2001.
        if 1994 <= self._year <= 2001:
            # Motherhood and Beauty Day.
            self._add_holiday_apr_7(tr("Մայրության և գեղեցկության տոն"))

        # Renamed by Law HO-15 of Apr 15, 2015.
        self._add_holiday_apr_24(
            # Day of Remembrance of the Victims of Armenian Genocide.
            tr("Հայոց ցեղասպանության զոհերի հիշատակի օր")
            if self._year >= 2015
            # Day of Remembrance of the Victims of Genocide.
            else tr("Ցեղասպանության զոհերի հիշատակի օր")
        )

        # Established by Law HO-117 of Dec 22, 2000.
        # Renamed by Law HO-200 of Jun 24, 2001.
        if self._year >= 2001:
            self._add_labor_day(
                # Labor Day.
                tr("Աշխատանքի օր")
                if self._year >= 2002
                # International Day of Workers' Solidarity.
                else tr("Աշխատավորների համերաշխության միջազգային օր")
            )

        if self._year >= 1995:
            self._add_world_war_two_victory_day(
                # Victory and Peace Day.
                tr("Հաղթանակի և Խաղաղության տոն"),
                is_western=False,
            )

        # Republic Day.
        self._add_holiday_may_28(tr("Հանրապետության օր"))

        if self._year >= 1996:
            # Constitution Day.
            self._add_holiday_jul_5(tr("Սահմանադրության օր"))

        if self._year >= 1992:
            # Independence Day.
            self._add_holiday_sep_21(tr("Անկախության օր"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Նոր տարվա գիշեր"))

    def _populate_workday_holidays(self):
        # Established by Law HO-200 of Jun 24, 2001.
        # Made non-working by Law HO-466 of Dec 19, 2002.
        if self._year == 2002:
            # Army Day.
            self._add_holiday_jan_28(tr("Բանակի օր"))

        # Established by Law HO-80 of May 2, 2005.
        if self._year >= 2006:
            # Mother Language Day.
            self._add_holiday_feb_21(tr("Մայրենի լեզվի օր"))

        # Established by Law HO-200 of Jun 24, 2001.
        if self._year >= 2002:
            # Saint Vardanants' Day.
            self._add_holiday_52_days_prior_easter(tr("Սուրբ Վարդանանց տոն"))

        # Established by Law HO-105 of Jun 16, 2005.
        if self._year >= 2006:
            self._add_holiday_feb_28(
                # Day of Remembrance of the Victims of the Massacres Organized in the Azerbaijan
                # SSR and the Protection of the Rights of the Deported Armenian Population.
                tr(
                    "Ադրբեջանական ԽՍՀ-ում կազմակերպված ջարդերի զոհերի հիշատակի եւ "
                    "բռնագաղթված հայ բնակչության իրավունքների պաշտպանության օր"
                )
            )

        # Made working by Law HO-200 of Jun 24, 2001.
        # Renamed by Law HO-335 of Jun 04, 2002.
        if self._year >= 2002:
            self._add_holiday_apr_7(
                # Motherhood and Beauty Day.
                tr("Մայրության և գեղեցկության տոն")
                if self._year >= 2003
                # Motherhood, Beauty and Love Day.
                else tr("Մայրության, գեղեցկության եւ սիրո տոն")
            )

        # Established by Law HO-83-N of Jul 3, 2019.
        if self._year >= 2020:
            # Armenian Cinema Day.
            self._add_holiday_apr_16(tr("Հայ կինոյի օր"))

        # Established by Law HO-191-N of May 18, 2021.
        if self._year >= 2022:
            # Taxpayer Day.
            self._add_holiday_apr_18(tr("Հարկ վճարողի օր"))

        # Established by Law HO-14-N of Apr 19, 2019.
        if self._year >= 2019:
            # Citizen of the Republic of Armenia Day.
            name = tr("Հայաստանի Հանրապետության քաղաքացու օր")
            if self._is_saturday(MAY, 1):
                self._add_holiday_last_sun_of_apr(name)
            else:
                self._add_holiday_last_sat_of_apr(name)

        # Established by Law HO-200 of Jun 24, 2001.
        if self._year >= 2002:
            # Defender's Day.
            self._add_holiday_may_8(tr("Երկրապահի օր"))

        # Established by Law HO-142 of May 21, 2011.
        if self._year >= 2012:
            # Family Day.
            self._add_holiday_may_15(tr("Ընտանիքի օր"))

        # Established by Law HO-187 of Dec 30, 2015.
        if self._year >= 2016:
            # Students and Youth Day.
            self._add_holiday_may_16(tr("Ուսանողների եւ երիտասարդների օր"))

        # Established by Law HO-200 of Jun 24, 2001.
        if self._year >= 2002:
            # Children's Rights Protection Day.
            self._add_childrens_day(tr("Երեխաների իրավունքների պաշտպանության օր"))

        # Established by Law HO-100 of Jun 21, 2006.
        if self._year >= 2007:
            # Day of Remembrance of the Repressed.
            self._add_holiday_jun_14(tr("Բռնադատվածների հիշատակի օր"))

        # Established by Law HO-370-N of Nov 7, 2024.
        if self._year >= 2025:
            # Father's Day.
            self._add_holiday_jun_17(tr("Հայրերի օր"))

        # Established by Law HO-228 of Oct 11, 2001.
        if self._year >= 2002:
            # Feast of Holy Etchmiadzin.
            self._add_holiday_63_days_past_easter(tr("Սուրբ Էջմիածնի տոն"))

        # Established by Law HO-67 of Mar 23, 2017.
        if self._year >= 2017:
            # State Symbols' Day.
            self._add_holiday_jul_5(tr("Պետական խորհրդանիշների օր"))

        # Established by Law HO-147-N of May 2, 2024.
        if self._year >= 2024:
            self._add_holiday_aug_3(
                # Day of Remembrance of the Victims of the Sinjar Yazidi Genocide of 2014.
                tr("2014 թվականի՝ Սինջարի եզդիների ցեղասպանության զոհերի հիշատակի օր")
            )

        # Established by Law HO-200 of Jun 24, 2001.
        # Renamed by Law HO-80 of May 2, 2005.
        if self._year >= 2001:
            self._add_holiday_sep_1(
                # Knowledge and Literacy Day.
                tr("Գիտելիքի եւ դպրության օր")
                if self._year >= 2005
                # Knowledge, Writing and Literacy Day.
                else tr("Գիտելիքի, գրի եւ դպրության օր")
            )

        # Established by Law HO-82-N of Mar 15, 2021.
        if self._year >= 2021:
            self._add_holiday_1st_sun_of_oct(
                # Day of National Minorities of the Republic of Armenia.
                tr("Հայաստանի Հանրապետության ազգային փոքրամասնությունների օր")
            )

        # Established by Law HO-134 of Oct 22, 2010.
        if self._year >= 2011:
            # Teacher's Day.
            self._add_holiday_oct_5(tr("Ուսուցչի օր"))

        # Established by Law HO-200 of Jun 24, 2001.
        if self._year >= 2001:
            # Translators' Day.
            self._add_holiday_2nd_sat_of_oct(tr("Թարգմանչաց տոն"))

        # Established by Law HO-127 of May 6, 2011.
        if self._year >= 2011:
            # Local Self-Government Day.
            self._add_holiday_nov_10(tr("Տեղական ինքնակառավարման օր"))

        # Established by Law HO-200 of Jun 24, 2001.
        # Renamed by Law HO-191-N of Nov 13, 2019.
        if self._year >= 2001:
            self._add_holiday_dec_7(
                # Earthquake Victims Remembrance Day and Disaster Resilience Day.
                tr("Երկրաշարժի զոհերի հիշատակի եւ աղետներին դիմակայունության օր")
                if self._year >= 2020
                # Earthquake Victims Remembrance Day.
                else tr("Երկրաշարժի զոհերի հիշատակի օր")
            )

        # Established by Law HO-15 of Apr 15, 2015.
        if self._year >= 2015:
            # Day of the Condemnation and Prevention of Genocides.
            self._add_holiday_dec_9(tr("Ցեղասպանությունների դատապարտման եւ կանխարգելման օր"))


class AM(Armenia):
    pass


class ARM(Armenia):
    pass


class ArmeniaStaticHolidays:
    """Armenia special holidays.

    Substituted holidays references:
        * [1998](https://www.arlis.am/hy/acts/6981)
        * [2000](https://www.arlis.am/hy/acts/7838)
        * [2003](https://www.arlis.am/hy/acts/10474)
        * 2005:
            * [1](https://www.arlis.am/hy/acts/13224)
            * [2](https://www.arlis.am/hy/acts/13452)
            * [3](https://www.arlis.am/hy/acts/13932)
        * 2006:
            * [1](https://www.arlis.am/hy/acts/21549)
            * [2](https://www.arlis.am/hy/acts/23593)
        * 2007:
            * [1](https://www.arlis.am/hy/acts/29581)
            * [2](https://www.arlis.am/hy/acts/34755)
        * 2008:
            * [1](https://www.arlis.am/hy/acts/40452)
            * [2](https://www.arlis.am/hy/acts/42730)
            * [3](https://www.arlis.am/hy/acts/44770)
            * [4](https://www.arlis.am/hy/acts/45880)
            * [5](https://www.arlis.am/hy/acts/46224)
        * 2009:
            * [1](https://www.arlis.am/hy/acts/48427)
            * [2](https://www.arlis.am/hy/acts/48654)
            * [3](https://www.arlis.am/hy/acts/50635)
        * 2010:
            * [1](https://www.arlis.am/hy/acts/55456)
            * [2](https://www.arlis.am/hy/acts/55457)
            * [3](https://www.arlis.am/hy/acts/60567)
        * 2011:
            * [1](https://www.arlis.am/hy/acts/60996)
            * [2](https://www.arlis.am/hy/acts/65969)
            * [3](https://www.arlis.am/hy/acts/69096)
        * 2012:
            * [1](https://www.arlis.am/hy/acts/60996)
            * [2](https://www.arlis.am/hy/acts/74278)
            * [3](https://www.arlis.am/hy/acts/75441)
            * [4](https://www.arlis.am/hy/acts/76805)
        * 2013:
            * [1](https://www.arlis.am/hy/acts/80097)
            * [2](https://www.arlis.am/hy/acts/82971)
            * [3](https://www.arlis.am/hy/acts/83282)
        * [2013-2014](https://www.arlis.am/hy/acts/86936)
        * 2015:
            * [1](https://www.arlis.am/hy/acts/94257)
            * [2](https://www.arlis.am/hy/acts/96650)
            * [3](https://www.arlis.am/hy/acts/96794)
            * [4](https://www.arlis.am/hy/acts/98926)
            * [5](https://www.arlis.am/hy/acts/99539)
        * 2016:
            * [1](https://www.arlis.am/hy/acts/103890)
            * [2](https://www.arlis.am/hy/acts/104324)
            * [3](https://www.arlis.am/hy/acts/108088)
        * 2017:
            * [1](https://www.arlis.am/hy/acts/112733)
            * [2](https://www.arlis.am/hy/acts/112882)
            * [3](https://www.arlis.am/hy/acts/115584)
        * 2018:
            * [1](https://www.arlis.am/hy/acts/119427)
            * [2](https://www.arlis.am/hy/acts/120476)
            * [3](https://www.arlis.am/hy/acts/121651)
            * [4](https://www.arlis.am/hy/acts/125762)
            * [5](https://www.arlis.am/hy/acts/126944)
        * 2020:
            * [1](https://www.arlis.am/hy/acts/138403)
            * [2](https://www.arlis.am/hy/acts/139280)
        * [2021](https://www.arlis.am/hy/acts/155821)
    """

    # Substituted date format.
    substituted_date_format = tr("%d.%m.%Y")
    # Day off (substituted from %s).
    substituted_label = tr("Հանգստյան օր (հետաձգվել է %s թվականից)")

    special_public_holidays = {
        1998: (DEC, 7, DEC, 12),
        2000: (MAY, 8, MAY, 6),
        2003: (JAN, 3, DEC, 29, 2002),
        2005: (
            (JAN, 3, JAN, 8),
            (MAR, 7, MAR, 5),
            (JUL, 4, JUL, 2),
        ),
        2006: (
            (JAN, 3, JAN, 8),
            (MAY, 8, MAY, 6),
        ),
        2007: (
            (JAN, 3, DEC, 30, 2006),
            (JAN, 4, JAN, 7),
            (APR, 30, APR, 28),
        ),
        2008: (
            (JAN, 3, DEC, 29, 2007),
            (JAN, 4, JAN, 12),
            (MAY, 2, MAY, 4),
            (JUN, 30, JUN, 28),
            (AUG, 18, AUG, 16),
            (SEP, 15, SEP, 13),
        ),
        2009: (
            (JAN, 5, JAN, 10),
            (JAN, 7, DEC, 27, 2008),
            (APR, 13, APR, 18),
            (JUL, 20, JUL, 18),
            (AUG, 17, AUG, 15),
            (SEP, 14, SEP, 12),
        ),
        2010: (
            (JAN, 8, DEC, 26, 2009),
            (APR, 5, APR, 10),
            (JUL, 12, JUL, 17),
            (AUG, 16, AUG, 21),
            (SEP, 13, SEP, 18),
            (SEP, 20, SEP, 11),
        ),
        2011: (
            (MAR, 7, MAR, 12),
            (APR, 25, APR, 30),
            (JUL, 4, JUL, 9),
            (AUG, 1, AUG, 6),
            (AUG, 15, AUG, 20),
            (SEP, 12, SEP, 17),
        ),
        2012: (
            (MAR, 9, MAR, 24),
            (APR, 9, APR, 14),
            (APR, 30, APR, 28),
            (JUL, 6, JUL, 14),
            (JUL, 16, JUL, 21),
            (AUG, 13, AUG, 18),
            (SEP, 17, SEP, 29),
        ),
        2013: (
            (APR, 1, APR, 13),
            (MAY, 10, MAY, 18),
            (MAY, 27, JUN, 1),
            (JUL, 8, JUL, 13),
            (AUG, 19, AUG, 24),
            (SEP, 16, SEP, 28),
            (DEC, 30, DEC, 28),
        ),
        2014: (
            (JAN, 27, FEB, 1),
            (APR, 21, APR, 26),
            (MAY, 2, MAY, 31),
            (JUL, 28, AUG, 2),
            (AUG, 18, AUG, 23),
            (SEP, 15, SEP, 27),
        ),
        2015: (
            (JAN, 8, DEC, 20, 2014),
            (JAN, 9, DEC, 27, 2014),
            (APR, 6, APR, 11),
            (APR, 23, APR, 18),
            (JUL, 13, JUL, 18),
            (SEP, 14, SEP, 19),
        ),
        2016: (
            (MAR, 7, MAR, 12),
            (MAR, 28, APR, 2),
            (SEP, 12, SEP, 24),
        ),
        2017: (
            (APR, 17, MAY, 6),
            (MAY, 8, MAY, 20),
            (SEP, 18, SEP, 23),
        ),
        2018: (
            (MAR, 9, MAR, 17),
            (APR, 2, APR, 7),
            (APR, 30, MAY, 5),
            (OCT, 11, OCT, 27),
            (OCT, 12, NOV, 3),
        ),
        2020: (
            (JAN, 27, FEB, 1),
            (MAY, 29, MAY, 23),
        ),
        2021: (SEP, 20, SEP, 25),
    }

    special_lo_public_holidays = {
        2018: (DEC, 7, DEC, 15),
    }

    special_sh_public_holidays = {
        2018: (DEC, 7, DEC, 15),
    }
