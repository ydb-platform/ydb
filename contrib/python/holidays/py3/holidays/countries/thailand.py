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

from datetime import date
from gettext import gettext as tr

from holidays.calendars.gregorian import (
    JAN,
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
    SAT,
    SUN,
    _timedelta,
)
from holidays.constants import ARMED_FORCES, BANK, GOVERNMENT, PUBLIC, SCHOOL, WORKDAY
from holidays.groups import InternationalHolidays, StaticHolidays, ThaiCalendarHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    SAT_TO_NEXT_MON,
    THU_FRI_TO_NEXT_WORKDAY,
    SAT_SUN_TO_NEXT_MON,
    SAT_SUN_TO_NEXT_TUE,
    SAT_SUN_TO_NEXT_MON_TUE,
    SAT_SUN_TO_NEXT_WORKDAY,
)


class Thailand(ObservedHolidayBase, InternationalHolidays, StaticHolidays, ThaiCalendarHolidays):
    """Thailand holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Thailand>
        * [Holidays Act (MAR 1914)](https://web.archive.org/web/20231021231154/https://ratchakitcha.soc.go.th/documents/1044125.pdf)
        * [Special Extension for MoJ (In-effect 1915 onwards)](https://web.archive.org/web/20250428123805/https://ratchakitcha.soc.go.th/documents/1046008.pdf)
        * [Holidays Act Amendment (MAR 1926)](https://web.archive.org/web/20231021231328/https://ratchakitcha.soc.go.th/documents/1073133.pdf)
        * [Ascension of HM King Ananda Mahidol (MAR 1935)](https://web.archive.org/web/20250215164432/https://th.wikisource.org/wiki/ประกาศนายกรัฐมนตรี_ลงวันที่_7_มีนาคม_2477_(รก.))
        * [Holidays Act Amendment (JAN 1938)](https://web.archive.org/web/20240812112232/https://ratchakitcha.soc.go.th/documents/1105432.pdf)
        * [Translation Typo Fixed for the JAN 1938 Amendment](https://web.archive.org/web/20250428132201/https://ratchakitcha.soc.go.th/documents/1105491.pdf)
        * [Constitution Petition Day renamed National Day (JUL 1938)](https://web.archive.org/web/20241109062813/https://th.wikisource.org/wiki/ประกาศสำนักนายกรัฐมนตรี_ลงวันที่_18_กรกฎาคม_2481)
        * [Holidays Act Amendment (MAR 1940)](https://web.archive.org/web/20240813142812/https://ratchakitcha.soc.go.th/documents/1110471.pdf)
        * [Holidays Act Amendment (SEP 1940) (In-effect 1941)](https://web.archive.org/web/20231021230955/https://ratchakitcha.soc.go.th/documents/1111954.pdf)
        * [Removal of Royal Language for King's Birthday (B.E 2484/1941)](https://web.archive.org/web/20241213001307/https://th.wikisource.org/wiki/ประกาศสำนักนายกรัฐมนตรี_ลงวันที่_19_กันยายน_2484)
        * [Holidays Act, Franco-Thai War Armistice Day added (B.E. 2485/1942)](https://web.archive.org/web/20250428123610/https://ratchakitcha.soc.go.th/documents/1114825.pdf)
        * [Holidays Act, Franco-Thai War Armistice Day Repealed (B.E. 2487/1944)](https://web.archive.org/web/20250428123653/https://ratchakitcha.soc.go.th/documents/1121365.pdf)
        * [Removal of Royal Language for King's Birthday Repealed (B.E 2488/1945)](https://web.archive.org/web/20241210051421/https://th.wikisource.org/wiki/ประกาศสำนักนายกรัฐมนตรี_ลงวันที่_12_มกราคม_2488)
        * [Holidays Act Amendment (DEC B.E. 2488/1945)](https://web.archive.org/web/20231022121809/https://ratchakitcha.soc.go.th/documents/1123218.pdf)
        * [Holidays Act Amendment (AUG B.E. 2489/1946)](https://web.archive.org/web/20250428132451/https://ratchakitcha.soc.go.th/documents/1124494.pdf)
        * [Special Weekend Arrangement for 4 Southern Provinces (4SP)](https://web.archive.org/web/20250428133015/https://ratchakitcha.soc.go.th/documents/1129175.pdf)
        * [Holidays Act, B.E. 2491 (1948)](https://web.archive.org/web/20240812124132/https://ratchakitcha.soc.go.th/documents/1130817.pdf)
        * [Holidays Act (No. 2), B.E. 2493 (1950)](https://web.archive.org/web/20240812132452/https://ratchakitcha.soc.go.th/documents/1141392.pdf)
        * [Holidays Act (No. 3), B.E. 2494 (1951)](https://web.archive.org/web/20250428133136/https://ratchakitcha.soc.go.th/documents/1143601.pdf)
        * [HM King Bhumibol Adulyadej Birthday Holidays Adjustment](https://web.archive.org/web/20250428133250/https://ratchakitcha.soc.go.th/documents/1145614.pdf)
        * [Holidays Act (No. 4), B.E. 2495 (1952)](https://web.archive.org/web/20250428133326/https://ratchakitcha.soc.go.th/documents/1148403.pdf)
        * [Holidays Act (No. 6), B.E. 2497 (1954)](https://web.archive.org/web/20240812201729/https://ratchakitcha.soc.go.th/documents/1159427.pdf)
        * [Holidays Act (No. 7), B.E. 2497 (1954); United Nations Day added](https://web.archive.org/web/20250428133416/https://ratchakitcha.soc.go.th/documents/1160256.pdf)
        * [Holidays Act (No. 8), B.E. 2499 (1956); Mothers Day, Children's Day added](https://web.archive.org/web/20250428133618/https://ratchakitcha.soc.go.th/documents/1169783.pdf)
        * [Holidays Act (No. 9), B.E. 2499 (1956); Weekend is now Buddhist Sabbath-SUN](https://web.archive.org/web/20250428133714/https://ratchakitcha.soc.go.th/documents/1172667.pdf)
        * [Holidays Act (No. 10), B.E. 2500 (1957); Weekend Change Reverted](https://web.archive.org/web/20250428133819/https://ratchakitcha.soc.go.th/documents/1179999.pdf)
        * [Holidays Act (No. 11), B.E. 2500 (1957)](https://web.archive.org/web/20250428133903/https://ratchakitcha.soc.go.th/documents/1180042.pdf)
        * [Holidays Act (No. 12), B.E. 2502 (1959); Weekend is full day SAT-SUN](https://web.archive.org/web/20250428134027/https://ratchakitcha.soc.go.th/documents/1187876.pdf)
        * [Holidays Act (No. 13), B.E. 2503 (1960); National Day is now Dec 5](https://web.archive.org/web/20230627092848/https://ratchakitcha.soc.go.th/documents/1196364.pdf)
        * [Holidays Act (No. 14), B.E. 2505 (1962); Asarnha Bucha added](https://web.archive.org/web/20250428134121/https://ratchakitcha.soc.go.th/documents/1206407.pdf)
        * [Holidays Act (No. 16), B.E. 2506 (1963); 4SP weekend is now SAT-SUN](https://archive.org/details/httpshr.rid.go.thwp-contentuploads20221206-16.pdf)
        * [Eid-al-Fitr and Eit al-Adha added for 4SP](https://archive.org/details/httpshr.rid.go.thwp-contentuploads20221205-2517.pdf)
        * [Holidays Act (No. 17), B.E. 2525 (1982); Chakri Day Full Name Changed](https://web.archive.org/web/20250428134349/https://ratchakitcha.soc.go.th/documents/1494132.pdf)
        * [Holidays Act (No. 19), B.E. 2540 (1997); Songkran Date Changed](https://web.archive.org/web/20250428134426/https://ratchakitcha.soc.go.th/documents/1685580.pdf)
        * [Holidays Act (No. 20), B.E. 2555 (2012); CNY in 4SP](https://web.archive.org/web/20250428134504/https://ratchakitcha.soc.go.th/documents/1914282.pdf)
        * [Holidays Act (No. 21), B.E. 2556 (2013); Songkhla added to 4SP](https://web.archive.org/web/20250428134536/https://ratchakitcha.soc.go.th/documents/2098813.pdf)
        * [Holidays Act (No. 22), B.E. 2560 (2017); Father's Day Date Clarification](https://web.archive.org/web/20230627142834/https://ratchakitcha.soc.go.th/documents/2098828.pdf)
        * [Holidays Act (No. 23), B.E. 2560 (2017); Rama X's Birthday added](https://web.archive.org/web/20250428134706/https://ratchakitcha.soc.go.th/documents/2104467.pdf)
        * [HM Queen Suthida's Birthday added](https://web.archive.org/web/20250428134748/https://ratchakitcha.soc.go.th/documents/17081602.pdf)
        * [Holidays Act, B.E. 2562 (2019)](https://web.archive.org/web/20250428134835/https://ratchakitcha.soc.go.th/documents/17082311.pdf)

    Checked with:
        * [Bank of Thailand](https://web.archive.org/web/20230205072056/https://www.bot.or.th/Thai/FinancialInstitutions/FIholiday/Pages/2023.aspx)

    In Lieus:
        * [isranews.org](https://web.archive.org/web/20230205073547/https://www.isranews.org/content-page/item/20544-วันหยุดชดเชย-มาจากไหน-sp-863880667.html)
        * <https://web.archive.org/web/20250428135057/https://resolution.soc.go.th/?prep_id=99159317>
        * <https://web.archive.org/web/20250428151918/https://resolution.soc.go.th/?prep_id=196007>
        * <https://github.com/vacanza/holidays/pull/929>
        * <https://web.archive.org/web/20250418080608/https://www.thairath.co.th/lifestyle/life/2812118>
        * <https://web.archive.org/web/20250408214054/https://www.thaipbs.or.th/news/content/346216>

    Certain holidays references:
        * [New Year's Day](https://web.archive.org/web/20230115102900/https://th.wikisource.org/wiki/ประกาศให้ใช้วันที่_1_มกราคม_เป็นวันขึ้นปีใหม่_ลงวันที่_24_ธันวาคม_2483)
        * [National Children's Day](https://web.archive.org/web/20230929070232/https://thainews.prd.go.th/banner/th/children'sday/)
        * [Chakri Memorial Day](https://web.archive.org/web/20221003075521/https://www.ocac.go.th/news/๖-เมษายน-วันจักรี/v)
        * Songkran Festival:
            * [museumsiam.org](https://web.archive.org/web/20230205074402/https://m.museumsiam.org/da-detail2.php?MID=3&CID=177&CONID=4033)
            * <https://web.archive.org/web/20250501040114/https://resolution.soc.go.th/?prep_id=123659>
        * [National Labour Day](https://web.archive.org/web/20220629085454/https://www.thairath.co.th/lifestyle/culture/1832869)
        * [National Day (24 June: Defunct)](https://web.archive.org/web/20091106202525/http://www.culture.go.th/study.php?&YY=2548&MM=11&DD=2)
        * Coronation Day:
            * <https://web.archive.org/web/20220817223130/https://www.matichon.co.th/politics/news_526200>
            * <https://web.archive.org/web/20230205075326/https://workpointtoday.com/news1-5/>
        * [HM Queen Suthida's Birthday](https://web.archive.org/web/20241217104142/https://www.thairath.co.th/news/politic/1567418)
        * [HM Maha Vajiralongkorn's Birthday](https://web.archive.org/web/20220817223130/https://www.matichon.co.th/politics/news_526200)
        * [HM Queen Sirikit the Queen Mother's Birthday](https://web.archive.org/web/20250203002257/https://hilight.kapook.com/view/14164)
        * [National Mother's Day](https://web.archive.org/web/20221207161434/https://www.brh.go.th/index.php/2019-02-27-04-11-52/542-12-2564)
        * [HM King Bhumibol Adulyadej Memorial Day](https://web.archive.org/web/20220817223130/https://www.matichon.co.th/politics/news_526200)
        * HM King Chulalongkorn Memorial Day:
            * <https://th.wikipedia.org/wiki/วันปิยมหาราช>
            * <https://web.archive.org/web/20231024220855/https://www.sanook.com/news/9072518/>
        * HM King Bhumibol Adulyadej's Birthday
            * [Ministry of Culture](https://web.archive.org/web/20091106202525/http://www.culture.go.th/study.php?&YY=2548&MM=11&DD=2)
            * <https://web.archive.org/web/20250126073943/https://hilight.kapook.com/view/148862>
        * [National Father's Day](https://web.archive.org/web/20230205075650/https://www.brh.go.th/index.php/2019-02-27-04-12-21/594-5-5)
        * Constitution Day:
            * <https://th.wikipedia.org/wiki/วันรัฐธรรมนูญ>
            * <https://web.archive.org/web/20250212210436/https://hilight.kapook.com/view/18208>
            * [Bank of Thailand](https://web.archive.org/web/20230205072056/https://www.bot.or.th/Thai/FinancialInstitutions/FIholiday/Pages/2023.aspx)
            * <https://web.archive.org/web/20161028001043/http://www.myhora.com:80/ปฏิทิน/ปฏิทิน-พ.ศ.2475.aspx>
        * New Year's Eve:
            * [Bank of Thailand](https://web.archive.org/web/20230205072056/https://www.bot.or.th/Thai/FinancialInstitutions/FIholiday/Pages/2023.aspx)
            * <https://web.archive.org/web/20250428140227/https://resolution.soc.go.th/?prep_id=205799>
            * <https://web.archive.org/web/20250428152003/https://resolution.soc.go.th/?prep_id=210744>
        * [Makha Bucha](https://web.archive.org/web/20240908185818/https://www.onab.go.th/th/content/category/detail/id/73/iid/3403)
        * [Visakha Bucha](https://web.archive.org/web/20240908190019/https://www.onab.go.th/th/content/category/detail/id/73/iid/3401)
        * [Asarnha Bucha](https://web.archive.org/web/20240816131909/https://www.onab.go.th/th/content/category/detail/id/73/iid/3397)
        * [Buddhist Lent Day](https://web.archive.org/web/20240908185432/https://www.onab.go.th/th/content/category/detail/id/73/iid/3395)
        * Royal Ploughing Ceremony:
            * <https://en.wikipedia.org/wiki/Royal_Ploughing_Ceremony>
            * <https://web.archive.org/web/20241211020149/https://www.lib.ru.ac.th/journal/may/may_phauchmongkol.html>
            * <https://web.archive.org/web/20250427185801/https://dl.parliament.go.th/handle/20.500.13072/103428>
            * <https://web.archive.org/web/20250427185656/https://dl.parliament.go.th/handle/20.500.13072/92816>
            * <https://web.archive.org/web/20250428135456/http://mdc.library.mju.ac.th/article/57695/297565/367757.pdf>
            * <https://web.archive.org/web/20250428140422/https://resolution.soc.go.th/PDF_UPLOAD/2510/932141.pdf>
            * <https://web.archive.org/web/20161028001043/http://www.myhora.com:80/ปฏิทิน/ปฏิทิน-พ.ศ.2475.aspx>
            * <https://web.archive.org/web/20251108075053/https://pridi.or.th/th/content/2024/05/1954>
        * [Royal Thai Armed Forces Day](https://th.wikipedia.org/wiki/วันกองทัพไทย)
        * [Teacher's Day](https://web.archive.org/web/20250117105542/http://event.sanook.com/day/teacher-day/)

    !!! info "Info"
        If Public Holiday falls on weekends, (in lieu) on workday.

        Despite the wording, this usually only applies to Monday only for
        holidays, consecutive holidays all have their own special in lieu
        declared separately.

        Data from 1992-1994 and 1998-2000 are declared discretely in
        special_holidays declarations above.

        Applied Automatically for Monday if on Weekends: 1961-1973

    !!! note "Note"
        No New Year's Eve (in lieu) for this period

        No In Lieu days available: 1974-1988

        Case-by-Case application for Workday if on Weekends: 1989-1994

        Applied Automatically for Workday if on Weekends: 1995-1997

        Case-by-Case application for Workday if on Weekends: 1998-2000

        Applied Automatically for Workday if on Weekends: 2001-Present

    Limitations:
        * This is only 100% accurate for 1997-2025; any future dates are up to the
            Royal Thai Government Gazette which updates on a year-by-year basis.
        * Thai Lunar Calendar Holidays only work until 2157 (B.E. 2700) as we only
            have Thai year-type data for cross-checking until then.
        * Royal Ploughing Ceremony Day is date is announced on an annual basis
            by the Court Astrologers, thus need an annual update to the library here
    """

    country = "TH"
    default_language = "th"
    # %s (in lieu).
    observed_label = tr("ชดเชย%s")
    # First Holiday Decree was promulgated in March 1914.
    start_year = 1914
    supported_categories = (ARMED_FORCES, BANK, GOVERNMENT, PUBLIC, SCHOOL, WORKDAY)
    supported_languages = ("en_US", "th", "uk")

    def __init__(self, *args, **kwargs):
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, ThailandStaticHolidays)
        ThaiCalendarHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _is_observed(self, dt: date) -> bool:
        return 1961 <= self._year <= 1973 or 1995 <= self._year <= 1997 or self._year >= 2001

    def _get_weekend(self, dt: date) -> set[int]:
        if dt >= date(1959, MAR, 1):
            # SAT & SUN (Full Day).
            weekend = {SAT, SUN}
        elif date(1956, OCT, 1) <= dt <= date(1957, OCT, 6):
            # Buddhist Sabbath Days and Sun (Full Day).
            weekend = {SUN}
            buddhist_sabbath_dates = self._thai_calendar.buddhist_sabbath_dates(dt.year)
            if dt in buddhist_sabbath_dates:
                weekend.add(dt.weekday())
        elif dt >= date(1939, FEB, 28):
            # SAT from 12:00 onwards and SUN (Full Day).
            weekend = {SUN}
        else:
            # Prior to this, there was no concept of weekend.
            weekend = set()

        return weekend

    def _populate_public_holidays(self):
        # Fixed Date Holidays

        # วันขึ้นปีใหม่
        # Status: In-Use.
        # Starts in the present form in 1941 (B.E. 2484).
        # From 1941-1945 and 1948-1954 JAN 2nd was also observed.
        # For pre- observance, see New Year's Eve entry.

        if self._year >= 1941:
            # New Year's Day.
            name = tr("วันขึ้นปีใหม่")
            self._add_observed(self._add_new_years_day(name))
            if self._year <= 1945 or 1948 <= self._year <= 1954:
                self._add_new_years_day_two(name)

        # วันเด็กแห่งชาติ
        # Status: In-Use.
        # Starts in 1955 (B.E. 2498) as the 1st Monday of OCT.
        #   Its status as a Public Holiday was reaffirmed in 1956.
        # No event was held in 1964 due to date changes came into effect too late.
        # Moved to 2nd Saturday of JAN since 1965.
        # No in-lieus are observed, and still remain a Public Holidays than just Observed.

        if self._year >= 1955 and self._year != 1964:
            # National Children's Day.
            name = tr("วันเด็กแห่งชาติ")
            if self._year <= 1963:
                self._add_holiday_1st_mon_of_oct(name)
            else:
                self._add_holiday_2nd_sat_of_jan(name)

        # วันลงนามในสัญญาพักรบระหว่างประเทศไทยกับประเทศอินโดจีนฝรั่งเศส
        # Status: Defunct.
        # Started in 1942 (B.E. 2485), abandoned from 1945 onwards.

        if 1942 <= self._year <= 1944:
            # Franco-Thai War Armistice Day.
            self._add_holiday_jan_28(tr("วันลงนามในสัญญาพักรบระหว่างประเทศไทยกับประเทศอินโดจีนฝรั่งเศส"))

        # วันจักรี
        # Status: In-Use.
        # Starts in present form in 1926 (B.E. 2469).

        if self._year >= 1926:
            if self._year >= 1983:
                # Chakri Memorial Day.
                name = tr("วันพระบาทสมเด็จพระพุทธยอดฟ้าจุฬาโลกมหาราช และวันที่ระลึกมหาจักรีบรมราชวงศ์")
            elif self._year >= 1938:
                # Chakri Day.
                name = tr("วันจักรี")
            else:
                # Maha Chakri Memorial Day.
                name = tr("วันที่ระลึกมหาจักรี")
            self._add_observed(self._add_holiday_apr_6(name))

        # พระราชพิธีตะรุษะสงกรานต์ แลนักขัตฤกษ์ (1914-1925)
        # ตะรุษะสงกรานต์ (1926-1937)
        # วันตรุษสงกรานต์ (1938-1939)
        # วันตรุษสงกรานต์และขึ้นปีใหม่ (1940)
        # วันสงกรานต์ (1941-Present)
        # Status: In-Use.
        # Started in 1914 (B.E. 2457).
        # Earliest form of Songkran public holidays was observed from MAR 28th to APR 15th
        # (19 days). Later extended for MoJ staff only upto APR 27th from 1915 (30 days).
        #  - 1926-1937, celebrated on MAR 31st-APR 3rd.
        #  - 1938-1940, celebrated on MAR 31st-APR 2nd.
        #  - 1941-1947, abandoned as a public holiday.
        #  - 1948-1953, celebrated on APR 13th-15th.
        #  - 1954-1956, abandoned as a public holiday.
        #  - 1957-1988, only celebrated on APR 13th.
        #  - 1989-1997, celebrated on APR 12th-14th.
        #  - 1998-Present, celebrated on APR 13th-15th.
        #    (Except for 2020 due to Covid-19 outbreaks)
        # This has its own in-lieu trigger.

        if self._year <= 1940 or 1948 <= self._year <= 1953 or 1957 <= self._year != 2020:
            # Songkran Festival.
            name = tr("วันสงกรานต์")
            if 1989 <= self._year <= 1997:
                dt = self._add_holiday_apr_12(name)
                self._add_holiday_apr_13(name)
                self._add_holiday_apr_14(name)
            elif 1957 <= self._year <= 1988:
                self._add_observed(self._add_holiday_apr_13(name))
            elif 1926 <= self._year <= 1940:
                if self._year == 1940:
                    # Songkran New Year.
                    name = tr("วันตรุษสงกรานต์และขึ้นปีใหม่")
                elif self._year >= 1938:
                    # Songkran New Year.
                    name = tr("วันตรุษสงกรานต์")
                else:
                    # Songkran New Year.
                    name = tr("ตะรุษะสงกรานต์")
                self._add_holiday_mar_31(name)
                self._add_holiday_apr_1(name)
                self._add_holiday_apr_2(name)
                if self._year <= 1937:
                    self._add_holiday_apr_3(name)
            elif self._year <= 1925:
                self._add_multiday_holiday(
                    # Songkran New Year Holidays.
                    self._add_holiday_mar_28(tr("พระราชพิธีตะรุษะสงกรานต์ แลนักขัตฤกษ์")),
                    18,
                )
            else:
                dt = self._add_holiday_apr_13(name)
                self._add_holiday_apr_14(name)
                self._add_holiday_apr_15(name)

            # วันหยุดชดเชยวันสงกรานต์
            # If Songkran happened to be held on the weekends, only one in-lieu
            #   public holiday is added.
            #   - CASE 1: THU-FRI-SAT -> 1 in-lieu on MON
            #   - CASE 2: FRI-SAT-SUN -> 1 in-lieu on MON
            #   - CASE 3: SAT-SUN-MON -> 1 in-lieu on TUE
            #   - CASE 4: SUN-MON-TUE -> 1 in-lieu on WED
            # See in lieu logic in `_add_observed(dt: date)`.
            # Status: In Use.

            if self._year >= 1995:
                self._add_observed(dt, rule=THU_FRI_TO_NEXT_WORKDAY + SAT_SUN_TO_NEXT_WORKDAY)

        # วันแรงงานแห่งชาติ
        # Status: In-Use.
        # Starts in the present form in 1974 (B.E. 2517).
        # Does existed officially since 1956 (B.E. 2499), but wasn't a public holiday until then.
        # *** NOTE: only observed by financial and private sectors.

        if self._year >= 1974:
            # National Labor Day.
            self._add_observed(self._add_labor_day(tr("วันแรงงานแห่งชาติ")))

        # วันขอพระราชทานรัฐธรรมนูญ (1938-1939)
        # วันชาติ (1940-Present)
        # Status: In-Use.
        # Started as Constitution Petition Day in 1938 (B.E. 2481) for JUN 24th.
        # Got its current name in 1940 by Plaek Phibunsongkhram.
        # This was observed JUN 23th-25th between 1940-1948.
        # Moved to Rama IX's birthday in 1960 (B.E. 2503) by Sarit Thanarat.

        if self._year >= 1938:
            name = (
                # National Day.
                tr("วันชาติ")
                if self._year >= 1939
                # Constitution Petition Day.
                else tr("วันขอพระราชทานรัฐธรรมนูญ")
            )
            self._add_observed(
                self._add_holiday_jun_24(name)
                if self._year <= 1959
                else self._add_holiday_dec_5(name)
            )
            if 1940 <= self._year <= 1948 or 1952 <= self._year <= 1953:
                self._add_holiday_jun_23(name)
                self._add_holiday_jun_25(name)

        # วันรัฐธรรมนูญชั่วคราว
        # Status: Defunct.
        # Started in 1938, abandoned in 1940.

        if 1938 <= self._year <= 1939:
            # Provisional Constitution Day.
            self._add_holiday_jun_27(tr("วันรัฐธรรมนูญชั่วคราว"))

        # ทำบุญพระบรมอัษฐิ และพระราชพิธีฉัตรมงคล (1914-1925)
        # พระราชพิธีฉัตรมงคล (1926-1937)
        # วันฉัตรมงคล (1958-Present)
        # First observed for Rama VI from 1914-1925 from NOV 9th-12th.
        # For Rama VII (1926-1935) this was FEB 24th-26th.
        #   After Rama VII abdicated on MAR 2nd, 1935, there's no recorded
        #   change in terms of holiday arrangement until 1938,
        #   thus, a gap in holidays arrangement is assumed.
        # Rama VIII passes away without a coronation ceremony.
        # Starts in 1958 (B.E. 2501) for Rama IX's Coronation: MAY 5th.
        # No celebration in 2017-2019 (B.E. 2560-2562).
        # Reestablished with Rama X's Coronation in 2020: MAY 4th.

        if self._year <= 1935 or self._year >= 1958:
            # Coronation Day.
            name = tr("วันฉัตรมงคล")
            if self._year >= 2020:
                # Rama X (2020-Present).
                self._add_observed(self._add_holiday_may_4(name))
            elif 1958 <= self._year <= 2016:
                # Rama IX (1958-2016).
                self._add_observed(self._add_holiday_may_5(name))
            elif 1926 <= self._year <= 1935:
                # Coronation Day.
                name = tr("พระราชพิธีฉัตรมงคล")
                # Rama VII (1926-1935).
                self._add_holiday_feb_24(name)
                self._add_holiday_feb_25(name)
                self._add_holiday_feb_26(name)
            elif self._year <= 1925:
                # Merit-making Ceremony for the Royal Ashes and the Coronation Day.
                name = tr("ทำบุญพระบรมอัษฐิ และพระราชพิธีฉัตรมงคล")
                # Rama VI (1914-1925).
                self._add_holiday_nov_9(name)
                self._add_holiday_nov_10(name)
                self._add_holiday_nov_11(name)
                self._add_holiday_nov_12(name)

        # วันเฉลิมพระชนมพรรษา พระราชินี
        # Status: In-Use.
        # Starts in 2019 (B.E. 2562).

        if self._year >= 2019:
            self._add_observed(
                self._add_holiday_jun_3(
                    # HM Queen Suthida's Birthday.
                    tr("วันเฉลิมพระชนมพรรษาสมเด็จพระนางเจ้าสุทิดา พัชรสุธาพิมลลักษณ พระบรมราชินี")
                )
            )

        # วันเฉลิมพระชนมพรรษา รัชกาลที่ 10
        # Status: In-Use.
        # Started in 2017 (B.E. 2560).

        if self._year >= 2017:
            self._add_observed(
                self._add_holiday_jul_28(
                    # HM King Maha Vajiralongkorn's Birthday.
                    tr(
                        "วันเฉลิมพระชนมพรรษาพระบาทสมเด็จพระปรเมนทรรามาธิบดี"
                        "ศรีสินทรมหาวชิราลงกรณ พระวชิรเกล้าเจ้าอยู่หัว"
                    )
                )
            )

        # วันเฉลิมพระชนมพรรษา พระบรมราชินีนาถ (1976-2017)
        # วันเฉลิมพระชนมพรรษา พระบรมราชชนนีพันปีหลวง (2017-Present)
        # Status: In-Use.
        # Started in 1976 (B.E. 2519) alongside Mother's Day.
        # Initial celebration as HM Queen Sirikit's Birthday.
        # Now acts as the Queen Mother from 2017 onwards.

        if self._year >= 1976:
            self._add_observed(
                self._add_holiday_aug_12(
                    # HM Queen Sirikit the Queen Mother's Birthday.
                    tr("วันเฉลิมพระชนมพรรษาสมเด็จพระบรมราชชนนีพันปีหลวง")
                    if self._year >= 2017
                    # HM Queen Sirikit's Birthday.
                    else tr("วันเฉลิมพระชนมพรรษาสมเด็จพระนางเจ้าสิริกิติ์ พระบรมราชินีนาถ")
                )
            )

        # วันแม่แห่งชาติ
        # Status: In-Use.
        # Started 1950 (B.E. 2493) initially as APR 15th and cancelled in
        #   1958 (B.E. 2501) when the Min. of Culture was abolished.
        #   Its status as a Public Holiday was reaffirmed in 1956.
        # Restarts again in 1976 (B.E. 2519) on Queen Sirikit's Birthday
        #   (AUG 12th) and stay that way from that point onwards.

        # National Mother's Day.
        name = tr("วันแม่แห่งชาติ")
        if 1950 <= self._year <= 1957:
            self._add_observed(self._add_holiday_apr_15(name))
        elif self._year >= 1976:
            self._add_observed(self._add_holiday_aug_12(name))

        # วันประกาศสันติภาพ
        # Status: Defunct.
        # Started in 1946 (B.E. 2489), removed in 1948.

        if 1946 <= self._year <= 1947:
            # Peace Proclamation Day.
            self._add_holiday_aug_16(tr("วันประกาศสันติภาพ"))

        # วันคล้ายวันสวรรคตพระบาทสมเด็จพระปรมินทรมหาภูมิพลอดุลยเดช บรมนาถบพิตร (2017-2018)
        # วันคล้ายวันสวรรคตพระบาทสมเด็จพระบรมชนกาธิเบศร มหาภูมิพลอดุลยเดชมหาราช บรมนาถบพิตร (2019-2022)
        # วันนวมินทรมหาราช (2023-Present)
        # Status: In-Use.
        # Started in 2017 (B.E. 2560).
        # Got conferred with 'the Great' title in 2019 (B.E. 2562).

        if self._year >= 2017:
            if self._year >= 2023:
                # HM King Bhumibol Adulyadej Memorial Day.
                name = tr("วันนวมินทรมหาราช")
            elif self._year >= 2019:
                # Anniversary for the Death of King Bhumibol Adulyadej the Great.
                name = tr(
                    "วันคล้ายวันสวรรคตพระบาทสมเด็จพระบรมชนกาธิเบศร มหาภูมิพลอดุลยเดชมหาราช บรมนาถบพิตร"
                )
            else:
                # Anniversary for the Death of King Bhumibol Adulyadej.
                name = tr("วันคล้ายวันสวรรคตพระบาทสมเด็จพระปรมินทรมหาภูมิพลอดุลยเดช บรมนาถบพิตร")
            self._add_observed(self._add_holiday_oct_13(name))

        # ทำบุญพระบรมอัษฐิพระพุทธเจ้าหลวง (1914-1925)
        # วันสวรรคตแห่งพระบาทสมเด็จพระพุทธเจ้าหลวง (1926-1937)
        # วันปิยมหาราช (1946-Present)
        # Status: In-Use.
        # Started in 1914 (B.E. 2457).
        # Was abandoned between 1938-1945.

        if self._year <= 1937 or self._year >= 1946:
            if self._year >= 1946:
                # HM King Chulalongkorn Memorial Day.
                name = tr("วันปิยมหาราช")
            elif self._year >= 1926:
                # Anniversary for the Death of HM King Chulalongkorn.
                name = tr("วันสวรรคตแห่งพระบาทสมเด็จพระพุทธเจ้าหลวง")
            else:
                # Merit-making Ceremony for the Royal Ashes of HM King Chulalongkorn.
                name = tr("ทำบุญพระบรมอัษฐิพระพุทธเจ้าหลวง")
            self._add_observed(self._add_holiday_oct_23(name))

        # วันสหประชาชาติ
        # Status: Defunct.
        # Added since 1951 (B.E. 2494), removed in 1957.
        # Technically removed in 1954, but was re-added soon after.

        if 1951 <= self._year <= 1956:
            # United Nations Day.
            self._add_united_nations_day(tr("วันสหประชาชาติ"))

        # เฉลิมพระชนมพรรษา (1914-1925)
        # เฉลิมพระชนม์พรรษา (1926-1937)
        # วันเฉลิมพระชนม์พรรษา (1938-1940)
        # วันเกิดในสมเด็จพระเจ้าอยู่หัว (1941-1944)**
        # วันเฉลิมพระชนมพรรษา (1945-1959)
        # วันเฉลิมพระชนมพรรษา รัชกาลที่ 9 (1960-2016)
        # วันคล้ายวันเฉลิมพระชนมพรรษา รัชกาลที่ 9 (2017-Present)
        # Status: In-Use.
        # Rama VI's Birthday was observed from DEC 30th-JAN 3rd between 1914-1925.
        # Rama VII's Birthday was observed from NOV 7th-9th between 1926-1934.
        #   After Rama VII abdicated on MAR 2nd, 1935, there's no recorded
        #   change in terms of holiday arrangement until 1938,
        #   thus, a gap in holidays arrangement is assumed.
        # Rama VIII's Birthday was observed on SEP 20th between 1938-1945.
        #   Between 1940-1945 the 2nd day (SEP 21st) was also observed as well.
        # Rama IX's Birthday was observed from 1946-Present.
        #   Between 1946-1953 DEC 6th was also observed. From 1948 onwards DEC 4th
        #   was observed too except in 1951 when it was temporarily moved to DEC 7th.
        # Replaced National Day (JUN 26th) in 1960 (B.E. 2503) by Sarit Thanarat.
        # Confirmed as still in-use as Rama IX's in 2017.
        # Rama IX got conferred with 'the Great' title in 2019 (B.E. 2562).
        # For historical purpose, 1926-1940 entry uses the old spelling.
        # **Official naming was override by "Birthday Name Decree" between 1941-1944.

        if self._year >= 2019:
            # HM King Bhumibol Adulyadej the Great's Birthday Anniversary.
            name = tr(
                "วันคล้ายวันเฉลิมพระชนมพรรษาพระบาทสมเด็จพระบรมชนกาธิเบศร มหาภูมิพลอดุลยเดชมหาราช บรมนาถบพิตร"
            )
        elif self._year >= 2016:
            # HM King Bhumibol Adulyadej Birthday Anniversary.
            name = tr("วันคล้ายวันเฉลิมพระชนมพรรษาพระบาทสมเด็จพระปรมินทรมหาภูมิพลอดุลยเดช บรมนาถบพิตร")
        elif self._year >= 1960:
            # HM King Bhumibol Adulyadej Birthday Anniversary.
            name = tr("วันเฉลิมพระชนมพรรษาพระบาทสมเด็จพระปรมินทรมหาภูมิพลอดุลยเดช บรมนาถบพิตร")
        elif self._year >= 1945:
            # The King's Birthday.
            name = tr("วันเฉลิมพระชนมพรรษา")
        elif self._year >= 1941:
            # The King's Birthday.
            name = tr("วันเกิดในสมเด็จพระเจ้าอยู่หัว")
        elif self._year >= 1938:
            # The King's Birthday.
            name = tr("วันเฉลิมพระชนม์พรรษา")
        elif self._year >= 1926:
            # The King's Birthday.
            name = tr("เฉลิมพระชนม์พรรษา")
        else:
            # The King's Birthday.
            name = tr("เฉลิมพระชนมพรรษา")

        if self._year >= 1946:
            # Rama IX (1946-2016; In Memoriam 2017-Present).
            self._add_observed(self._add_holiday_dec_5(name))
            if self._year <= 1953:
                self._add_holiday_dec_6(name)
                if self._year == 1951:
                    self._add_holiday_dec_7(name)
                elif self._year >= 1948:
                    self._add_holiday_dec_4(name)
        elif self._year >= 1938:
            # Rama VIII (1938*-1945).
            # *There was no decree confirming his holiday until 1938.
            self._add_holiday_sep_20(name)
            if self._year >= 1940:
                self._add_holiday_sep_21(name)
        elif 1926 <= self._year <= 1934:
            # Rama VII (1926-1934).
            self._add_holiday_nov_7(name)
            self._add_holiday_nov_8(name)
            self._add_holiday_nov_9(name)
        elif self._year <= 1925:
            # Rama VI (1914-1925).
            if self._year <= 1924:
                self._add_holiday_dec_30(name)
                self._add_holiday_dec_31(name)
            if self._year >= 1915:
                self._add_holiday_jan_1(name)
                self._add_holiday_jan_2(name)
                self._add_holiday_jan_3(name)

        # วันพ่อแห่งชาติ
        # Status: In-Use.
        # Starts in 1980 (B.E. 2523).
        # Technically, a replication of HM King Bhumibol Adulyadej's Birthday
        #   but it's in the official calendar, so may as well have this here.

        if self._year >= 1980:
            # National Father's Day.
            self._add_observed(self._add_holiday_dec_5(tr("วันพ่อแห่งชาติ")))

        # วันรัฐธรรมนูญ
        # Status: In-Use.
        # Starts in 1938 (B.E. 2481).
        # Initially as DEC 9th-11th, got trimmed down to DEC 10th in 1947.
        # Readded Pre- and Post- holidays in 1950 before removed again in 1954.

        if self._year >= 1938:
            # Constitution Day.
            name = tr("วันรัฐธรรมนูญ")
            self._add_observed(self._add_holiday_dec_10(name))
            if self._year <= 1947 or 1950 <= self._year <= 1953:
                self._add_holiday_dec_9(name)
                self._add_holiday_dec_11(name)

        # วันสิ้นปี
        # Status: In-Use.
        # Started in 1941 (B.E. 2484; as part of New Year Holidays)
        # Abandoned in 1957, presumed to restart in 1989.
        # This has its own in-lieu trigger.

        if 1941 <= self._year <= 1956 or self._year >= 1989:
            # New Year's Eve.
            name = tr("วันสิ้นปี")
            self._add_new_years_eve(name)

            # วันหยุดชดเชยวันสิ้นปี
            # Status: In-Use.
            # Added separately from New Year's Eve itself so that it would't
            #   go over the next year.
            #   - CASE 1: SAT-SUN -> 1 in-lieu on TUE.
            #   - CASE 2: SUN-MON -> 1 in-lieu on TUE.
            # See in lieu logic in `_add_observed(dt: date)`.

            if self._year >= 1995 and self._year != 2024:
                self._add_observed(
                    date(self._year - 1, DEC, 31), name=name, rule=SAT_SUN_TO_NEXT_TUE
                )

        # Thai Lunar Calendar Holidays
        # See `_ThaiLunisolar` in holidays/utils.py for more details.

        # วันมาฆบูชา
        # Status: In-Use.
        # Started in 1915 (B.E. 2457**), not observed between 1926-1937.
        # For historical purpose, pre-1925 entry uses the old spelling.
        # **For pre-1941 data, Buddhist Era year starts on APR 1st.

        if 1915 <= self._year <= 1925 or self._year >= 1938:
            self._add_observed(
                self._add_makha_bucha(
                    # Makha Bucha.
                    tr("วันมาฆบูชา")
                    if self._year >= 1938
                    # Makha Bucha, the Fourfold Assembly Day.
                    else tr("มาฆบูชา จาตุรงฅ์สันนิบาต")
                )
            )

        # วันวิสาขบูชา
        # Status: In-Use.
        # Started in 1914 (B.E. 2457) with pre- and post- observance.
        # From 1938-1953 only post- is added.
        # For historical purpose, pre-1957 entry uses the old spelling.
        # Note that the ones during Rama VII era uses ศ instead of ส.

        if self._year >= 1957:
            # Visakha Bucha.
            name = tr("วันวิสาขบูชา")
        elif self._year >= 1938:
            # Visakha Bucha.
            name = tr("วันวิสาขะบูชา")
        elif self._year >= 1926:
            # Visakha Bucha.
            name = tr("วิศาขะบูชา")
        else:
            # Visakha Bucha.
            name = tr("วิสาขะบูชา")

        dt = self._add_visakha_bucha(name)
        if self._year <= 1953:
            self._add_holiday(name, _timedelta(dt, +1))
            if self._year <= 1937:
                self._add_holiday(name, _timedelta(dt, -1))
        else:
            self._add_observed(dt)

        # วันเข้าพรรษา-วันอาสาฬหบูชา
        # Status: In-Use.
        # - Started in 1914 (B.E. 2457) with Asarnha Bucha and pre-Asarnha Bucha observance.
        # - From 1938-1953 only Asarnha Bucha is added.
        # - 2nd, 3rd, 4th, and 5th day was also observed pre-1926.
        # - Asarnha Bucha was re-added with its own name from 1962 onwards.
        # For historical purpose, pre-1938 entry uses the old spelling.
        # When used in combo with Asarnha Bucha Day.
        #  - CASE 1: FRI-SAT -> 1 in-lieu on MON
        #  - CASE 2: SAT-SUN -> 1 in-lieu on MON
        #  - CASE 3: SUN-MON -> 1 in-lieu on TUE

        name = (
            # Buddhist Lent Day.
            tr("วันเข้าพรรษา")
            if self._year >= 1938
            # Buddhist Lent Day.
            else tr("เข้าปุริมพรรษา")
        )
        dt = self._add_khao_phansa(name)
        if self._year <= 1953:
            self._add_asarnha_bucha(name)
            if self._year <= 1937:
                self._add_holiday(name, _timedelta(dt, -2))
                if self._year <= 1925:
                    self._add_holiday(name, _timedelta(dt, +1))
                    self._add_holiday(name, _timedelta(dt, +2))
                    self._add_holiday(name, _timedelta(dt, +3))
                    self._add_holiday(name, _timedelta(dt, +4))
        else:
            self._add_observed(dt, rule=SAT_TO_NEXT_MON)
            if self._year >= 1962:
                self._add_observed(
                    # Asarnha Bucha.
                    self._add_asarnha_bucha(tr("วันอาสาฬหบูชา")),
                    rule=SAT_SUN_TO_NEXT_MON_TUE,
                )

    def _populate_armed_forces_holidays(self):
        # วันกองทัพไทย
        # Status: In-Use.
        # First started in 1959 (B.E. 2502) on the Ministry of Defense Foundation Day (APR 8th).
        # Moved to JAN 25th (Supposedly King Naresuan's Decisive Battle) in 1980.
        # Corrected to the battle's actual date (JAN 18th) in 2007.
        # Only applys to members of the Royal Thai Armed Forces.

        if self._year >= 1959:
            # Royal Thai Armed Forces Day.
            name = tr("วันกองทัพไทย")
            if self._year >= 2007:
                self._add_holiday_jan_18(name)
            elif self._year >= 1980:
                self._add_holiday_jan_25(name)
            else:
                self._add_holiday_apr_8(name)

    def _populate_bank_holidays(self):
        # Bank of Thailand, the ones who decreed this wasn't found until DEC 10th, 1942.
        # So it's safe to assume with that as our start date.
        if self._year <= 1942:
            return None

        # Bank Holidays

        # วันหยุดเพิ่มเติมสำหรับการปิดบัญชีประจำปีของธนาคารเพื่อการเกษตรและสหกรณ์การเกษตร
        # Status: Defunct.
        # If held on the weekends, no in-lieus.
        # Abandoned in 2022.

        if self._year <= 2021:
            self._add_holiday_apr_1(
                # Additional Closing Day for Bank for Agriculture and Agricultural Cooperatives.
                tr("วันหยุดเพิ่มเติมสำหรับการปิดบัญชีประจำปีของธนาคารเพื่อการเกษตรและสหกรณ์การเกษตร")
            )

        # วันหยุดภาคครึ่งปีของสถาบันการเงินและสถาบันการเงินเฉพาะกิจ
        # Status: Defunct.
        # If held on the weekends, no in-lieus.
        # Abandoned in 2019.

        if self._year <= 2018:
            # Mid-Year Closing Day.
            self._add_holiday_jul_1(tr("วันหยุดภาคครึ่งปีของสถาบันการเงินและสถาบันการเงินเฉพาะกิจ"))

    def _populate_government_holidays(self):
        # No Future Fixed Date Holidays

        # วันพืชมงคล
        # Restarts in 1947 (B.E. 2490), become holiday again since 1952
        #   but since we lacked the exact date records, this will be ignored.
        # Become an holiday again until 1960 (B.E. 2503).
        # Removed as an holiday in 1999 due to financial crisis, reinstated in 2000.
        # No event was held in 2021 due to local Covid-19 situation, though it stays a day off.
        # Is dated on an annual basis by the Royal Palace, always on weekdays.
        # For historic research, วันเกษตรแห่งชาติ (National Agricultural Day) also concides with
        #   this from 1966 onwards. For earlier records the date was refered as วันแรกนาขวัญ.
        # This isn't even fixed even by the Thai Lunar Calendar besides being in Month 6
        #   to concides with the rainy season, but instead by Court Astrologers; All chosen dates
        #   so far are all in the first three weeks of MAY.
        # *** NOTE: only observed by government sectors.
        # TODO: Update this annually around Dec of each year.

        # Got only 1 source for 1992 and 1993, might need some recheck later.
        raeknakhwan_dates = {
            1960: (MAY, 2),
            1961: (MAY, 11),
            1962: (MAY, 7),
            1963: (MAY, 10),
            1964: (MAY, 8),
            1965: (MAY, 13),
            1966: (MAY, 13),
            1967: (MAY, 11),
            1968: (MAY, 10),
            1969: (MAY, 9),
            1970: (MAY, 8),
            1971: (MAY, 7),
            1972: (MAY, 8),
            1973: (MAY, 7),
            1974: (MAY, 8),
            1975: (MAY, 7),
            1976: (MAY, 10),
            1977: (MAY, 12),
            1978: (MAY, 11),
            1979: (MAY, 7),
            1980: (MAY, 14),
            1981: (MAY, 7),
            1982: (MAY, 19),
            1983: (MAY, 11),
            1984: (MAY, 10),
            1985: (MAY, 9),
            1986: (MAY, 9),
            1987: (MAY, 8),
            1988: (MAY, 11),
            1989: (MAY, 11),
            1990: (MAY, 11),
            1991: (MAY, 10),
            1992: (MAY, 14),
            1993: (MAY, 17),
            1994: (MAY, 11),
            1995: (MAY, 10),
            1996: (MAY, 16),
            1997: (MAY, 9),
            1998: (MAY, 8),
            # Not a holiday in 1999 date, was held on MAY, 14.
            2000: (MAY, 15),
            2001: (MAY, 16),
            2002: (MAY, 9),
            2003: (MAY, 8),
            2004: (MAY, 7),
            2005: (MAY, 11),
            2006: (MAY, 11),
            2007: (MAY, 10),
            2008: (MAY, 9),
            2009: (MAY, 11),
            2010: (MAY, 13),
            2011: (MAY, 13),
            2012: (MAY, 9),
            2013: (MAY, 13),
            2014: (MAY, 9),
            2015: (MAY, 13),
            2016: (MAY, 9),
            2017: (MAY, 12),
            2018: (MAY, 14),
            2019: (MAY, 9),
            2020: (MAY, 11),
            2021: (MAY, 10),
            2022: (MAY, 13),
            2023: (MAY, 17),
            2024: (MAY, 10),
            2025: (MAY, 9),
            2026: (MAY, 13),
        }
        if 1960 <= self._year <= 2026 and self._year != 1999:
            self._add_observed(
                # Royal Ploughing Ceremony.
                self._add_holiday(tr("วันพืชมงคล"), raeknakhwan_dates.get(self._year))
            )

    def _populate_school_holidays(self):
        # วันครู
        # Status: In-Use.
        # Started in 1957.
        # Only applies to Ministry of Education (Students, Teachers, etc.), no in-lieus are given.

        if self._year >= 1957:
            # Teacher's Day.
            self._add_holiday_jan_16(tr("วันครู"))

    def _populate_workday_holidays(self):
        # These are classes as "วันสำคัญ" (Date of National Observance) by the government
        # but are not holidays.

        if self._year >= 1948:
            # วันทหารผ่านศึก
            # Status: In-Use.
            # Started in 1948.

            # Thai Veterans Day.
            self._add_holiday_feb_3(tr("วันทหารผ่านศึก"))

        if self._year >= 1982:
            # วันวิทยาศาสตร์แห่งชาติ
            # Status: In-Use.
            # Started in 1982.

            # National Science Day.
            self._add_holiday_aug_18(tr("วันวิทยาศาสตร์แห่งชาติ"))

        if self._year >= 1985:
            # วันศิลปินแห่งชาติ
            # Status: In-Use.
            # Started in 1985.

            # National Artist Day.
            self._add_holiday_feb_26(tr("วันศิลปินแห่งชาติ"))

        if self._year >= 1989:
            # วันสตรีสากล
            # Status: In-Use.
            # Started in 1989.

            # International Women's Day.
            self._add_womens_day(tr("วันสตรีสากล"))

        if self._year >= 1990:
            # วันอนุรักษ์ทรัพยากรป่าไม้ของชาติ
            # Status: In-Use.
            # Started in 1990.

            # National Forest Conservation Day.
            self._add_holiday_jan_14(tr("วันอนุรักษ์ทรัพยากรป่าไม้ของชาติ"))

            # วันพ่อขุนรามคำแหงมหาราช
            # Status: In-Use.
            # Started in 1990.

            # HM King Ramkhamhaeng Memorial Day.
            self._add_holiday_jan_17(tr("วันพ่อขุนรามคำแหงมหาราช"))

        if self._year >= 1995:
            # วันการบินแห่งชาติ
            # Status: In-Use.
            # Started in 1995.

            # National Aviation Day.
            self._add_holiday_jan_13(tr("วันการบินแห่งชาติ"))

        if self._year >= 2017:
            # วันพระราชทานธงชาติไทย
            # Status: In-Use.
            # Started in 2017.

            # Thai National Flag Day.
            self._add_holiday_sep_28(tr("วันพระราชทานธงชาติไทย"))

        # วันลอยกระทง
        # Status: In-Use.
        # Started in 1914.

        # Loy Krathong.
        self._add_loy_krathong(tr("วันลอยกระทง"))


class TH(Thailand):
    pass


class THA(Thailand):
    pass


class ThailandStaticHolidays:
    """
    วันหยุดพิเศษ (เพิ่มเติม) - see Bank of Thailand's DB for Cross-Check.

    Special Bank Holidays Pre-1992:
       * [HM Queen Rambai Barni's Royal Cremation Ceremony](https://web.archive.org/web/20250428140115/https://ratchakitcha.soc.go.th/documents/1560949.pdf)
    """

    # Special In Lieu Holiday.
    thai_special_in_lieu_holidays = tr("วันหยุดชดเชย")
    # Thai Election Day.
    thai_election = tr("วันเลือกตั้ง")
    # Bridge Public Holiday.
    thai_bridge_public_holiday = tr("วันหยุดพิเศษ (เพิ่มเติม)")

    # Special Cases.

    # HM King Bhumibol Adulyadej's 60th Anniversary of Accession Event.
    rama_ix_sixty_accession = tr("พระราชพิธีฉลองสิริราชสมบัติครบ 60 ปี พ.ศ. 2549")
    # Emergency Lockdown (Thai Political Unrest).
    thai_political_emergency_lockdown = tr("วันหยุดพิเศษ (การเมือง)")
    # Emergency Lockdown (2011 Thailand Floods).
    thai_flood_2011_emergency_lockdown = tr("วันหยุดพิเศษ (มหาอุทกภัย พ.ศ. 2554)")
    # Songkran Festival.
    songkran_festival = tr("วันสงกรานต์")

    special_bank_holidays = {
        # HM Queen Rambai Barni's Royal Cremation Ceremony.
        1985: (APR, 9, tr("วันพระราชพิธีถวายพระเพลิงพระบรมศพสมเด็จพระนางเจ้ารำไพพรรณี"))
    }
    special_public_holidays = {
        # 1992-1994 (include In Lieus, Checked with Bank of Thailand Data).
        # 1995-1997 (Bank of Thailand Data).
        # 1998-2000 (include In Lieus, Checked with Bank of Thailand Data).
        # From 2001 Onwards (Checked with Bank of Thailand Data).
        1992: (
            (MAY, 18, thai_special_in_lieu_holidays),
            (DEC, 7, thai_special_in_lieu_holidays),
        ),
        1993: (
            (MAR, 8, thai_special_in_lieu_holidays),
            (MAY, 3, thai_special_in_lieu_holidays),
            (OCT, 25, thai_special_in_lieu_holidays),
            (DEC, 6, thai_special_in_lieu_holidays),
        ),
        1994: (
            (JAN, 3, thai_special_in_lieu_holidays),
            (MAY, 2, thai_special_in_lieu_holidays),
            (JUL, 25, thai_special_in_lieu_holidays),
            (OCT, 24, thai_special_in_lieu_holidays),
            (DEC, 12, thai_special_in_lieu_holidays),
        ),
        # HM King Bhumibol Adulyadej's Golden Jubilee.
        1996: (JUN, 10, tr("พระราชพิธีกาญจนาภิเษก พ.ศ. 2539")),
        1998: (
            (MAY, 11, thai_special_in_lieu_holidays),
            (DEC, 7, thai_special_in_lieu_holidays),
        ),
        1999: (
            (MAY, 3, thai_special_in_lieu_holidays),
            (MAY, 31, thai_special_in_lieu_holidays),
            (OCT, 25, thai_special_in_lieu_holidays),
            (DEC, 6, thai_special_in_lieu_holidays),
        ),
        2000: (
            (JAN, 3, thai_special_in_lieu_holidays),
            (FEB, 21, thai_special_in_lieu_holidays),
            (AUG, 14, thai_special_in_lieu_holidays),
            (DEC, 11, thai_special_in_lieu_holidays),
            (DEC, 29, thai_election),
        ),
        2006: (
            (APR, 19, thai_election),
            (JUN, 9, rama_ix_sixty_accession),
            (JUN, 12, rama_ix_sixty_accession),
            (JUN, 13, rama_ix_sixty_accession),
            # Emergency Lockdown (Thai Military Coup d'état).
            (SEP, 20, tr("วันหยุดพิเศษ (คมช.)")),
        ),
        2009: (
            (JAN, 2, thai_bridge_public_holiday),
            (APR, 10, thai_political_emergency_lockdown),
            (APR, 16, thai_political_emergency_lockdown),
            (APR, 17, thai_political_emergency_lockdown),
            (JUL, 6, thai_bridge_public_holiday),
        ),
        2010: (
            (MAY, 20, thai_bridge_public_holiday),
            (MAY, 21, thai_bridge_public_holiday),
            (AUG, 13, thai_bridge_public_holiday),
        ),
        2011: (
            (MAY, 16, thai_bridge_public_holiday),
            (OCT, 27, thai_flood_2011_emergency_lockdown),
            (OCT, 28, thai_flood_2011_emergency_lockdown),
            (OCT, 29, thai_flood_2011_emergency_lockdown),
            (OCT, 30, thai_flood_2011_emergency_lockdown),
            (OCT, 31, thai_flood_2011_emergency_lockdown),
        ),
        2012: (APR, 9, thai_bridge_public_holiday),
        2013: (DEC, 30, thai_bridge_public_holiday),
        2014: (AUG, 11, thai_bridge_public_holiday),
        2015: (
            (JAN, 2, thai_bridge_public_holiday),
            (MAY, 4, thai_bridge_public_holiday),
        ),
        2016: (
            (MAY, 6, thai_bridge_public_holiday),
            (JUL, 18, thai_bridge_public_holiday),
            # Day of Mourning for HM King Bhumibol Adulyadej.
            (OCT, 14, tr("วันหยุดพิเศษ (ร่วมถวายอาลัย ส่งดวงพระวิญญาณพระบรมศพ)")),
        ),
        # HM King Bhumibol Adulyadej's Royal Cremation Ceremony.
        2017: (OCT, 26, tr("วันพระราชพิธีถวายพระเพลิงพระบรมศพพระบาทสมเด็จพระปรมินทรมหาภูมิพลอดุลยเดช")),
        # HM King Maha Vajiralongkorn's Coronation Celebrations.
        2019: (MAY, 6, tr("พระราชพิธีบรมราชาภิเษก พระบาทสมเด็จพระวชิรเกล้าเจ้าอยู่หัว")),
        2020: (
            (NOV, 19, thai_bridge_public_holiday),
            (NOV, 20, thai_bridge_public_holiday),
            (DEC, 11, thai_bridge_public_holiday),
        ),
        2021: (
            (FEB, 12, thai_bridge_public_holiday),
            (APR, 12, thai_bridge_public_holiday),
            (SEP, 24, thai_bridge_public_holiday),
        ),
        2022: (
            (JUL, 15, thai_bridge_public_holiday),
            (JUL, 29, thai_bridge_public_holiday),
            (OCT, 14, thai_bridge_public_holiday),
            (DEC, 30, thai_bridge_public_holiday),
        ),
        2023: (
            (MAY, 5, thai_bridge_public_holiday),
            (JUL, 31, thai_bridge_public_holiday),
            (DEC, 29, thai_bridge_public_holiday),
        ),
        2024: (
            (APR, 12, thai_bridge_public_holiday),
            (DEC, 30, thai_bridge_public_holiday),
        ),
        2025: (
            (JUN, 2, thai_bridge_public_holiday),
            (AUG, 11, thai_bridge_public_holiday),
        ),
        2026: (JAN, 2, thai_bridge_public_holiday),
    }
    # Royal Ploughing Ceremony.
    special_workday_holidays = {1999: (MAY, 14, tr("วันพืชมงคล"))}

    special_public_holidays_observed = {
        2007: (DEC, 24, thai_election),
        2020: (
            (JUL, 27, songkran_festival),
            (SEP, 4, songkran_festival),
            (SEP, 7, songkran_festival),
        ),
    }
