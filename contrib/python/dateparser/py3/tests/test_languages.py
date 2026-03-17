import logging
from datetime import datetime
from io import StringIO

import pytest
from parameterized import param, parameterized

from dateparser import parse
from dateparser.conf import apply_settings, settings
from dateparser.date import DateDataParser
from dateparser.languages import Locale, default_loader
from dateparser.languages.validation import LanguageValidator
from dateparser.search import search_dates
from dateparser.search.detection import AutoDetectLanguage, ExactLanguages
from dateparser.utils import normalize_unicode
from tests import BaseTestCase


class TestLocaleTranslation:
    @pytest.mark.parametrize(
        "date_string,expected,locale,keep_formatting",
        [
            (
                "December 04, 1999, 11:04:59 PM",
                "december 04, 1999, 11:04:59 pm",
                "en",
                True,
            ),
            (
                "December 04, 1999, 11:04:59 PM",
                "december 04 1999 11:04:59 pm",
                "en",
                False,
            ),
            ("23 März, 18:37", "23 march, 18:37", "de", True),
            ("23 März 18:37", "23 march 18:37", "de", False),
        ],
    )
    def test_keep_formatting(self, date_string, expected, locale, keep_formatting):
        result = default_loader.get_locale(locale).translate(
            date_string=date_string, keep_formatting=keep_formatting, settings=settings
        )
        print(result)
        assert expected == result


class TestBundledLanguages(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.language = NotImplemented
        self.datetime_string = NotImplemented
        self.translation = NotImplemented
        self.tokens = NotImplemented
        self.result = NotImplemented
        self.settings = NotImplemented

    @parameterized.expand(
        [
            param("en", "Sep 03 2014", "september 03 2014"),
            param("en", "friday, 03 september 2014", "friday 03 september 2014"),
            # Chinese
            param("zh", "1年11个月", "1 year 11 month"),
            param("zh", "1年11個月", "1 year 11 month"),
            param("zh", "2015年04月08日10点05", "2015-04-08 10:05"),
            param("zh", "2015年04月08日10:05", "2015-04-08 10:05"),
            param("zh", "2013年04月08日", "2013-04-08"),
            param("zh", "周一", "monday"),
            param("zh", "礼拜一", "monday"),
            param("zh", "周二", "tuesday"),
            param("zh", "礼拜二", "tuesday"),
            param("zh", "周三", "wednesday"),
            param("zh", "礼拜三", "wednesday"),
            param("zh", "星期日 2015年04月08日10:05", "sunday 2015-04-08 10:05"),
            param("zh", "周六 2013年04月08日", "saturday 2013-04-08"),
            param("zh", "下午3:30", "3:30 pm"),
            param("zh", "凌晨3:30", "3:30 am"),
            param("zh", "中午", "12:00"),
            # French
            param("fr", "20 Février 2012", "20 february 2012"),
            param("fr", "Mercredi 19 Novembre 2013", "wednesday 19 november 2013"),
            param("fr", "18 octobre 2012 à 19 h 21 min", "18 october 2012  19:21"),
            # German
            param("de", "29. Juni 2007", "29. june 2007"),
            param("de", "Montag 5 Januar, 2015", "monday 5 january 2015"),
            param("de", "vor einer Woche", "1 week ago"),
            param("de", "in zwei Monaten", "in 2 month"),
            param("de", "übermorgen", "in 2 day"),
            param("de", "3 mrz 1999", "3 march 1999"),
            # Hungarian
            param("hu", "2016 augusztus 11.", "2016 august 11."),
            param("hu", "2016-08-13 szombat 10:21", "2016-08-13 saturday 10:21"),
            param(
                "hu",
                "2016. augusztus 14. vasárnap 10:21",
                "2016. august 14. sunday 10:21",
            ),
            param("hu", "hétfő", "monday"),
            param("hu", "tegnapelőtt", "2 day ago"),
            param("hu", "ma", "0 day ago"),
            param("hu", "2 hónappal ezelőtt", "2 month ago"),
            param(
                "hu", "2016-08-13 szombat 10:21 GMT", "2016-08-13 saturday 10:21 GMT"
            ),
            # Spanish
            param("es", "Miércoles 31 Diciembre 2014", "wednesday 31 december 2014"),
            # Italian
            param("it", "Giovedi Maggio 29 2013", "thursday may 29 2013"),
            param("it", "19 Luglio 2013", "19 july 2013"),
            # Portuguese
            param("pt", "22 de dezembro de 2014 às 02:38", "22  december  2014  02:38"),
            # Russian
            param("ru", "5 августа 2014 г. в 12:00", "5 august 2014 year.  12:00"),
            # Turkish
            param("tr", "2 Ocak 2015 Cuma, 16:49", "2 january 2015 friday 16:49"),
            # Czech
            param("cs", "22. prosinec 2014 v 2:38", "22. december 2014  2:38"),
            # Dutch
            param(
                "nl",
                "maandag 22 december 2014 om 2:38",
                "monday 22 december 2014  2:38",
            ),
            # Romanian
            param("ro", "22 Decembrie 2014 la 02:38", "22 december 2014  02:38"),
            # Polish
            param("pl", "4 stycznia o 13:50", "4 january  13:50"),
            param("pl", "29 listopada 2014 o 08:40", "29 november 2014  08:40"),
            # Ukrainian
            param("uk", "30 листопада 2013 о 04:27", "30 november 2013  04:27"),
            param("uk", "22 верес 2021 о 07:37", "22 september 2021  07:37"),
            param("uk", "28 лютого 2020 року об 11:57", "28 february 2020 year  11:57"),
            param(
                "uk",
                "середу, 28 лютого 2020 року об 11:57",
                "wednesday 28 february 2020 year  11:57",
            ),
            param(
                "uk",
                "понед, 12 вересня 2022 року об 09:22",
                "monday 12 september 2022 year  09:22",
            ),
            # Belarusian
            param("be", "5 снежня 2015 г. у 12:00", "5 december 2015 year.  12:00"),
            param("be", "11 верасня 2015 г. у 12:11", "11 september 2015 year.  12:11"),
            param("be", "3 стд 2015 г. у 10:33", "3 january 2015 year.  10:33"),
            # Arabic
            param("ar", "6 يناير، 2015، الساعة 05:16 مساءً", "6 january 2015 05:16 pm"),
            param("ar", "7 يناير، 2015، الساعة 11:00 صباحاً", "7 january 2015 11:00 am"),
            # Vietnamese
            param("vi", "Thứ Năm, ngày 8 tháng 1 năm 2015", "thursday 8 january 2015"),
            param("vi", "Thứ Tư, 07/01/2015 | 22:34", "wednesday 07/01/2015  22:34"),
            param("vi", "9 Tháng 1 2015 lúc 15:08", "9 january 2015  15:08"),
            # Thai
            param(
                "th",
                "เมื่อ กุมภาพันธ์ 09, 2015, 09:27:57 AM",
                "february 09 2015 09:27:57 am",
            ),
            param(
                "th", "เมื่อ กรกฎาคม 05, 2012, 01:18:06 AM", "july 05 2012 01:18:06 am"
            ),
            param(
                "th",
                "วันเสาร์ที่ 16 ธันวาคม 2560 7:00 pm",
                "saturday 16 december 2560 7:00 pm",
            ),
            param(
                "th",
                "วันอาทิตย์ที่ 17 ธันวาคม 2560 6:00 pm",
                "sunday 17 december 2560 6:00 pm",
            ),
            # Tagalog
            param("tl", "Biyernes Hulyo 3, 2015", "friday july 3 2015"),
            param("tl", "Pebrero 5, 2015 7:00 pm", "february 5 2015 7:00 pm"),
            # Indonesian
            param("id", "06 Sep 2015", "06 september 2015"),
            param("id", "07 Feb 2015 20:15", "07 february 2015 20:15"),
            param("id", "Minggu, 18 Mar 2018 07:30", "sunday 18 march 2018 07:30"),
            param("id", "3 minggu yang lalu", "3 week ago"),
            param("id", "5 minggu", "5 week"),
            # Miscellaneous
            param("en", "2014-12-12T12:33:39-08:00", "2014-12-12 12:33:39-08:00"),
            param("en", "2014-10-15T16:12:20+00:00", "2014-10-15 16:12:20+00:00"),
            param("en", "28 Oct 2014 16:39:01 +0000", "28 october 2014 16:39:01 +0000"),
            param("es", "13 Febrero 2015 a las 23:00", "13 february 2015  23:00"),
            # Danish
            param("da", "Sep 03 2014", "september 03 2014"),
            param("da", "fredag, 03 september 2014", "friday 03 september 2014"),
            param("da", "fredag d. 3 september 2014", "friday  3 september 2014"),
            # Finnish
            param("fi", "maanantai tammikuu 16, 2015", "monday january 16 2015"),
            param("fi", "ma tammi 16, 2015", "monday january 16 2015"),
            param("fi", "tiistai helmikuu 16, 2015", "tuesday february 16 2015"),
            param("fi", "ti helmi 16, 2015", "tuesday february 16 2015"),
            param("fi", "keskiviikko maaliskuu 16, 2015", "wednesday march 16 2015"),
            param("fi", "ke maalis 16, 2015", "wednesday march 16 2015"),
            param("fi", "torstai huhtikuu 16, 2015", "thursday april 16 2015"),
            param("fi", "to huhti 16, 2015", "thursday april 16 2015"),
            param("fi", "perjantai toukokuu 16, 2015", "friday may 16 2015"),
            param("fi", "pe touko 16, 2015", "friday may 16 2015"),
            param("fi", "lauantai kesäkuu 16, 2015", "saturday june 16 2015"),
            param("fi", "la kesä 16, 2015", "saturday june 16 2015"),
            param("fi", "sunnuntai heinäkuu 16, 2015", "sunday july 16 2015"),
            param("fi", "su heinä 16, 2015", "sunday july 16 2015"),
            param("fi", "su elokuu 16, 2015", "sunday august 16 2015"),
            param("fi", "su elo 16, 2015", "sunday august 16 2015"),
            param("fi", "su syyskuu 16, 2015", "sunday september 16 2015"),
            param("fi", "su syys 16, 2015", "sunday september 16 2015"),
            param("fi", "su lokakuu 16, 2015", "sunday october 16 2015"),
            param("fi", "su loka 16, 2015", "sunday october 16 2015"),
            param("fi", "su marraskuu 16, 2015", "sunday november 16 2015"),
            param("fi", "su marras 16, 2015", "sunday november 16 2015"),
            param("fi", "su joulukuu 16, 2015", "sunday december 16 2015"),
            param("fi", "su joulu 16, 2015", "sunday december 16 2015"),
            param("fi", "1. tammikuuta, 2016", "1. january 2016"),
            param("fi", "tiistaina, 27. lokakuuta 2015", "tuesday 27. october 2015"),
            param("fi", "28 maalis klo 9:37", "28 march  9:37"),
            # Japanese
            param("ja", "午後3時", "pm 3:00"),
            param("ja", "2時", "2:00"),
            param("ja", "11時42分", "11:42"),
            param("ja", "3ヶ月", "3 month"),
            param("ja", "約53か月前", "53 month ago"),
            param("ja", "3月", "march"),
            param("ja", "十二月", "december"),
            param("ja", "2月10日", "2-10"),
            param("ja", "2013年2月", "2013 year february"),
            param("ja", "2013年04月08日", "2013-04-08"),
            param("ja", "2016年03月24日 木曜日 10時05分", "2016-03-24 thursday 10:05"),
            param("ja", "2016年3月20日 21時40分", "2016-3-20 21:40"),
            param("ja", "2016年03月21日 23時05分11秒", "2016-03-21 23:05:11"),
            param("ja", "2016年3月21日(月) 14時48分", "2016-3-21 monday 14:48"),
            param("ja", "2016年3月20日(日) 21時40分", "2016-3-20 sunday 21:40"),
            param("ja", "2016年3月20日 (日) 21時40分", "2016-3-20 sunday 21:40"),
            param("ja", "正午", "12:00"),
            param("ja", "明日の13時20分", "in 1 day 13:20"),
            # Hebrew
            param("he", "20 לאפריל 2012", "20 april 2012"),
            param("he", "יום רביעי ה-19 בנובמבר 2013", "wednesday 19 november 2013"),
            param("he", "18 לאוקטובר 2012 בשעה 19:21", "18 october 2012  19:21"),
            param("he", "יום ה' 6/10/2016", "thursday 6/10/2016"),
            param("he", "חצות", "12 am"),
            param("he", "1 אחר חצות", "1 am"),
            param("he", "3 לפנות בוקר", "3 am"),
            param("he", "3 בבוקר", "3 am"),
            param("he", "3 בצהריים", "3 pm"),
            param("he", "6 לפנות ערב", "6 pm"),
            param("he", "6 אחרי הצהריים", "6 pm"),
            param("he", "6 אחרי הצהרים", "6 pm"),
            # Bangla
            param("bn", "সেপ্টেম্বর 03 2014", "september 03 2014"),
            param("bn", "শুক্রবার, 03 সেপ্টেম্বর 2014", "friday 03 september 2014"),
            # Hindi
            param("hi", "सोमवार 13 जून 1998", "monday 13 june 1998"),
            param("hi", "मंगल 16 1786 12:18", "tuesday 16 1786 12:18"),
            param("hi", "शनि 11 अप्रैल 2002 03:09", "saturday 11 april 2002 03:09"),
            # Swedish
            param("sv", "Sept 03 2014", "september 03 2014"),
            param("sv", "fredag, 03 september 2014", "friday 03 september 2014"),
            # af
            param("af", "5 Mei 2017", "5 may 2017"),
            param(
                "af", "maandag, Augustus 15 2005 10 vm", "monday august 15 2005 10 am"
            ),
            # agq
            param("agq", "12 ndzɔ̀ŋɔ̀tɨ̀fʉ̀ghàdzughù 1999", "12 september 1999"),
            param("agq", "tsuʔndzɨkɔʔɔ 14 see 10 ak", "saturday 14 may 10 pm"),
            # ak
            param("ak", "esusow aketseaba-kɔtɔnimba", "may"),
            param("ak", "8 mumu-ɔpɛnimba ben", "8 december tuesday"),
            # am
            param("am", "ፌብሩወሪ 22 8:00 ጥዋት", "february 22 8:00 am"),
            param("am", "ኖቬም 10", "november 10"),
            # as
            param("as", "17 জানুৱাৰী 1885", "17 january 1885"),
            param("as", "বৃহষ্পতিবাৰ 1 জুলাই 2009", "thursday 1 july 2009"),
            # asa
            param("asa", "12 julai 1879 08:00 ichamthi", "12 july 1879 08:00 pm"),
            param(
                "asa",
                "jpi 2 desemba 2007 01:00 icheheavo",
                "sunday 2 december 2007 01:00 am",
            ),
            # ast
            param("ast", "d'ochobre 11, 11:00 de la mañana", "october 11 11:00 am"),
            param("ast", "vienres 19 payares 1 tarde", "friday 19 november 1 pm"),
            # az-Cyrl
            param("az-Cyrl", "7 феврал 1788 05:30 пм", "7 february 1788 05:30 pm"),
            param("az-Cyrl", "чәршәнбә ахшамы ијл 14", "tuesday july 14"),
            # az-Latn
            param("az-Latn", "yanvar 13 şənbə", "january 13 saturday"),
            param("az-Latn", "b noy 12", "sunday november 12"),
            # az
            param("az", "17 iyn 2000 cümə axşamı", "17 june 2000 thursday"),
            param("az", "22 sentyabr 2003 bazar ertəsi", "22 september 2003 monday"),
            # bas
            param("bas", "1906 6 hìlòndɛ̀ ŋgwà njaŋgumba", "1906 6 june monday"),
            param("bas", "ŋgwà kɔɔ, 11 màtùmb 5 i ɓugajɔp", "friday 11 march 5 pm"),
            # be
            param("be", "13 лютага 1913", "13 february 1913"),
            param("be", "жнівень 12, чацвер", "august 12 thursday"),
            # bem
            param(
                "bem",
                "palichimo 12 machi 2015 11:00 uluchelo",
                "monday 12 march 2015 11:00 am",
            ),
            param("bem", "5 epreo 2000 pa mulungu", "5 april 2000 sunday"),
            # bez
            param(
                "bez",
                "1 pa mwedzi gwa hutala 1889 10:00 pamilau",
                "1 january 1889 10:00 am",
            ),
            param("bez", "31 pa mwedzi gwa kumi na mbili hit", "31 december thursday"),
            # bm
            param("bm", "12 ɔkutɔburu 2001 araba", "12 october 2001 wednesday"),
            param("bm", "alamisa 15 uti 1998", "thursday 15 august 1998"),
            # bo
            param("bo", "ཟླ་བ་བཅུ་གཅིག་པ་ 18", "november 18"),
            param(
                "bo",
                "གཟའ་ཕུར་བུ་ 12 ཟླ་བ་བཅུ་པ་ 1879 10:15 ཕྱི་དྲོ་",
                "thursday 12 october 1879 10:15 pm",
            ),
            # br
            param(
                "br", "merc'her c'hwevrer 12 07:32 gm", "wednesday february 12 07:32 pm"
            ),
            param("br", "10 gwengolo 2002 sadorn", "10 september 2002 saturday"),
            # brx
            param("brx", "6 अखथबर 2019 10:00 बेलासे", "6 october 2019 10:00 pm"),
            param("brx", "बिसथि 8 फेब्रुवारी", "thursday 8 february"),
            # bs-Cyrl
            param("bs-Cyrl", "2 септембар 2000, четвртак", "2 september 2000 thursday"),
            param("bs-Cyrl", "1 јули 1987 9:25 поподне", "1 july 1987 9:25 pm"),
            # bs-Latn
            param("bs-Latn", "23 septembar 1879, petak", "23 september 1879 friday"),
            param(
                "bs-Latn",
                "subota 1 avg 2009 02:27 popodne",
                "saturday 1 august 2009 02:27 pm",
            ),
            # bs
            param("bs", "10 maj 2020 utorak", "10 may 2020 tuesday"),
            param("bs", "ponedjeljak, 1989 2 januar", "monday 1989 2 january"),
            # ca
            param("ca", "14 d'abril 1980 diumenge", "14 april 1980 sunday"),
            param("ca", "3 de novembre 2004 dj", "3 november 2004 thursday"),
            # ce
            param("ce", "6 январь 1987 пӏераскан де", "6 january 1987 friday"),
            param("ce", "оршотан де 3 июль 1890", "monday 3 july 1890"),
            # cgg
            param("cgg", "20 okwakataana 2027 orwamukaaga", "20 may 2027 saturday"),
            param("cgg", "okwaikumi na ibiri 12 oks", "december 12 wednesday"),
            # chr
            param(
                "chr",
                "ᎤᎾᏙᏓᏉᏅᎯ 16 ᏕᎭᎷᏱ 1562 11:16 ᏒᎯᏱᎢᏗᏢ",
                "monday 16 june 1562 11:16 pm",
            ),
            param("chr", "13 ᎠᏂᏍᎬᏘ ᎤᎾᏙᏓᏈᏕᎾ 8:00 ᏌᎾᎴ", "13 may saturday 8:00 am"),
            # cy
            param(
                "cy",
                "dydd sadwrn 27 chwefror 1990 9 yb",
                "saturday 27 february 1990 9 am",
            ),
            param("cy", "19 gorff 2000 dydd gwener", "19 july 2000 friday"),
            # dav
            param("dav", "mori ghwa kawi 24 kuramuka kana", "february 24 thursday"),
            param("dav", "11 ike 4 luma lwa p", "11 september 4 pm"),
            # dje
            param("dje", "2 žuweŋ 2030 alz 11 zaarikay b", "2 june 2030 friday 11 pm"),
            param("dje", "sektanbur 12 alarba", "september 12 wednesday"),
            # dsb
            param("dsb", "njeźela julija 15 2 wótpołdnja", "sunday july 15 2 pm"),
            param("dsb", "awgusta 10 sob", "august 10 saturday"),
            # dua
            param("dua", "madiɓɛ́díɓɛ́ 15 ɗónɛsú 7 idiɓa", "july 15 friday 7 am"),
            param("dua", "éti 12 tiníní", "sunday 12 november"),
            # dyo
            param("dyo", "mee 1 2000 talata", "may 1 2000 tuesday"),
            param("dyo", "arjuma de 10", "friday december 10"),
            # dz
            param("dz", "ཟླ་བཅུ་གཅིག་པ་ 10 གཟའ་ཉི་མ་", "november 10 saturday"),
            param("dz", "མིར་ 2 སྤྱི་ཟླ་དྲུག་པ 2009 2 ཕྱི་ཆ་", "monday 2 june 2009 2 pm"),
            # ebu
            param(
                "ebu", "mweri wa gatantatũ 11 maa 08:05 ut", "june 11 friday 08:05 pm"
            ),
            param("ebu", "2 igi 1998 njumamothii", "2 december 1998 saturday"),
            # ee
            param(
                "ee", "5 afɔfĩe 2009 05:05 ɣetrɔ kɔsiɖa", "5 april 2009 05:05 pm sunday"
            ),
            param("ee", "yawoɖa 1890 deasiamime 23", "thursday 1890 august 23"),
            # el
            param("el", "απρίλιος 13 09:09 μμ", "april 13 09:09 pm"),
            param("el", "1 ιούνιος 2002 07:17 πμ", "1 june 2002 07:17 am"),
            # eo
            param("eo", "12 aŭgusto 1887 06:06 atm", "12 august 1887 06:06 am"),
            param("eo", "vendredo 10 sep 1957", "friday 10 september 1957"),
            # et
            param(
                "et", "3 juuni 2001 neljapäev 07:09 pm", "3 june 2001 thursday 07:09 pm"
            ),
            param("et", "7 veebr 2004", "7 february 2004"),
            # eu
            param("eu", "1 urtarrila 1990 asteazkena", "1 january 1990 wednesday"),
            param("eu", "ig 30 martxoa 1905", "sunday 30 march 1905"),
            # ewo
            param("ewo", "ngɔn lála 13 08:07 ngəgógəle", "march 13 08:07 pm"),
            param(
                "ewo",
                "séradé ngad 12 1915 2:00 ngəgógəle",
                "saturday november 12 1915 2:00 pm",
            ),
            # ff
            param(
                "ff",
                "1 colte 1976 hoore-biir 04:15 subaka",
                "1 february 1976 saturday 04:15 am",
            ),
            param("ff", "naasaande 3 yar 02:00 kikiiɗe", "thursday 3 october 02:00 pm"),
            # fil
            param("fil", "2 setyembre 1880 biyernes", "2 september 1880 friday"),
            param("fil", "15 ago 1909 lun", "15 august 1909 monday"),
            # fo
            param("fo", "mánadagur 30 januar 1976", "monday 30 january 1976"),
            param("fo", "2 apríl 1890 fríggjadagur", "2 april 1890 friday"),
            # fur
            param("fur", "12 avost 1990 domenie", "12 august 1990 sunday"),
            param(
                "fur",
                "miercus 5 fev 1990 10:10 p",
                "wednesday 5 february 1990 10:10 pm",
            ),
            # fy
            param("fy", "febrewaris 2 1987 freed", "february 2 1987 friday"),
            param("fy", "to 20 maaie 2010", "thursday 20 may 2010"),
            # ga
            param("ga", "1 bealtaine 2019 dé céadaoin", "1 may 2019 wednesday"),
            param(
                "ga", "deireadh fómhair 12 aoine 10:09 pm", "october 12 friday 10:09 pm"
            ),
            # gd
            param(
                "gd",
                "2 am faoilleach 1890 diardaoin 02:13 m",
                "2 january 1890 thursday 02:13 am",
            ),
            param(
                "gd", "did an t-ògmhios 15 1876 08:15 f", "sunday june 15 1876 08:15 pm"
            ),
            # gl
            param("gl", "1 xullo 2009 sáb", "1 july 2009 saturday"),
            param("gl", "martes 15 setembro 1980", "tuesday 15 september 1980"),
            # gsw
            param("gsw", "5 auguscht 1856 10:08 am namittag", "5 august 1856 10:08 pm"),
            param(
                "gsw",
                "ziischtig 13 dezämber 03:12 vormittag",
                "tuesday 13 december 03:12 am",
            ),
            # gu
            param("gu", "10 સપ્ટેમ્બર 2005 ગુરુવાર", "10 september 2005 thursday"),
            param("gu", "સોમવાર 1 જુલાઈ 1980", "monday 1 july 1980"),
            # guz
            param("guz", "apiriri 2 1789 chumatano", "april 2 1789 wednesday"),
            param(
                "guz", "esabato 11 cul 2000 10:19 ma", "saturday 11 july 2000 10:19 am"
            ),
            # gv
            param("gv", "3 toshiaght-arree 2023 jeh", "3 february 2023 friday"),
            param("gv", "1 m-souree 1999 jedoonee", "1 june 1999 sunday"),
            # ha
            param("ha", "18 yuni 1920 laraba", "18 june 1920 wednesday"),
            param("ha", "2 afi 1908 lit", "2 april 1908 monday"),
            # haw
            param("haw", "1 'apelila 1968 p6", "1 april 1968 saturday"),
            param("haw", "po'alima 29 'ok 1899", "friday 29 october 1899"),
            # hr
            param("hr", "2 ožujak 1980 pet", "2 march 1980 friday"),
            param("hr", "nedjelja 3 lis 1879", "sunday 3 october 1879"),
            param("hr", "06. travnja 2021.", "06. april 2021."),
            param("hr", "13. svibanj 2022. u 14:34", "13. may 2022.  14:34"),
            param("hr", "20. studenoga 2010. @ 07:28", "20. november 2010.  07:28"),
            param("hr", "13. studenog 1989.", "13. november 1989."),
            param("hr", "u listopadu 2056.", " october 2056."),
            param("hr", "u studenome 1654.", " november 1654."),
            param("hr", "u studenomu 2001.", " november 2001."),
            param("hr", "15. studenog 2007.", "15. november 2007."),
            # hsb
            param(
                "hsb",
                "5 měrc 1789 póndźela 11:13 popołdnju",
                "5 march 1789 monday 11:13 pm",
            ),
            param("hsb", "štwórtk 2000 awg 14", "thursday 2000 august 14"),
            # hy
            param(
                "hy",
                "2 դեկտեմբերի 2006 շբթ 02:00 կա",
                "2 december 2006 saturday 02:00 am",
            ),
            param("hy", "չորեքշաբթի մյս 17, 2009", "wednesday may 17 2009"),
            # ig
            param("ig", "1 ọgọọst 2001 wenezdee", "1 august 2001 wednesday"),
            param("ig", "mbọsị ụka 23 epr 1980", "sunday 23 april 1980"),
            # ii
            param("ii", "ꆏꊂꇖ 12 ꌕꆪ 1980", "thursday 12 march 1980"),
            param("ii", "ꉆꆪ 1 02:05 ꁯꋒ", "august 1 02:05 pm"),
            # is
            param("is", "þriðjudagur 15 júlí 2001", "tuesday 15 july 2001"),
            param("is", "fös 10 desember 08:17 fh", "friday 10 december 08:17 am"),
            # jgo
            param(
                "jgo",
                "pɛsaŋ pɛ́nɛ́pfúꞌú 15 10:16 ŋka mbɔ́t nji",
                "september 15 10:16 pm",
            ),
            param("jgo", "ápta mɔ́ndi 10 nduŋmbi saŋ 2009", "tuesday 10 january 2009"),
            # jmc
            param(
                "jmc",
                "2 aprilyi 2015 jumapilyi 03:10 kyiukonyi",
                "2 april 2015 sunday 03:10 pm",
            ),
            param("jmc", "alh 11 julyai 1987", "thursday 11 july 1987"),
            # kab
            param(
                "kab",
                "3 meɣres 1999 kuẓass 11:16 n tmeddit",
                "3 march 1999 wednesday 11:16 pm",
            ),
            param("kab", "1 yennayer 2004 sḍis", "1 january 2004 friday"),
            # kam
            param(
                "kam",
                "mwai wa katatũ 12 wa katano 09:18 ĩyawĩoo",
                "march 12 friday 09:18 pm",
            ),
            param("kam", "1 mwai wa ĩkumi na ilĩ 1789 wth", "1 december 1789 saturday"),
            # kde
            param(
                "kde",
                "mwedi wa nnyano na umo 12 1907 liduva litandi",
                "june 12 1907 saturday",
            ),
            param("kde", "2 mei 11:10 chilo ll6", "2 may 11:10 pm thursday"),
            # kea
            param("kea", "sigunda-fera 12 julhu 1902", "monday 12 july 1902"),
            param("kea", "2 diz 2005 kua", "2 december 2005 wednesday"),
            # khq
            param(
                "khq",
                "1 žanwiye 2019 ati 01:09 adduha",
                "1 january 2019 monday 01:09 am",
            ),
            param("khq", "alhamiisa 12 noowanbur 1908", "thursday 12 november 1908"),
            # ki
            param("ki", "1 mwere wa gatano 1980 09:12 hwaĩ-inĩ", "1 may 1980 09:12 pm"),
            param(
                "ki",
                "njumatana 2 wmw 2000 01:12 kiroko",
                "wednesday 2 november 2000 01:12 am",
            ),
            # kk
            param("kk", "3 маусым 1956 дс", "3 june 1956 monday"),
            param("kk", "жексенбі 12 қыркүйек 1890", "sunday 12 september 1890"),
            # kl
            param("kl", "2 martsi 2001 ataasinngorneq", "2 march 2001 monday"),
            param("kl", "pin 1 oktoberi 1901", "wednesday 1 october 1901"),
            # kln
            param(
                "kln",
                "3 ng'atyaato koang'wan 10:09 kooskoliny",
                "3 february thursday 10:09 pm",
            ),
            param(
                "kln", "kipsuunde nebo aeng' 14 2009 kos", "december 14 2009 wednesday"
            ),
            # kok
            param(
                "kok",
                "1 नोव्हेंबर 2000 आदित्यवार 01:19 मनं",
                "1 november 2000 sunday 01:19 pm",
            ),
            param("kok", "मंगळार 2 फेब्रुवारी 2003", "tuesday 2 february 2003"),
            # ksb
            param("ksb", "jumaamosi 1 ago 09:19 makeo", "saturday 1 august 09:19 am"),
            param("ksb", "3 febluali 1980 jmn", "3 february 1980 tuesday"),
            # ksf
            param(
                "ksf", "ŋwíí a ntɔ́ntɔ 3 1990 09:15 cɛɛ́nko", "january 3 1990 09:15 pm"
            ),
            param("ksf", "2 ŋ3 1789 jǝǝdí", "2 march 1789 thursday"),
            # ksh
            param(
                "ksh",
                "mohndaach 12 fäbrowa 2001 12:18 nm",
                "monday 12 february 2001 12:18 pm",
            ),
            param("ksh", "5 oujoß 12:17 uhr vörmiddaachs", "5 august 12:17 am"),
            # kw
            param("kw", "14 mis metheven 1980 dy yow", "14 june 1980 thursday"),
            param("kw", "mis kevardhu 2019 1 sad", "december 2019 1 saturday"),
            # ky
            param(
                "ky",
                "22 февраль 2025 01:12 түштөн кийинки",
                "22 february 2025 01:12 pm",
            ),
            param("ky", "шаршемби 11 авг 1908", "wednesday 11 august 1908"),
            # lag
            param("lag", "17 kʉvɨɨrɨ 2018 ijumáa", "17 august 2018 friday"),
            param("lag", "táatu 16 kwiinyi 1978", "monday 16 october 1978"),
            # lb
            param(
                "lb", "2 mäerz 2034 don 02:19 moies", "2 march 2034 thursday 02:19 am"
            ),
            param("lb", "samschdeg 15 abrëll", "saturday 15 april"),
            # lg
            param("lg", "sebuttemba 17 1980 lw6", "september 17 1980 saturday"),
            param("lg", "2 okitobba 2010 lwakusatu", "2 october 2010 wednesday"),
            # lkt
            param(
                "lkt", "18 čhaŋwápetȟo wí 2013 owáŋgyužažapi", "18 may 2013 saturday"
            ),
            param(
                "lkt", "1 tȟahékapšuŋ wí 1978 aŋpétuzaptaŋ", "1 december 1978 friday"
            ),
            # ln
            param("ln", "23 yan 2001 mokɔlɔ mwa mísáto", "23 january 2001 wednesday"),
            param(
                "ln",
                "mtn 17 sánzá ya zómi na míbalé 09:17 ntɔ́ngɔ́",
                "friday 17 december 09:17 am",
            ),
            # lo
            param("lo", "18 ພຶດສະພາ 1908 ວັນອາທິດ", "18 may 1908 sunday"),
            param("lo", "8 ກໍລະກົດ 2003 03:03 ຫຼັງທ່ຽງ", "8 july 2003 03:03 pm"),
            # lt
            param("lt", "15 gegužės 1970 trečiadienis", "15 may 1970 wednesday"),
            param("lt", "an 2 rugsėjo 09:18 priešpiet", "tuesday 2 september 09:18 am"),
            # lu
            param(
                "lu",
                "2 ciongo 2016 njw 02:16 dilolo",
                "2 january 2016 thursday 02:16 pm",
            ),
            param("lu", "16 lùshìkà 2009", "16 august 2009"),
            # luo
            param("luo", "15 dwe mar adek 1908 tan", "15 march 1908 thursday"),
            param("luo", "jumapil 3 dao 2008 01:12 ot", "sunday 3 july 2008 01:12 pm"),
            # luy
            param("luy", "23 juni 1970 murwa wa kanne", "23 june 1970 thursday"),
            param("luy", "jumatano, 5 aprili 1998", "wednesday 5 april 1998"),
            # lv
            param("lv", "14 jūnijs 2010 10:10 priekšpusdienā", "14 june 2010 10:10 am"),
            param(
                "lv",
                "24 okt 2000 piektdiena 11:11 pēcpusd",
                "24 october 2000 friday 11:11 pm",
            ),
            # mas
            param(
                "mas",
                "2 olodoyíóríê inkókúâ 1954 08:16 ɛnkakɛnyá",
                "2 april 1954 08:16 am",
            ),
            param(
                "mas",
                "15 ɔɛn 2032 alaámisi 02:13 ɛndámâ",
                "15 march 2032 thursday 02:13 pm",
            ),
            # mer
            param("mer", "1 mĩĩ 1924 wetano 10:05 ũg", "1 may 1924 friday 10:05 pm"),
            param("mer", "6 njuraĩ 1895 muramuko", "6 july 1895 monday"),
            # mfe
            param("mfe", "27 zilye 1988 merkredi", "27 july 1988 wednesday"),
            param("mfe", "lindi 3 oktob 1897", "monday 3 october 1897"),
            # mg
            param("mg", "1 martsa 1789 alakamisy", "1 march 1789 thursday"),
            param("mg", "5 aogositra 1911 zoma", "5 august 1911 friday"),
            # mgh
            param(
                "mgh",
                "sabato mweri wo unethanu 15 07:18 wichishu",
                "sunday may 15 07:18 am",
            ),
            param(
                "mgh",
                "2 tis 1789 jumamosi 08:17 mchochil'l",
                "2 september 1789 saturday 08:17 pm",
            ),
            # mgo
            param("mgo", "5 iməg mbegtug aneg 5", "5 january thursday"),
            param("mgo", "aneg 2 iməg zò 17 1908", "monday november 17 1908"),
            # mk
            param("mk", "4 септември 2009 09:18 претпл", "4 september 2009 09:18 am"),
            param(
                "mk",
                "вторник 10 август 1777 01:12 попл",
                "tuesday 10 august 1777 01:12 pm",
            ),
            # mn
            param(
                "mn", "дөрөвдүгээр сар 15 баасан 10:10 үө", "april 15 friday 10:10 am"
            ),
            param("mn", "12 9-р сар 2019 пүрэв", "12 september 2019 thursday"),
            # mr
            param(
                "mr",
                "16 फेब्रुवारी 1908 गुरु 02:03 मउ",
                "16 february 1908 thursday 02:03 pm",
            ),
            param("mr", "शनिवार 15 सप्टें 1888", "saturday 15 september 1888"),
            # ms
            param("ms", "4 mei 1768 jumaat 09:09 ptg", "4 may 1768 friday 09:09 pm"),
            param(
                "ms",
                "isnin 14 disember 2001 11:09 pg",
                "monday 14 december 2001 11:09 am",
            ),
            # mt
            param("mt", "3 frar 1998 il-ħadd", "3 february 1998 sunday"),
            param("mt", "16 awwissu 2019 erb", "16 august 2019 wednesday"),
            # mua
            param("mua", "1 cokcwaklaŋne 2014 comzyiiɗii", "1 february 2014 tuesday"),
            param(
                "mua",
                "fĩi yuru 17 1908 cze 10:08 lilli",
                "december 17 1908 saturday 10:08 pm",
            ),
            # naq
            param("naq", "20 ǂkhoesaob 1934 wunstaxtsees", "20 july 1934 wednesday"),
            param(
                "naq",
                "do 12 gamaǀaeb 1999 05:12 ǃuias",
                "thursday 12 june 1999 05:12 pm",
            ),
            # nb
            param("nb", "2 mars 1998 torsdag", "2 march 1998 thursday"),
            param("nb", "ons 15 des 2001", "wednesday 15 december 2001"),
            # nd
            param("nd", "19 nkwenkwezi 1967 mgqibelo", "19 may 1967 saturday"),
            param("nd", "sit 1 zibandlela 2011", "wednesday 1 january 2011"),
            # ne
            param(
                "ne",
                "1 फेब्रुअरी 2003 बिहिबार 09:18 अपराह्न्",
                "1 february 2003 thursday 09:18 pm",
            ),
            param("ne", "आइत् 4 अक्टोबर 1957", "sunday 4 october 1957"),
            # nl
            param("nl", "4 augustus 1678 zaterdag", "4 august 1678 saturday"),
            param("nl", "vr 27 juni 1997", "friday 27 june 1997"),
            # nmg
            param("nmg", "5 ngwɛn ńna 1897 sɔ́ndɔ mafú málal", "5 april 1897 wednesday"),
            param(
                "nmg",
                "mɔ́ndɔ 1 ng11 1678 04:15 kugú",
                "monday 1 november 1678 04:15 pm",
            ),
            # nn
            param(
                "nn",
                "tysdag 2 september 1897 01:12 fm",
                "tuesday 2 september 1897 01:12 am",
            ),
            param(
                "nn",
                "2 mai 2000 søndag 09:18 ettermiddag",
                "2 may 2000 sunday 09:18 pm",
            ),
            # nnh
            param(
                "nnh",
                "1 saŋ tsɛ̀ɛ cÿó màga lyɛ̌' 08:18 ncwònzém",
                "1 may saturday 08:18 pm",
            ),
            param("nnh", "3 saŋ lepyè shúm 1789 mvfò lyɛ̌'", "3 march 1789 monday"),
            # nus
            param(
                "nus",
                "7 kornyoot 2006 diɔ̱k lätni 01:12 tŋ",
                "7 june 2006 wednesday 01:12 pm",
            ),
            param("nus", "bäkɛl, 12 tio̱p in di̱i̱t 2008", "saturday 12 december 2008"),
            # nyn
            param("nyn", "5 okwakashatu 1980 okt", "5 march 1980 friday"),
            param("nyn", "2 kms 2087 sande", "2 july 2087 sunday"),
            # om
            param("om", "15 bitooteessa 1997 02:23 wb", "15 march 1997 02:23 pm"),
            param("om", "jimaata 13 gur 01:12 wd", "friday 13 february 01:12 am"),
            # os
            param("os", "хуыцаубон 1998 апрелы 12", "sunday 1998 april 12"),
            param("os", "1 ноя 1990 ӕртыццӕг", "1 november 1990 wednesday"),
            # pa-Guru
            param(
                "pa-Guru",
                "ਸ਼ਨਿੱਚਰਵਾਰ 4 ਫ਼ਰਵਰੀ 1989 01:12 ਬਾਦ",
                "saturday 4 february 1989 01:12 pm",
            ),
            param("pa-Guru", "2 ਅਕਤੂਬਰ 2015 ਸੋਮਵਾਰ", "2 october 2015 monday"),
            # pa
            param("pa", "2 ਅਗਸਤ 1682 ਸ਼ਨਿੱਚਰ", "2 august 1682 saturday"),
            param("pa", "12 ਅਕਤੂ 11:08 ਪੂਦੁ", "12 october 11:08 am"),
            # qu
            param("qu", "5 pauqar waray 1878 miércoles", "5 march 1878 wednesday"),
            param("qu", "6 int 2009 domingo", "6 june 2009 sunday"),
            # rm
            param("rm", "1 schaner 1890 venderdi", "1 january 1890 friday"),
            param("rm", "me 6 avust 2009", "wednesday 6 august 2009"),
            # rn
            param("rn", "11 ntwarante 2008 12:34 zmw", "11 march 2008 12:34 pm"),
            param("rn", "7 nze 1999 ku wa kabiri", "7 september 1999 tuesday"),
            # rof
            param(
                "rof",
                "13 mweri wa tisa ijtn 12:56 kingoto",
                "13 september wednesday 12:56 pm",
            ),
            param("rof", "ijumanne 2 mweri wa saba 1890", "tuesday 2 july 1890"),
            # rw
            param("rw", "16 kamena 2001 kuwa gatanu", "16 june 2001 friday"),
            param("rw", "3 ukuboza 2013 gnd", "3 december 2013 saturday"),
            # rwk
            param("rwk", "3 aprilyi 2009 ijumaa", "3 april 2009 friday"),
            param(
                "rwk", "jumamosi 2 januari 02:13 utuko", "saturday 2 january 02:13 am"
            ),
            # sah
            param(
                "sah",
                "16 тохсунньу 2003 сэрэдэ 09:59 эк",
                "16 january 2003 wednesday 09:59 pm",
            ),
            param(
                "sah", "баскыһыанньа 14 балаҕан ыйа 1998", "sunday 14 september 1998"
            ),
            # saq
            param(
                "saq",
                "1 lapa le okuni 1980 kun 10:45 tesiran",
                "1 march 1980 monday 10:45 am",
            ),
            param(
                "saq",
                "mderot ee inet 12 lapa le ong'wan 1824",
                "wednesday 12 april 1824",
            ),
            # sbp
            param(
                "sbp",
                "1 mupalangulwa mulungu 08:15 lwamilawu",
                "1 january sunday 08:15 am",
            ),
            param("sbp", "jtn 17 mokhu 2001", "wednesday 17 october 2001"),
            # se
            param(
                "se", "láv 22 cuoŋománnu 10:08 iđitbeaivi", "saturday 22 april 10:08 am"
            ),
            param(
                "se",
                "duorasdat 11 borgemánnu 1978 12:09 eb",
                "thursday 11 august 1978 12:09 pm",
            ),
            # seh
            param("seh", "12 fevreiro 2005 sha", "12 february 2005 friday"),
            param("seh", "chiposi 2 decembro 1987", "monday 2 december 1987"),
            # ses
            param(
                "ses",
                "18 žuyye 2009 atalaata 03:12 aluula",
                "18 july 2009 tuesday 03:12 pm",
            ),
            param("ses", "asibti 2 awi 1987", "saturday 2 april 1987"),
            # sg
            param(
                "sg", "5 ngubùe 1890 bïkua-ûse 12:08 lk", "5 april 1890 monday 12:08 pm"
            ),
            param("sg", "bk3 23 föndo 2001", "tuesday 23 june 2001"),
            # shi-Latn
            param(
                "shi-Latn",
                "6 bṛayṛ 2014 akṛas 07:06 tifawt",
                "6 february 2014 wednesday 07:06 am",
            ),
            param("shi-Latn", "asamas 15 ɣuct 2045", "sunday 15 august 2045"),
            # sk
            param("sk", "15 marec 1987 utorok", "15 march 1987 tuesday"),
            param("sk", "streda 17 mája 2003", "wednesday 17 may 2003"),
            param("sk", "o 2 mesiace", "in 2 month"),
            param("sk", "o týždeň", "in 1 week"),
            param("sk", "predvčerom", "2 day ago"),
            param("sk", "v sobotu", " saturday"),
            # sl
            param(
                "sl", "12 junij 2003 petek 10:09 pop", "12 june 2003 friday 10:09 pm"
            ),
            param(
                "sl",
                "ponedeljek 15 okt 1997 09:07 dopoldne",
                "monday 15 october 1997 09:07 am",
            ),
            # smn
            param(
                "smn",
                "1 njuhčâmáánu 2008 majebaargâ 08:08 ip",
                "1 march 2008 tuesday 08:08 am",
            ),
            param("smn", "láv 23 roovvâd 1897", "saturday 23 october 1897"),
            # sn
            param("sn", "11 chikumi 1998 chipiri", "11 june 1998 tuesday"),
            param("sn", "china 2 mbudzi 1890", "thursday 2 november 1890"),
            # so
            param(
                "so",
                "sab 5 bisha saddexaad 1765 11:08 gn",
                "saturday 5 march 1765 11:08 pm",
            ),
            param("so", "16 lit 2008 axd", "16 december 2008 sunday"),
            # sq
            param(
                "sq",
                "2 qershor 1997 e mërkurë 10:08 pasdite",
                "2 june 1997 wednesday 10:08 pm",
            ),
            param(
                "sq",
                "pre 15 gusht 1885 04:54 e paradites",
                "friday 15 august 1885 04:54 am",
            ),
            # sr-Cyrl
            param(
                "sr-Cyrl",
                "16 април 2016 суб 03:46 по подне",
                "16 april 2016 saturday 03:46 pm",
            ),
            param("sr-Cyrl", "уторак 3 новембар 1999", "tuesday 3 november 1999"),
            # sr-Latn
            param("sr-Latn", "4 septembar 2000 četvrtak", "4 september 2000 thursday"),
            param(
                "sr-Latn",
                "uto 18 maj 2004 11:15 pre podne",
                "tuesday 18 may 2004 11:15 am",
            ),
            # sr
            param(
                "sr",
                "3 децембар 2005 уто 10:15 по подне",
                "3 december 2005 tuesday 10:15 pm",
            ),
            param("sr", "петак 12 август 2001", "friday 12 august 2001"),
            # sv
            param(
                "sv",
                "4 augusti 2007 lördag 02:44 fm",
                "4 august 2007 saturday 02:44 am",
            ),
            param(
                "sv", "onsdag 16 mars 08:15 eftermiddag", "wednesday 16 march 08:15 pm"
            ),
            # sw
            param(
                "sw", "5 mei 1994 jumapili 10:17 asubuhi", "5 may 1994 sunday 10:17 am"
            ),
            param("sw", "jumanne 2 desemba 2003", "tuesday 2 december 2003"),
            # ta
            param(
                "ta",
                "6 ஏப்ரல் 1997 செவ்வாய் 02:09 முற்பகல்",
                "6 april 1997 tuesday 02:09 am",
            ),
            param("ta", "ஞாயி 1 ஜூன் 1998", "sunday 1 june 1998"),
            # te
            param("te", "సోమవారం 3 నవంబర 1887", "monday 3 november 1887"),
            param("te", "5 మార్చి 2001 శుక్రవారం", "5 march 2001 friday"),
            # teo
            param("teo", "2 omodok'king'ol 1996 nakaare", "2 june 1996 tuesday"),
            param(
                "teo",
                "nakasabiti 4 jol 2001 01:12 ebongi",
                "saturday 4 july 2001 01:12 pm",
            ),
            # to
            param(
                "to",
                "5 fēpueli 2007 mōn 02:17 efiafi",
                "5 february 2007 monday 02:17 pm",
            ),
            param(
                "to",
                "falaite 14 'okatopa 2015 09:48 hh",
                "friday 14 october 2015 09:48 am",
            ),
            # twq
            param(
                "twq", "17 feewiriye 2023 11:12 zaarikay b", "17 february 2023 11:12 pm"
            ),
            param("twq", "alzuma 11 sektanbur 2019", "friday 11 september 2019"),
            # tzm
            param(
                "tzm",
                "2 yulyuz 2002 akwas 01:16 ḍeffir aza",
                "2 july 2002 thursday 01:16 pm",
            ),
            param("tzm", "asa 13 nwanbir 2005", "sunday 13 november 2005"),
            # uz-Cyrl
            param(
                "uz-Cyrl",
                "пайшанба 24 ноябр 1957 01:18 то",
                "thursday 24 november 1957 01:18 am",
            ),
            param("uz-Cyrl", "4 авг 1887 чоршанба", "4 august 1887 wednesday"),
            # uz-Latn
            param(
                "uz-Latn",
                "3 iyul 1997 payshanba 08:17 tk",
                "3 july 1997 thursday 08:17 pm",
            ),
            param("uz-Latn", "shan 15 sentabr 2008", "saturday 15 september 2008"),
            # uz
            param(
                "uz",
                "1 fevral 1776 dushanba 09:17 to",
                "1 february 1776 monday 09:17 am",
            ),
            param("uz", "juma 18 aprel 2027", "friday 18 april 2027"),
            # vun
            param("vun", "2 aprilyi 1956 jumatatuu", "2 april 1956 monday"),
            param(
                "vun",
                "jumamosi 12 oktoba 02:16 kyiukonyi",
                "saturday 12 october 02:16 pm",
            ),
            # wae
            param("wae", "zištag 16 abrille 2002", "tuesday 16 april 2002"),
            param("wae", "27 öigšte 1669 fritag", "27 august 1669 friday"),
            # xog
            param("xog", "21 marisi 2001 owokubili", "21 march 2001 tuesday"),
            param(
                "xog",
                "kuta 30 okitobba 1955 02:17 eigulo",
                "friday 30 october 1955 02:17 pm",
            ),
            # yav
            param(
                "yav", "12 imɛŋ i puɔs 1998 metúkpíápɛ", "12 september 1998 wednesday"
            ),
            param(
                "yav",
                "5 o10 2001 séselé 12:07 kiɛmɛ́ɛm",
                "5 october 2001 saturday 12:07 am",
            ),
            # yo
            param(
                "yo",
                "5 èrèlè 2005 ọjọ́rú 10:07 àárọ̀",
                "5 february 2005 wednesday 10:07 am",
            ),
            param("yo", "ọjọ́ àbámẹ́ta 2 oṣù ẹ̀bibi 1896", "saturday 2 may 1896"),
            # zu
            param("zu", "3 mashi 2007 ulwesibili 10:08", "3 march 2007 tuesday 10:08"),
            param("zu", "son 23 umasingana 1996", "sunday 23 january 1996"),
        ]
    )
    def test_translation(self, shortname, datetime_string, expected_translation):
        self.given_settings()
        self.given_bundled_language(shortname)
        self.given_string(datetime_string)
        self.when_datetime_string_translated()
        self.then_string_translated_to(expected_translation)

    @parameterized.expand(
        [
            # English
            param("en", "yesterday", "1 day ago"),
            param("en", "today", "0 day ago"),
            param("en", "day before yesterday", "2 day ago"),
            param("en", "last month", "1 month ago"),
            param("en", "less than a minute ago", "45 second ago"),
            param("en", "10h11", "10:11"),
            param("en", "10h11m", "10 hour 11 minute"),
            param("en", "3d8h2m", "3 day 8 hour 2 minute"),
            param("en", "5d9h59m10s", "5 day 9 hour 59 minute 10 second"),
            param("en", "3d1h", "3 day 1 hour"),
            param("en", "3d29m", "3 day 29 minute"),
            param("en", "1.5d10s", "1.5 day 10 second"),
            param("en", "7m1s", "7 minute 1 second"),
            # German
            param("de", "vorgestern", "2 day ago"),
            param("de", "heute", "0 day ago"),
            param("de", "vor 3 Stunden", "3 hour ago"),
            param("de", "vor 2 Monaten", "2 month ago"),
            param("de", "vor 2 Monaten, 2 Wochen", "2 month ago 2 week"),
            # French
            param("fr", "avant-hier", "2 day ago"),
            param("fr", "hier", "1 day ago"),
            param("fr", "aujourd'hui", "0 day ago"),
            param("fr", "après dix ans", "in 10 year"),
            # Spanish
            param("es", "anteayer", "2 day ago"),
            param("es", "ayer", "1 day ago"),
            param("es", "ayer a las", "1 day ago "),
            param("es", "hoy", "0 day ago"),
            param("es", "hace un horas", "1 hour ago"),
            param("es", "2 semanas", "2 week"),
            param("es", "2 año", "2 year"),
            # Italian
            param("it", "altro ieri", "2 day ago"),
            param("it", "ieri", "1 day ago"),
            param("it", "oggi", "0 day ago"),
            param("it", "2 settimana fa", "2 week ago"),
            param("it", "2 anno fa", "2 year ago"),
            # Portuguese
            param("pt", "anteontem", "2 day ago"),
            param("pt", "ontem", "1 day ago"),
            param("pt", "hoje", "0 day ago"),
            param("pt", "56 minutos", "56 minute"),
            param("pt", "12 dias", "12 day"),
            param("pt", "há 14 min.", "14 minute ago."),
            param("pt", "1 segundo atrás", "1 second ago"),
            # Russian
            param("ru", "9 месяцев", "9 month"),
            param("ru", "8 недель", "8 week"),
            param("ru", "7 лет", "7 year"),
            param("ru", "позавчера", "2 day ago"),
            param("ru", "сейчас", "0 second ago"),
            param("ru", "спустя 2 дня", "in 2 day"),
            param("ru", "вчера", "1 day ago"),
            param("ru", "сегодня", "0 day ago"),
            param("ru", "завтра", "in 1 day"),
            param("ru", "послезавтра", "in 2 day"),
            param("ru", "послепослезавтра", "in 3 day"),
            param("ru", "во вторник", " tuesday"),
            param("ru", "в воскресенье", " sunday"),
            param("ru", "в воскресение", " sunday"),
            param("ru", "в вск", " sunday"),
            param("ru", "несколько секунд", "44 second"),
            param("ru", "через пару секунд", "in 2 second"),
            param("ru", "одну минуту назад", "1 minute ago"),
            param("ru", "через полчаса", "in 30 minute"),
            param("ru", "сорок минут назад", "40 minute ago"),
            param("ru", "в течение пары часов", "in 2 hour"),
            param("ru", "через четыре часа", "in 4 hour"),
            param("ru", "в течение суток", "in 1 day"),
            param("ru", "двое суток назад", "2 day ago"),
            param("ru", "неделю назад", "1 week ago"),
            param("ru", "две недели назад", "2 week ago"),
            param("ru", "три месяца назад", "3 month ago"),
            param("ru", "спустя полгода", "in 6 month"),
            param("ru", "через год", "in 1 year"),
            param("ru", "через полтора года", "in 18 month"),
            # Turkish
            param("tr", "dün", "1 day ago"),
            param("tr", "22 dakika", "22 minute"),
            param("tr", "12 hafta", "12 week"),
            param("tr", "13 yıl", "13 year"),
            # Czech
            param("cs", "40 sekunda", "40 second"),
            param("cs", "4 týden", "4 week"),
            param("cs", "14 roků", "14 year"),
            # Chinese
            param("zh", "昨天", "1 day ago"),
            param("zh", "前天", "2 day ago"),
            param("zh", "50 秒", "50 second"),
            param("zh", "7 周", "7 week"),
            param("zh", "12 年", "12 year"),
            param("zh", "半小时前", "30 minute ago"),
            # Danish
            param("da", "i går", "1 day ago"),
            param("da", "i dag", "0 day ago"),
            param("da", "sidste måned", "1 month ago"),
            param("da", "mindre end et minut siden", "45  seconds"),
            # Dutch
            param("nl", "17 uur geleden", "17 hour ago"),
            param("nl", "27 jaar geleden", "27 year ago"),
            param("nl", "45 minuten", "45 minute"),
            param("nl", "nu", "0 second ago"),
            param("nl", "eergisteren", "2 day ago"),
            param("nl", "volgende maand", "in 1 month"),
            # Romanian
            param("ro", "23 săptămâni în urmă", "23 week ago"),
            param("ro", "23 săptămâni", "23 week"),
            param("ro", "13 oră", "13 hour"),
            # Arabic
            param("ar", "يومين", "2 day"),
            param("ar", "أمس", "1 day ago"),
            param("ar", "4 عام", "4 year"),
            param("ar", "منذ 2 ساعات", "ago 2 hour"),
            param("ar", "منذ ساعتين", "ago 2 hour"),
            param("ar", "اليوم السابق", "1 day ago"),
            param("ar", "اليوم", "0 day ago"),
            # Polish
            param("pl", "2 godz.", "2 hour."),
            param("pl", "Wczoraj o 07:40", "1 day ago  07:40"),
            # Vietnamese
            param("vi", "2 tuần 3 ngày", "2 week 3 day"),
            param("vi", "21 giờ trước", "21 hour ago"),
            param("vi", "Hôm qua 08:16", "1 day ago 08:16"),
            param("vi", "Hôm nay 15:39", "0 day ago 15:39"),
            # French
            param("fr", "maintenant", "0 second ago"),
            param("fr", "demain", "in 1 day"),
            param("fr", "Il y a moins d'une minute", "1 minute ago"),
            param("fr", "Il y a moins de 30s", "30 second ago"),
            # Tagalog
            param("tl", "kahapon", "1 day ago"),
            param("tl", "ngayon", "0 second ago"),
            # Ukrainian
            param("uk", "позавчора", "2 day ago"),
            param("uk", "післязавтра", "in 2 day"),
            param("uk", "через 2 дні", "in 2 day"),
            param("uk", "через 2 доби", "in 2 day"),
            param("uk", "через 5 діб", "in 5 day"),
            param("uk", "через п'ять діб", "in 5 day"),
            param("uk", "за вісім днів", "in 8 day"),
            param("uk", "2 роки", "2 year"),
            # Belarusian
            param("be", "9 месяцаў", "9 month"),
            param("be", "8 тыдняў", "8 week"),
            param("be", "1 тыдзень", "1 week"),
            param("be", "2 года", "2 year"),
            param("be", "3 гады", "3 year"),
            param("be", "11 секунд", "11 second"),
            param("be", "учора", "1 day ago"),
            param("be", "пазаўчора", "2 day ago"),
            param("be", "сёння", "0 day ago"),
            param("be", "некалькі хвілін", "2 minute"),
            # Indonesian
            param("id", "baru saja", "0 second ago"),
            param("id", "hari ini", "0 day ago"),
            param("id", "kemarin", "1 day ago"),
            param("id", "kemarin lusa", "2 day ago"),
            param("id", "sehari yang lalu", "1 day  ago"),
            param("id", "seminggu yang lalu", "1 week  ago"),
            param("id", "sebulan yang lalu", "1 month  ago"),
            param("id", "setahun yang lalu", "1 year  ago"),
            # Finnish
            param("fi", "1 vuosi sitten", "1 year ago"),
            param("fi", "2 vuotta sitten", "2 year ago"),
            param("fi", "3 v sitten", "3 year ago"),
            param("fi", "4 v. sitten", "4 year. ago"),
            param("fi", "5 vv. sitten", "5 year. ago"),
            param("fi", "1 kuukausi sitten", "1 month ago"),
            param("fi", "2 kuukautta sitten", "2 month ago"),
            param("fi", "3 kk sitten", "3 month ago"),
            param("fi", "1 viikko sitten", "1 week ago"),
            param("fi", "2 viikkoa sitten", "2 week ago"),
            param("fi", "3 vk sitten", "3 week ago"),
            param("fi", "4 vko sitten", "4 week ago"),
            param("fi", "1 päivä sitten", "1 day ago"),
            param("fi", "2 päivää sitten", "2 day ago"),
            param("fi", "8 pvää sitten", "8 day ago"),
            param("fi", "3 pv sitten", "3 day ago"),
            param("fi", "4 p. sitten", "4 day. ago"),
            param("fi", "5 pvä sitten", "5 day ago"),
            param("fi", "1 tunti sitten", "1 hour ago"),
            param("fi", "2 tuntia sitten", "2 hour ago"),
            param("fi", "3 t sitten", "3 hour ago"),
            param("fi", "1 minuutti sitten", "1 minute ago"),
            param("fi", "2 minuuttia sitten", "2 minute ago"),
            param("fi", "3 min sitten", "3 minute ago"),
            param("fi", "1 sekunti sitten", "1 second ago"),
            param("fi", "2 sekuntia sitten", "2 second ago"),
            param("fi", "1 sekuntti sitten", "1 second ago"),
            param("fi", "2 sekunttia sitten", "2 second ago"),
            param("fi", "3 s sitten", "3 second ago"),
            param("fi", "eilen", "1 day ago"),
            param("fi", "tänään", "0 day ago"),
            param("fi", "huomenna", "in 1 day"),
            param("fi", "nyt", "0 second ago"),
            param("fi", "ensi viikolla", "in 1 week"),
            param("fi", "viime viikolla", "1 week ago"),
            param("fi", "toissa vuonna", "2 year ago"),
            param("fi", "9 kuukautta sitten", "9 month ago"),
            param("fi", "3 viikon päästä", "in 3 week"),
            param("fi", "10 tunnin kuluttua", "in 10 hour"),
            # Japanese
            param("ja", "今年", "0 year ago"),
            param("ja", "去年", "1 year ago"),
            param("ja", "17年前", "17 year ago"),
            param("ja", "今月", "0 month ago"),
            param("ja", "先月", "1 month ago"),
            param("ja", "1ヶ月前", "1 month ago"),
            param("ja", "2ヶ月前", "2 month ago"),
            param("ja", "今週", "0 week ago"),
            param("ja", "先週", "1 week ago"),
            param("ja", "先々週", "2 week ago"),
            param("ja", "2週間前", "2 week ago"),
            param("ja", "3週間", "3 week"),
            param("ja", "今日", "0 day ago"),
            param("ja", "昨日", "1 day ago"),
            param("ja", "一昨日", "2 day ago"),
            param("ja", "3日前", "3 day ago"),
            param("ja", "1時間", "1 hour"),
            param("ja", "23時間前", "23 hour ago"),
            param("ja", "30分", "30 minute"),
            param("ja", "3分間", "3 minute"),
            param("ja", "60秒", "60 second"),
            param("ja", "3秒前", "3 second ago"),
            param("ja", "現在", "0 second ago"),
            param("ja", "明後日", "in 2 day"),
            # Hebrew
            param("he", "אתמול", "1 day ago"),
            param("he", "אתמול בשעה 3", "1 day ago  3"),
            param("he", "היום", "0 day ago"),
            param("he", "לפני יומיים", "2 day ago"),
            param("he", "לפני שבועיים", "2 week ago"),
            # Bulgarian
            param("bg", "вдругиден", "in 2 day"),
            param("bg", "утре", "in 1 day"),
            param("bg", "след 5 дни", "in 5 day"),
            param("bg", "вчера", "1 day ago"),
            param("bg", "преди 9 дни", "9 day ago"),
            param("bg", "преди 10 минути", "10 minute ago"),
            param("bg", "преди час", "1 hour ago"),
            param("bg", "преди 4 години", "4 year ago"),
            param("bg", "преди десетилетие", "10 year ago"),
            # Bangla
            param("bn", "গতকাল", "1 day ago"),
            param("bn", "আজ", "0 day ago"),
            param("bn", "গত মাস", "1 month ago"),
            param("bn", "আগামী সপ্তাহ", "in 1 week"),
            # Hindi
            param("hi", "१ सप्ताह", "1 week"),
            param("hi", "२४ मिनट पहले", "24 minute ago"),
            param("hi", "5 वर्ष", "5 year"),
            param("hi", "५३ सप्ताह बाद", "53 week in"),
            param("hi", "12 सेकंड पूर्व", "12 second ago"),
            # Swedish
            param("sv", "igår", "1 day ago"),
            param("sv", "idag", "0 day ago"),
            param("sv", "förrgår", "2 day ago"),
            param("sv", "förra månaden", "1 month ago"),
            param("sv", "nästa månad", "in 1 month"),
            # Georgian
            param("ka", "გუშინ", "1 day ago"),
            param("ka", "დღეს", "0 day ago"),
            param("ka", "ერთ თვე", "1 month"),
            param("ka", "დღეიდან ერთ კვირა", "in 1 week"),
            # af
            param("af", "3 week gelede", "3 week ago"),
            param("af", "volgende jaar 10:08 nm", "in 1 year 10:08 pm"),
            param("af", "oor 45 sekondes", "in 45 second"),
            # am
            param("am", "የሚቀጥለው ሳምንት", "in 1 week"),
            param("am", "በ10 ሳምንት ውስጥ", "in 10 week"),
            # as
            param("as", "কাইলৈ", "in 1 day"),
            param("as", "আজি", "0 day ago"),
            # asa
            param("asa", "ighuo", "1 day ago"),
            param("asa", "yavo 09:27 ichamthi", "in 1 day 09:27 pm"),
            # ast
            param("ast", "el mes viniente 02:17 tarde", "in 1 month 02:17 pm"),
            param("ast", "hai 22 selmana", "22 week ago"),
            param("ast", "en 5 minutos", "in 5 minute"),
            # az-Latn
            param("az-Latn", "keçən həftə", "1 week ago"),
            param("az-Latn", "10 ay ərzində", "in 10 month"),
            param("az-Latn", "22 saniyə öncə", "22 second ago"),
            # az
            param("az", "12 saat ərzində", "in 12 hour"),
            param("az", "15 həftə öncə", "15 week ago"),
            # bas
            param("bas", "lɛ̀n 12:08 i ɓugajɔp", "0 day ago 12:08 pm"),
            param("bas", "yààni", "1 day ago"),
            # bem
            param("bem", "lelo", "0 day ago"),
            param("bem", "17 umweshi", "17 month"),
            # bez
            param("bez", "hilawu 08:44 pamilau", "in 1 day 08:44 am"),
            param("bez", "neng'u ni", "0 day ago"),
            param("bez", "12 mlungu gumamfu", "12 week"),
            # bm
            param("bm", "sini 01:18 am", "in 1 day 01:18 am"),
            param("bm", "kunu", "1 day ago"),
            param("bm", "22 dɔgɔkun", "22 week"),
            # bo
            param("bo", "ཁས་ས་", "1 day ago"),
            param("bo", "སང་ཉིན་", "in 1 day"),
            # br
            param("br", "ar sizhun diaraok", "1 week ago"),
            param("br", "ar bloaz a zeu 02:19 gm", "in 1 year 02:19 pm"),
            param("br", "10 deiz", "10 day"),
            # brx
            param("brx", "गाबोन", "in 1 day"),
            param("brx", "मैया 11:58 फुं", "1 day ago 11:58 am"),
            param("brx", "17 मिनिथ", "17 minute"),
            # bs-Cyrl
            param("bs-Cyrl", "следећег месеца", "in 1 month"),
            param("bs-Cyrl", "прошле године 10:05 пре подне", "1 year ago 10:05 am"),
            param("bs-Cyrl", "пре 28 недеља", "28 week ago"),
            # bs-Latn
            param("bs-Latn", "sljedeće godine", "in 1 year"),
            param("bs-Latn", "prije 4 mjeseci", "4 month ago"),
            param("bs-Latn", "za 36 sati", "in 36 hour"),
            # bs
            param("bs", "prije 12 sekundi", "12 second ago"),
            param("bs", "za 5 godinu", "in 5 year"),
            param("bs", "ovaj sat", "0 hour ago"),
            # ca
            param("ca", "d'aquí a 22 hores", "in 22 hour"),
            param("ca", "fa 17 anys", "17 year ago"),
            param("ca", "el mes passat", "1 month ago"),
            param("ca", "la pròxima setmana", "in 1 week"),
            param("ca", "despús-ahir", "2 day ago"),
            param("ca", "en un dia", "in 1 day"),
            param("ca", "demà passat", "in 2 day"),
            # ce
            param("ce", "72 сахьт даьлча", "in 72 hour"),
            param("ce", "42 шо хьалха", "42 year ago"),
            param("ce", "рогӏерчу баттахь", "in 1 month"),
            # cgg
            param("cgg", "nyenkyakare", "in 1 day"),
            param("cgg", "nyomwabazyo", "1 day ago"),
            param("cgg", "5 omwaka", "5 year"),
            # chr
            param("chr", "ᎯᎠ ᎢᏯᏔᏬᏍᏔᏅ", "0 minute ago"),
            param("chr", "ᎾᎿ 8 ᎧᎸᎢ ᏥᎨᏒ", "8 month ago"),
            param("chr", "ᎾᎿ 22 ᎢᏯᏔᏬᏍᏔᏅ", "in 22 minute"),
            # cs
            param("cs", "za 3 rok", "in 3 year"),
            param("cs", "před 11 měsícem", "11 month ago"),
            param("cs", "tento měsíc", "0 month ago"),
            # cy
            param("cy", "wythnos ddiwethaf", "1 week ago"),
            param("cy", "25 o flynyddoedd yn ôl", "25 year ago"),
            param("cy", "ymhen 4 awr", "in 4 hour"),
            # da
            param("da", "for 15 måneder siden", "15 month ago"),
            param("da", "om 60 sekunder", "in 60 second"),
            param("da", "sidste måned", "1 month ago"),
            # dav
            param("dav", "iguo", "1 day ago"),
            param("dav", "kesho 02:12 luma lwa p", "in 1 day 02:12 pm"),
            param("dav", "15 juma", "15 week"),
            # de
            param("de", "nächstes jahr", "in 1 year"),
            param("de", "letzte woche 04:25 nachm", "1 week ago 04:25 pm"),
            # dje
            param("dje", "hõo 08:08 subbaahi", "0 day ago 08:08 am"),
            param("dje", "suba", "in 1 day"),
            param("dje", "7 handu", "7 month"),
            # dsb
            param("dsb", "pśed 10 góźinami", "10 hour ago"),
            param("dsb", "za 43 minutow", "in 43 minute"),
            param("dsb", "pśiducy tyźeń", "in 1 week"),
            # dua
            param("dua", "kíɛlɛ nítómb́í", "1 day ago"),
            param("dua", "12 ŋgandɛ", "12 hour"),
            param("dua", "wɛ́ŋgɛ̄", "0 day ago"),
            # dyo
            param("dyo", "fucen", "1 day ago"),
            param("dyo", "kajom", "in 1 day"),
            param("dyo", "6 fuleeŋ", "6 month"),
            # dz
            param("dz", "ནངས་པ་", "in 1 day"),
            param("dz", "སྐར་མ་ 3 ནང་", "in 3 minute"),
            param("dz", "ལོ་འཁོར་ 21 ཧེ་མ་", "21 year ago"),
            # ebu
            param("ebu", "ĩgoro", "1 day ago"),
            param("ebu", "2 ndagĩka", "2 minute"),
            param("ebu", "rũciũ", "in 1 day"),
            # ee
            param("ee", "ɣleti si va yi", "1 month ago"),
            param("ee", "ƒe 24 si wo va yi", "24 year ago"),
            param("ee", "le sekend 20 me", "in 20 second"),
            # el
            param("el", "πριν από 45 λεπτό", "45 minute ago"),
            param("el", "σε 22 μήνες", "in 22 month"),
            param("el", "επόμενη εβδομάδα 12:09 μμ", "in 1 week 12:09 pm"),
            # et
            param("et", "eelmine nädal", "1 week ago"),
            param("et", "1 a pärast", "in 1 year"),
            param("et", "4 tunni eest", "4 hour ago"),
            # eu
            param("eu", "aurreko hilabetea", "1 month ago"),
            param("eu", "duela 15 segundo", "15 second ago"),
            param("eu", "2 hilabete barru", "in 2 month"),
            # ewo
            param("ewo", "okírí", "in 1 day"),
            param("ewo", "angogé 10:15 kíkíríg", "1 day ago 10:15 am"),
            param("ewo", "5 m̀bú", "5 year"),
            # ff
            param("ff", "hannde", "0 day ago"),
            param("ff", "haŋki 01:14 subaka", "1 day ago 01:14 am"),
            param("ff", "2 yontere", "2 week"),
            # fil
            param("fil", "22 min ang nakalipas", "22 minute ago"),
            param("fil", "sa 5 taon", "in 5 year"),
            param("fil", "nakalipas na linggo", "1 week ago"),
            # fo
            param("fo", "seinasta mánað", "1 month ago"),
            param("fo", "um 3 viku", "in 3 week"),
            param("fo", "7 tímar síðan", "7 hour ago"),
            # fur
            param("fur", "ca di 16 setemanis", "in 16 week"),
            param("fur", "15 secont indaûr", "15 second ago"),
            param("fur", "doman", "in 1 day"),
            # fy
            param("fy", "folgjende moanne", "in 1 month"),
            param("fy", "oer 24 oere", "in 24 hour"),
            param("fy", "2 deien lyn", "2 day ago"),
            # ga
            param("ga", "i gceann 6 nóiméad", "in 6 minute"),
            param("ga", "12 seachtain ó shin", "12 week ago"),
            param("ga", "an bhliain seo chugainn", "in 1 year"),
            # gd
            param("gd", "an ceann 2 mhìosa", "in 2 month"),
            param("gd", "15 uair a thìde air ais", "15 hour ago"),
            param("gd", "am mìos seo chaidh", "1 month ago"),
            # gl
            param("gl", "hai 25 semanas", "25 week ago"),
            param("gl", "en 2 horas", "in 2 hour"),
            param("gl", "o ano pasado", "1 year ago"),
            # gsw
            param("gsw", "moorn", "in 1 day"),
            param("gsw", "geschter", "1 day ago"),
            # gu
            param("gu", "2 વર્ષ પહેલા", "2 year ago"),
            param("gu", "આવતા મહિને", "in 1 month"),
            param("gu", "22 કલાક પહેલાં", "22 hour ago"),
            # guz
            param("guz", "mambia", "in 1 day"),
            param("guz", "igoro", "1 day ago"),
            # ha
            param("ha", "gobe", "in 1 day"),
            param("ha", "jiya", "1 day ago"),
            # hr
            param("hr", "sljedeća godina", "in 1 year"),
            param("hr", "sljedeće godine", "in 1 year"),
            param("hr", "sljedećoj godini", "in 1 year"),
            param("hr", "iduća godina", "in 1 year"),
            param("hr", "iduće godine", "in 1 year"),
            param("hr", "idućoj godini", "in 1 year"),
            param("hr", "prošla godina", "1 year ago"),
            param("hr", "prošle godine", "1 year ago"),
            param("hr", "prošloj godini", "1 year ago"),
            param("hr", "sljedeći mjesec", "in 1 month"),
            param("hr", "sljedećeg mjeseca", "in 1 month"),
            param("hr", "sljedećem mjesecu", "in 1 month"),
            param("hr", "idući mjesec", "in 1 month"),
            param("hr", "idućeg mjeseca", "in 1 month"),
            param("hr", "idućem mjesecu", "in 1 month"),
            param("hr", "prošli mjesec", "1 month ago"),
            param("hr", "prošlog mjeseca", "1 month ago"),
            param("hr", "prošlom mjesecu", "1 month ago"),
            param("hr", "sljedeći tjedan", "in 1 week"),
            param("hr", "sljedećeg tjedna", "in 1 week"),
            param("hr", "sljedećem tjednu", "in 1 week"),
            param("hr", "idući tjedan", "in 1 week"),
            param("hr", "idućeg tjedna", "in 1 week"),
            param("hr", "idućem tjednu", "in 1 week"),
            param("hr", "prošli tjedan", "1 week ago"),
            param("hr", "prošlog tjedna", "1 week ago"),
            param("hr", "prošlom tjednu", "1 week ago"),
            param("hr", "prije 7 godina", "7 year ago"),
            param("hr", "za 7 godina", "in 7 year"),
            param("hr", "prije 2 godine", "2 year ago"),
            param("hr", "za 2 godine", "in 2 year"),
            param("hr", "prije 1 godinu", "1 year ago"),
            param("hr", "za 1 godinu", "in 1 year"),
            param("hr", "prije 7 mjeseci", "7 month ago"),
            param("hr", "za 7 mjeseci", "in 7 month"),
            param("hr", "prije 2 mjeseca", "2 month ago"),
            param("hr", "za 2 mjeseca", "in 2 month"),
            param("hr", "prije 1 mjesec", "1 month ago"),
            param("hr", "za 1 mjesec", "in 1 month"),
            param("hr", "prije 7 tjedana", "7 week ago"),
            param("hr", "za 7 tjedana", "in 7 week"),
            param("hr", "prije 2 tjedna", "2 week ago"),
            param("hr", "za 2 tjedna", "in 2 week"),
            param("hr", "prije 1 tjedan", "1 week ago"),
            param("hr", "za 1 tjedan", "in 1 week"),
            param("hr", "prije 7 dana", "7 day ago"),
            param("hr", "za 7 dana", "in 7 day"),
            param("hr", "prije 1 dan", "1 day ago"),
            param("hr", "za 1 dan", "in 1 day"),
            param("hr", "prije 7 sati", "7 hour ago"),
            param("hr", "za 7 sati", "in 7 hour"),
            param("hr", "prije 2 sata", "2 hour ago"),
            param("hr", "za 2 sata", "in 2 hour"),
            param("hr", "prije 1 sat", "1 hour ago"),
            param("hr", "za 1 sat", "in 1 hour"),
            param("hr", "prije 7 minuta", "7 minute ago"),
            param("hr", "za 7 minuta", "in 7 minute"),
            param("hr", "prije 2 minute", "2 minute ago"),
            param("hr", "za 2 minute", "in 2 minute"),
            param("hr", "prije 7 sekundi", "7 second ago"),
            param("hr", "za 7 sekundi", "in 7 second"),
            param("hr", "prije 2 sekunde", "2 second ago"),
            param("hr", "za 2 sekunde", "in 2 second"),
            param("hr", "prije 1 sekundu", "1 second ago"),
            param("hr", "za 1 sekundu", "in 1 second"),
            param("hr", "jučer", "1 day ago"),
            param("hr", "prekjučer", "2 day ago"),
            param("hr", "sutra", "in 1 day"),
            param("hr", "prekosutra", "in 2 day"),
            param("hr", "lani", "1 year ago"),
            param("hr", "preklani", "2 year ago"),
            param("hr", "dogodine", "in 1 year"),
            param("hr", "nagodinu", "in 1 year"),
            # hsb
            param("hsb", "před 5 tydźenjemi", "5 week ago"),
            param("hsb", "za 60 sekundow", "in 60 second"),
            param("hsb", "lětsa", "0 year ago"),
            # hy
            param("hy", "հաջորդ ամիս", "in 1 month"),
            param("hy", "2 վայրկյան առաջ", "2 second ago"),
            param("hy", "3 տարուց", "in 3 year"),
            # id
            param("id", "5 tahun yang lalu", "5 year ago"),
            param("id", "dalam 43 menit", "in 43 minute"),
            param("id", "dlm 23 dtk", "in 23 second"),
            # ig
            param("ig", "nnyaafụ", "1 day ago"),
            param("ig", "taata", "0 day ago"),
            # is
            param("is", "í næstu viku", "in 1 week"),
            param("is", "fyrir 3 mánuðum", "3 month ago"),
            param("is", "eftir 2 klst", "in 2 hour"),
            # it
            param("it", "tra 3 minuti", "in 3 minute"),
            param("it", "5 giorni fa", "5 day ago"),
            param("it", "anno prossimo", "in 1 year"),
            # jgo
            param("jgo", "ɛ́ gɛ mɔ́ 20 háwa", "20 hour ago"),
            param("jgo", "ɛ́ gɛ́ mɔ́ pɛsaŋ 5", "5 month ago"),
            param("jgo", "nǔu ŋguꞌ 2", "in 2 year"),
            # jmc
            param("jmc", "ngama", "in 1 day"),
            param("jmc", "ukou", "1 day ago"),
            # ka
            param("ka", "ამ საათში", "0 hour ago"),
            param("ka", "7 კვირის წინ", "7 week ago"),
            param("ka", "6 წუთში", "in 6 minute"),
            # kab
            param("kab", "iḍelli", "1 day ago"),
            param("kab", "ass-a", "0 day ago"),
            # kam
            param("kam", "ũmũnthĩ", "0 day ago"),
            param("kam", "ũnĩ", "in 1 day"),
            # kde
            param("kde", "nundu", "in 1 day"),
            param("kde", "lido", "1 day ago"),
            # kea
            param("kea", "es simana li", "0 week ago"),
            param("kea", "di li 2 min", "in 2 minute"),
            param("kea", "a ten 6 anu", "6 year ago"),
            # khq
            param("khq", "hõo", "0 day ago"),
            param("khq", "bi", "1 day ago"),
            # ki
            param("ki", "rũciũ", "in 1 day"),
            param("ki", "ira", "1 day ago"),
            # kk
            param("kk", "3 апта бұрын", "3 week ago"),
            param("kk", "5 секундтан кейін", "in 5 second"),
            param("kk", "өткен ай", "1 month ago"),
            # kl
            param("kl", "om 8 sapaatip-akunnera", "in 8 week"),
            param("kl", "for 6 ukioq siden", "6 year ago"),
            param("kl", "om 56 minutsi", "in 56 minute"),
            # kln
            param("kln", "raini", "0 day ago"),
            param("kln", "mutai", "in 1 day"),
            # km
            param("km", "ម៉ោងនេះ", "0 hour ago"),
            param("km", "19 ខែមុន", "19 month ago"),
            param("km", "ក្នុង​រយៈ​ពេល 23 ម៉ោង", "in 23 hour"),
            # kn
            param("kn", "18 ತಿಂಗಳುಗಳ ಹಿಂದೆ", "18 month ago"),
            param("kn", "26 ಸೆಕೆಂಡ್‌ನಲ್ಲಿ", "in 26 second"),
            param("kn", "ಈ ನಿಮಿಷ", "0 minute ago"),
            # ko
            param("ko", "2분 후", "in 2 minute"),
            param("ko", "5년 전", "5 year ago"),
            param("ko", "다음 달", "in 1 month"),
            # ksb
            param("ksb", "keloi", "in 1 day"),
            param("ksb", "evi eo", "0 day ago"),
            # ksf
            param("ksf", "ridúrǝ́", "in 1 day"),
            param("ksf", "rinkɔɔ́", "1 day ago"),
            # ksh
            param("ksh", "nächste woche", "in 1 week"),
            param("ksh", "en 3 johre", "in 3 year"),
            param("ksh", "diese mohnd", "0 month ago"),
            # ky
            param("ky", "ушул мүнөттө", "0 minute ago"),
            param("ky", "6 айд кийин", "in 6 month"),
            param("ky", "5 мүнөт мурун", "5 minute ago"),
            # lag
            param("lag", "lamʉtoondo", "in 1 day"),
            param("lag", "niijo", "1 day ago"),
            # lb
            param("lb", "virun 2 stonn", "2 hour ago"),
            param("lb", "an 5 joer", "in 5 year"),
            param("lb", "leschte mount", "1 month ago"),
            # lg
            param("lg", "nkya", "in 1 day"),
            param("lg", "ggulo", "1 day ago"),
            # lkt
            param("lkt", "hékta wíyawapi 8 k'uŋ héhaŋ", "8 month ago"),
            param("lkt", "tȟokáta okó kiŋháŋ", "in 1 week"),
            param("lkt", "letáŋhaŋ owápȟe 4 kiŋháŋ", "in 4 hour"),
            # ln
            param("ln", "lóbi elékí", "1 day ago"),
            param("ln", "lóbi ekoyâ", "in 1 day"),
            # lo
            param("lo", "ໃນອີກ 5 ຊົ່ວໂມງ", "in 5 hour"),
            param("lo", "3 ປີກ່ອນ", "3 year ago"),
            # lt
            param("lt", "praėjusią savaitę", "1 week ago"),
            param("lt", "prieš 12 mėnesį", "12 month ago"),
            param("lt", "po 2 valandų", "in 2 hour"),
            # lu
            param("lu", "lelu", "0 day ago"),
            param("lu", "makelela", "1 day ago"),
            # luo
            param("luo", "nyoro", "1 day ago"),
            param("luo", "kiny", "in 1 day"),
            # luy
            param("luy", "mgorova", "1 day ago"),
            param("luy", "lero", "0 day ago"),
            # lv
            param("lv", "pēc 67 minūtes", "in 67 minute"),
            param("lv", "pirms 5 nedēļām", "5 week ago"),
            param("lv", "nākamajā gadā", "in 1 year"),
            # mas
            param("mas", "tááisérè", "in 1 day"),
            param("mas", "ŋolé", "1 day ago"),
            # mer
            param("mer", "ĩgoro", "1 day ago"),
            param("mer", "narua", "0 day ago"),
            # mfe
            param("mfe", "zordi", "0 day ago"),
            param("mfe", "demin", "in 1 day"),
            # mg
            param("mg", "rahampitso", "in 1 day"),
            param("mg", "omaly", "1 day ago"),
            # mgh
            param("mgh", "lel'lo", "0 day ago"),
            param("mgh", "n'chana", "1 day ago"),
            # mgo
            param("mgo", "ikwiri", "1 day ago"),
            param("mgo", "isu", "in 1 day"),
            # mk
            param("mk", "пред 4 минута", "4 minute ago"),
            param("mk", "за 6 месеци", "in 6 month"),
            param("mk", "минатата година", "1 year ago"),
            # ml
            param("ml", "ഈ മിനിറ്റിൽ", "0 minute ago"),
            param("ml", "7 മണിക്കൂറിൽ", "in 7 hour"),
            param("ml", "2 വർഷം മുമ്പ്", "2 year ago"),
            # mn
            param("mn", "5 цагийн өмнө", "5 hour ago"),
            param("mn", "10 жилийн дараа", "in 10 year"),
            param("mn", "өнгөрсөн долоо хоног", "1 week ago"),
            # mr
            param("mr", "2 मिनिटांमध्ये", "in 2 minute"),
            param("mr", "5 महिन्यापूर्वी", "5 month ago"),
            param("mr", "हे वर्ष", "0 year ago"),
            # ms
            param("ms", "dalam 7 hari", "in 7 day"),
            param("ms", "3 thn lalu", "3 year ago"),
            param("ms", "bulan depan", "in 1 month"),
            # mt
            param("mt", "ix-xahar li għadda", "1 month ago"),
            param("mt", "2 sena ilu", "2 year ago"),
            param("mt", "il-ġimgħa d-dieħla", "in 1 week"),
            # mua
            param("mua", "tǝ'nahko", "0 day ago"),
            param("mua", "tǝ'nane", "in 1 day"),
            # my
            param("my", "ပြီးခဲ့သည့် 7 မိနစ်", "7 minute ago"),
            param("my", "12 လအတွင်း", "in 12 month"),
            param("my", "ယခု သီတင်းပတ်", "0 week ago"),
            # nb
            param("nb", "om 6 timer", "in 6 hour"),
            param("nb", "om 2 måneder", "in 2 month"),
            param("nb", "forrige uke", "1 week ago"),
            param("nb", "for 3 dager siden", "3 day ago"),
            param("nb", "for 3 timer siden", "3 hour ago"),
            param("nb", "3 dager siden", "3 day ago"),
            param("nb", "3 mnd siden", "3 month ago"),
            param("nb", "2 uker siden", "2 week ago"),
            param("nb", "1 uke siden", "1 week ago"),
            param("nb", "10 timer siden", "10 hour ago"),
            # nd
            param("nd", "kusasa", "in 1 day"),
            param("nd", "izolo", "1 day ago"),
            # ne
            param("ne", "5 वर्ष अघि", "5 year ago"),
            param("ne", "35 मिनेटमा", "in 35 minute"),
            param("ne", "यो हप्ता", "0 week ago"),
            # nl
            param("nl", "15 dgn geleden", "15 day ago"),
            param("nl", "over 2 maand", "in 2 month"),
            param("nl", "vorige jaar", "1 year ago"),
            # nmg
            param("nmg", "nakugú", "1 day ago"),
            param("nmg", "namáná", "in 1 day"),
            # nn
            param("nn", "for 5 minutter siden", "5 minute ago"),
            param("nn", "om 3 uker", "in 3 week"),
            param("nn", "i morgon", "in 1 day"),
            # nnh
            param("nnh", "jǔɔ gẅie à ne ntóo", "in 1 day"),
            param("nnh", "jǔɔ gẅie à ka tɔ̌g", "1 day ago"),
            # nus
            param("nus", "ruun", "in 1 day"),
            param("nus", "walɛ 06:23 tŋ", "0 day ago 06:23 pm"),
            # nyn
            param("nyn", "nyomwabazyo", "1 day ago"),
            param("nyn", "erizooba", "0 day ago"),
            # os
            param("os", "3 боны размӕ", "3 day ago"),
            param("os", "47 сахаты фӕстӕ", "in 47 hour"),
            param("os", "знон", "1 day ago"),
            # pa-Guru
            param("pa-Guru", "ਅਗਲਾ ਹਫ਼ਤਾ", "in 1 week"),
            param("pa-Guru", "5 ਮਹੀਨੇ ਪਹਿਲਾਂ", "5 month ago"),
            param("pa-Guru", "22 ਮਿੰਟਾਂ ਵਿੱਚ", "in 22 minute"),
            # pa
            param("pa", "15 ਘੰਟੇ ਪਹਿਲਾਂ", "15 hour ago"),
            param("pa", "16 ਸਕਿੰਟ ਵਿੱਚ", "in 16 second"),
            param("pa", "ਅਗਲਾ ਸਾਲ", "in 1 year"),
            # pl
            param("pl", "6 tygodni temu", "6 week ago"),
            param("pl", "za 8 lat", "in 8 year"),
            param("pl", "ta minuta", "0 minute ago"),
            # rm
            param("rm", "damaun", "in 1 day"),
            param("rm", "ier", "1 day ago"),
            # ro
            param("ro", "acum 2 de ore", "2 hour ago"),
            param("ro", "peste 5 de ani", "in 5 year"),
            param("ro", "săptămâna trecută", "1 week ago"),
            # rof
            param("rof", "linu", "0 day ago"),
            param("rof", "ng'ama", "in 1 day"),
            # ru
            param("ru", "12 секунд назад", "12 second ago"),
            param("ru", "через 8 месяцев", "in 8 month"),
            param("ru", "в прошлом году", "1 year ago"),
            # rwk
            param("rwk", "ukou", "1 day ago"),
            param("rwk", "ngama", "in 1 day"),
            # sah
            param("sah", "20 чаас ынараа өттүгэр", "20 hour ago"),
            param("sah", "50 сылынан", "in 50 year"),
            param("sah", "ааспыт нэдиэлэ", "1 week ago"),
            # saq
            param("saq", "duo", "0 day ago"),
            param("saq", "taisere", "in 1 day"),
            # sbp
            param("sbp", "pamulaawu", "in 1 day"),
            param("sbp", "ineng'uni", "0 day ago"),
            # se
            param("se", "51 minuhta maŋŋilit", "in 51 minute"),
            param("se", "3 jahkki árat", "3 year ago"),
            param("se", "ihttin", "in 1 day"),
            # seh
            param("seh", "manguana", "in 1 day"),
            param("seh", "zuro", "1 day ago"),
            # ses
            param("ses", "suba", "in 1 day"),
            param("ses", "hõo", "0 day ago"),
            # sg
            param("sg", "bîrï", "1 day ago"),
            param("sg", "lâsô", "0 day ago"),
            # shi-Latn
            param("shi-Latn", "iḍlli", "1 day ago"),
            param("shi-Latn", "askka 06:15 tifawt", "in 1 day 06:15 am"),
            # shi-Tfng
            param("shi-Tfng", "ⴰⵙⴽⴽⴰ", "in 1 day"),
            param("shi-Tfng", "ⴰⵙⵙⴰ", "0 day ago"),
            # shi
            param("shi", "ⵉⴹⵍⵍⵉ", "1 day ago"),
            # si
            param("si", "තත්පර 14කින්", "in 14 second"),
            param("si", "වසර 2කට පෙර", "2 year ago"),
            param("si", "මෙම සතිය", "0 week ago"),
            # sk
            param("sk", "pred 11 týždňami", "11 week ago"),
            param("sk", "o 25 rokov", "in 25 year"),
            param("sk", "v tejto hodine", "0 hour ago"),
            # sl
            param("sl", "pred 4 dnevom", "4 day ago"),
            param("sl", "čez 76 leto", "in 76 year"),
            param("sl", "naslednji mesec", "in 1 month"),
            # sn
            param("sn", "mangwana", "in 1 day"),
            param("sn", "nhasi", "0 day ago"),
            # so
            param("so", "berri", "in 1 day"),
            param("so", "shalay", "1 day ago"),
            # sq
            param("sq", "pas 6 muajsh", "in 6 month"),
            param("sq", "72 orë më parë", "72 hour ago"),
            param("sq", "javën e ardhshme", "in 1 week"),
            # sr-Cyrl
            param("sr-Cyrl", "пре 5 година", "5 year ago"),
            param("sr-Cyrl", "за 52 нед", "in 52 week"),
            param("sr-Cyrl", "данас", "0 day ago"),
            param("sr-Cyrl", "за 3 годину", "in 3 year"),
            # sr-Latn
            param("sr-Latn", "za 120 sekundi", "in 120 second"),
            param("sr-Latn", "pre 365 dana", "365 day ago"),
            param("sr-Latn", "prošle nedelje", "1 week ago"),
            # sr
            param("sr", "пре 40 сати", "40 hour ago"),
            param("sr", "за 100 год", "in 100 year"),
            param("sr", "овог месеца", "0 month ago"),
            # sv
            param("sv", "för 15 vecka sedan", "15 week ago"),
            param("sv", "om 2 sekunder", "in 2 second"),
            param("sv", "förra året", "1 year ago"),
            # sw
            param("sw", "sekunde 25 zilizopita", "25 second ago"),
            param("sw", "miezi 5 iliyopita", "5 month ago"),
            param("sw", "mwaka uliopita", "1 year ago"),
            # ta
            param("ta", "7 நாட்களுக்கு முன்", "7 day ago"),
            param("ta", "45 ஆண்டுகளில்", "in 45 year"),
            param("ta", "இப்போது", "0 second ago"),
            # te
            param("te", "12 గంటల క్రితం", "12 hour ago"),
            param("te", "25 సంవత్సరాల్లో", "in 25 year"),
            param("te", "గత వారం", "1 week ago"),
            # teo
            param("teo", "moi", "in 1 day"),
            param("teo", "lolo", "0 day ago"),
            # to
            param("to", "miniti 'e 5 kuo'osi", "5 minute ago"),
            param("to", "'i he ta'u 'e 6", "in 6 year"),
            param("to", "'aneafi", "1 day ago"),
            # tr
            param("tr", "11 saat önce", "11 hour ago"),
            param("tr", "10 yıl sonra", "in 10 year"),
            param("tr", "geçen ay", "1 month ago"),
            # twq
            param("twq", "hõo", "0 day ago"),
            param("twq", "suba", "in 1 day"),
            # tzm
            param("tzm", "assenaṭ", "1 day ago"),
            param("tzm", "asekka", "in 1 day"),
            # uk
            param("uk", "18 хвилин тому", "18 minute ago"),
            param("uk", "через 22 роки", "in 22 year"),
            param("uk", "цього тижня", "0 week ago"),
            param("uk", "півгодини тому", "30 minute ago"),
            param("uk", "пів години тому", "30 minute ago"),
            param("uk", "півроку тому", "6 month ago"),
            param("uk", "за півтора року", "in 18 month"),
            # uz-Cyrl
            param("uz-Cyrl", "кейинги ой", "in 1 month"),
            param("uz-Cyrl", "30 йил аввал", "30 year ago"),
            param("uz-Cyrl", "59 сониядан сўнг", "in 59 second"),
            # uz-Latn
            param("uz-Latn", "3 haftadan keyin", "in 3 week"),
            param("uz-Latn", "5 soat oldin", "5 hour ago"),
            param("uz-Latn", "shu yil", "0 year ago"),
            # uz
            param("uz", "25 soat oldin", "25 hour ago"),
            param("uz", "8 yildan keyin", "in 8 year"),
            param("uz", "bugun", "0 day ago"),
            # vi
            param("vi", "sau 22 giờ nữa", "in 22 hour"),
            param("vi", "15 tháng trước", "15 month ago"),
            param("vi", "tuần sau", "in 1 week"),
            # vun
            param("vun", "ngama", "in 1 day"),
            param("vun", "ukou", "1 day ago"),
            # wae
            param("wae", "vor 11 minüta", "11 minute ago"),
            param("wae", "i 15 wuča", "in 15 week"),
            param("wae", "hitte", "0 day ago"),
            # xog
            param("xog", "enkyo", "in 1 day"),
            param("xog", "edho", "1 day ago"),
            # yav
            param("yav", "ínaan", "0 day ago"),
            param("yav", "nakinyám", "in 1 day"),
            # yo
            param("yo", "ọ̀la", "in 1 day"),
            param("yo", "òní", "0 day ago"),
            # yue
            param("yue", "13 個星期後", "in 13 week"),
            param("yue", "2 小時前", "2 hour ago"),
            param("yue", "上個月", "1 month ago"),
            # zgh
            param("zgh", "ⴰⵙⵙⴰ", "0 day ago"),
            param("zgh", "ⵉⴹⵍⵍⵉ", "1 day ago"),
            # zh-Hans
            param("zh-Hans", "3秒钟后", "in 3 second"),
            param("zh-Hans", "4年后", "in 4 year"),
            param("zh-Hans", "上周", "1 week ago"),
            # zh-Hant
            param("zh-Hant", "7 分鐘後", "in 7 minute"),
            param("zh-Hant", "12 個月前", "12 month ago"),
            param("zh-Hant", "這一小時", "0 hour ago"),
            # zu
            param("zu", "10 amaminithi edlule", "10 minute ago"),
            param("zu", "20 unyaka odlule", "20 year ago"),
            param("zu", "manje", "0 second ago"),
        ]
    )
    def test_freshness_translation(
        self, shortname, datetime_string, expected_translation
    ):
        self.given_settings(settings={"NORMALIZE": False})
        # Finnish language use "t" as hour, so empty SKIP_TOKENS.
        if shortname == "fi":
            self.settings.SKIP_TOKENS = []
        self.given_bundled_language(shortname)
        self.given_string(datetime_string)
        self.when_datetime_string_translated()
        self.then_string_translated_to(expected_translation)

    @parameterized.expand(
        [
            param(
                "pt",
                "sexta-feira, 10 de junho de 2014 14:52",
                [
                    "sexta-feira",
                    " ",
                    "10",
                    " ",
                    "de",
                    " ",
                    "junho",
                    " ",
                    "de",
                    " ",
                    "2014",
                    " ",
                    "14",
                    ":",
                    "52",
                ],
            ),
            param("it", "14_luglio_15", ["14", "luglio", "15"]),
            param("zh", "1年11个月", ["1", "年", "11", "个月"]),
            param("zh", "1年11個月", ["1", "年", "11", "個月"]),
            param("tr", "2 saat önce", ["2 saat önce"]),
            param(
                "fr",
                "il ya environ 23 heures'",
                ["il ya", " ", "environ", " ", "23", " ", "heures"],
            ),
            param(
                "de", "Gestern um 04:41", ["Gestern", " ", "um", " ", "04", ":", "41"]
            ),
            param(
                "de",
                "Donnerstag, 8. Januar 2015 um 07:17",
                [
                    "Donnerstag",
                    " ",
                    "8",
                    ".",
                    " ",
                    "Januar",
                    " ",
                    "2015",
                    " ",
                    "um",
                    " ",
                    "07",
                    ":",
                    "17",
                ],
            ),
            param(
                "ru",
                "8 января 2015 г. в 9:10",
                [
                    "8",
                    " ",
                    "января",
                    " ",
                    "2015",
                    " ",
                    "г",
                    ".",
                    " ",
                    "в",
                    " ",
                    "9",
                    ":",
                    "10",
                ],
            ),
            param(
                "cs",
                "6. leden 2015 v 22:29",
                ["6", ".", " ", "leden", " ", "2015", " ", "v", " ", "22", ":", "29"],
            ),
            param(
                "nl",
                "woensdag 7 januari 2015 om 21:32",
                [
                    "woensdag",
                    " ",
                    "7",
                    " ",
                    "januari",
                    " ",
                    "2015",
                    " ",
                    "om",
                    " ",
                    "21",
                    ":",
                    "32",
                ],
            ),
            param(
                "ro",
                "8 Ianuarie 2015 la 13:33",
                ["8", " ", "Ianuarie", " ", "2015", " ", "la", " ", "13", ":", "33"],
            ),
            param(
                "ar",
                "8 يناير، 2015، الساعة 10:01 صباحاً",
                [
                    "8",
                    " ",
                    "يناير",
                    " ",
                    "2015",
                    "الساعة",
                    " ",
                    "10",
                    ":",
                    "01",
                    " ",
                    "صباحاً",
                ],
            ),
            param(
                "th",
                "8 มกราคม 2015 เวลา 12:22 น.",
                [
                    "8",
                    " ",
                    "มกราคม",
                    " ",
                    "2015",
                    " ",
                    "เวลา",
                    " ",
                    "12",
                    ":",
                    "22",
                    " ",
                    "น.",
                ],
            ),
            param(
                "pl",
                "8 stycznia 2015 o 10:19",
                ["8", " ", "stycznia", " ", "2015", " ", "o", " ", "10", ":", "19"],
            ),
            param(
                "vi",
                "Thứ Năm, ngày 8 tháng 1 năm 2015",
                [
                    "Thứ Năm",
                    " ",
                    "ngày",
                    " ",
                    "8",
                    " ",
                    "tháng 1",
                    " ",
                    "năm",
                    " ",
                    "2015",
                ],
            ),
            param(
                "tl",
                "Biyernes Hulyo 3 2015",
                ["Biyernes", " ", "Hulyo", " ", "3", " ", "2015"],
            ),
            param(
                "be",
                "3 верасня 2015 г. у 11:10",
                [
                    "3",
                    " ",
                    "верасня",
                    " ",
                    "2015",
                    " ",
                    "г",
                    ".",
                    " ",
                    "у",
                    " ",
                    "11",
                    ":",
                    "10",
                ],
            ),
            param(
                "id",
                "3 Juni 2015 13:05:46",
                ["3", " ", "Juni", " ", "2015", " ", "13", ":", "05", ":", "46"],
            ),
            param(
                "he",
                "ה-21 לאוקטובר 2016 ב-15:00",
                ["ה-", "21", " ", "לאוקטובר", " ", "2016", " ", "ב-", "15", ":", "00"],
            ),
            param(
                "bn",
                "3 জুন 2015 13:05:46",
                ["3", " ", "জুন", " ", "2015", " ", "13", ":", "05", ":", "46"],
            ),
            param(
                "hi",
                "13 मार्च 2013 11:15:09",
                ["13", " ", "मार्च", " ", "2013", " ", "11", ":", "15", ":", "09"],
            ),
            param(
                "mgo",
                "aneg 5 12 iməg àdùmbə̀ŋ 2001 09:14 pm",
                [
                    "aneg 5",
                    " ",
                    "12",
                    " ",
                    "iməg àdùmbə̀ŋ",
                    " ",
                    "2001",
                    " ",
                    "09",
                    ":",
                    "14",
                    " ",
                    "pm",
                ],
            ),
            param(
                "qu",
                "2 kapaq raymi 1998 domingo",
                ["2", " ", "kapaq raymi", " ", "1998", " ", "domingo"],
            ),
            param(
                "os",
                "24 сахаты размӕ 10:09 ӕмбисбоны размӕ",
                ["24 сахаты размӕ", " ", "10", ":", "09", " ", "ӕмбисбоны размӕ"],
            ),
            param(
                "pa",
                "25 ਘੰਟੇ ਪਹਿਲਾਂ 10:08 ਬਾਦੁ",
                ["25 ਘੰਟੇ ਪਹਿਲਾਂ", " ", "10", ":", "08", " ", "ਬਾਦੁ"],
            ),
            param("en", "25_April_2008", ["25", "April", "2008"]),
            param(
                "af",
                "hierdie uur 10:19 vm",
                ["hierdie uur", " ", "10", ":", "19", " ", "vm"],
            ),
            param(
                "rof",
                "7 mweri wa kaana 1998 12:09 kang'ama",
                [
                    "7",
                    " ",
                    "mweri wa kaana",
                    " ",
                    "1998",
                    " ",
                    "12",
                    ":",
                    "09",
                    " ",
                    "kang'ama",
                ],
            ),
            param(
                "saq",
                "14 lapa le tomon obo 2098 ong",
                ["14", " ", "lapa le tomon obo", " ", "2098", " ", "ong"],
            ),
            param(
                "wae",
                "cor 6 wučä 09:19 pm",
                ["cor 6 wučä", " ", "09", ":", "19", " ", "pm"],
            ),
            param("naq", "13 ǃkhanǀgôab 1887", ["13", " ", "ǃkhanǀgôab", " ", "1887"]),
        ]
    )
    def test_split(self, shortname, datetime_string, expected_tokens):
        self.given_settings(settings={"NORMALIZE": False})
        self.given_bundled_language(shortname)
        self.given_string(datetime_string)
        self.when_datetime_string_splitted()
        self.then_tokens_are(expected_tokens)

    @parameterized.expand(
        [
            param("en", "17th October, 2034 @ 01:08 am PDT", strip_timezone=True),
            param("en", "#@Sept#04#2014", strip_timezone=False),
            param("en", "2014-12-13T00:11:00Z", strip_timezone=False),
            param("de", "Donnerstag, 8. Januar 2015 um 07:17", strip_timezone=False),
            param("da", "Torsdag, 8. januar 2015 kl. 07:17", strip_timezone=False),
            param("ru", "8 января 2015 г. в 9:10", strip_timezone=False),
            param("cs", "Pondělí v 22:29", strip_timezone=False),
            param("nl", "woensdag 7 januari om 21:32", strip_timezone=False),
            param("ro", "8 Ianuarie 2015 la 13:33", strip_timezone=False),
            param("ar", "ساعتين", strip_timezone=False),
            param("tr", "3 hafta", strip_timezone=False),
            param("th", "17 เดือนมิถุนายน", strip_timezone=False),
            param("pl", "przedwczoraj", strip_timezone=False),
            param("fa", "ژانویه 8, 2015، ساعت 15:46", strip_timezone=False),
            param("vi", "2 tuần 3 ngày", strip_timezone=False),
            param("tl", "Hulyo 3, 2015 7:00 pm", strip_timezone=False),
            param("be", "3 верасня 2015 г. у 11:10", strip_timezone=False),
            param("id", "01 Agustus 2015 18:23", strip_timezone=False),
            param("he", "6 לדצמבר 1973", strip_timezone=False),
            param("bn", "3 সপ্তাহ", strip_timezone=False),
        ]
    )
    def test_applicable_languages(self, shortname, datetime_string, strip_timezone):
        self.given_settings()
        self.given_bundled_language(shortname)
        self.given_string(datetime_string)
        self.when_datetime_string_checked_if_applicable(strip_timezone)
        self.then_language_is_applicable()

    @parameterized.expand(
        [
            param("ru", "08.haziran.2014, 11:07", strip_timezone=False),
            param("ar", "6 دقیقه", strip_timezone=False),
            param("fa", "ساعتين", strip_timezone=False),
            param("cs", "3 hafta", strip_timezone=False),
        ]
    )
    def test_not_applicable_languages(self, shortname, datetime_string, strip_timezone):
        self.given_settings()
        self.given_bundled_language(shortname)
        self.given_string(datetime_string)
        self.when_datetime_string_checked_if_applicable(strip_timezone)
        self.then_language_is_not_applicable()

    @apply_settings
    def given_settings(self, settings=None):
        self.settings = settings

    def given_string(self, datetime_string):
        if self.settings.NORMALIZE:
            datetime_string = normalize_unicode(datetime_string)
        self.datetime_string = datetime_string

    def given_bundled_language(self, shortname):
        self.language = default_loader.get_locale(shortname)

    def when_datetime_string_translated(self):
        self.translation = self.language.translate(
            self.datetime_string, settings=self.settings
        )

    def when_datetime_string_splitted(self, keep_formatting=False):
        self.tokens = self.language._get_dictionary(self.settings).split(
            self.datetime_string
        )

    def when_datetime_string_checked_if_applicable(self, strip_timezone):
        self.result = self.language.is_applicable(
            self.datetime_string, strip_timezone, settings=self.settings
        )

    def then_string_translated_to(self, expected_string):
        self.assertEqual(expected_string, self.translation)

    def then_tokens_are(self, expected_tokens):
        self.assertEqual(expected_tokens, self.tokens)

    def then_language_is_applicable(self):
        self.assertTrue(self.result)

    def then_language_is_not_applicable(self):
        self.assertFalse(self.result)


class BaseLanguageDetectorTestCase(BaseTestCase):
    __test__ = False

    NOT_DETECTED = object()

    def setUp(self):
        super().setUp()
        self.datetime_string = NotImplemented
        self.detector = NotImplemented
        self.detected_language = NotImplemented
        self.known_languages = None

    @parameterized.expand(
        [
            param("1 january 2015", "en"),
        ]
    )
    def test_valid_dates_detected(self, datetime_string, expected_language):
        self.given_locales(expected_language)
        self.given_detector()
        self.given_settings()
        self.given_string(datetime_string)
        self.when_searching_for_first_applicable_language()
        self.then_language_was_detected(expected_language)

    @parameterized.expand(
        [
            param("foo"),
        ]
    )
    def test_invalid_dates_not_detected(self, datetime_string):
        self.given_locales("en")
        self.given_detector()
        self.given_settings()
        self.given_string(datetime_string)
        self.when_searching_for_first_applicable_language()
        self.then_no_language_was_detected()

    def test_invalid_date_after_valid_date_not_detected(self):
        self.given_settings()
        self.given_locales("en")
        self.given_detector()
        self.given_previously_detected_string("1 january 2015")
        self.given_string("foo")
        self.when_searching_for_first_applicable_language()
        self.then_no_language_was_detected()

    def test_valid_date_after_invalid_date_detected(self):
        self.given_locales("en")
        self.given_settings()
        self.given_detector()
        self.given_previously_detected_string("foo")
        self.given_string("1 january 2015")
        self.when_searching_for_first_applicable_language()
        self.then_language_was_detected("en")

    def given_locales(self, *shortnames):
        self.known_languages = [
            default_loader.get_locale(shortname) for shortname in shortnames
        ]

    @apply_settings
    def given_settings(self, settings=None):
        self.settings = settings

    def given_previously_detected_string(self, datetime_string):
        for _ in self.detector.iterate_applicable_languages(
            datetime_string, modify=True, settings=self.settings
        ):
            break

    def given_string(self, datetime_string):
        self.datetime_string = datetime_string

    def given_detector(self):
        raise NotImplementedError

    def when_searching_for_first_applicable_language(self):
        for language in self.detector.iterate_applicable_languages(
            self.datetime_string, modify=True, settings=self.settings
        ):
            self.detected_language = language
            break
        else:
            self.detected_language = self.NOT_DETECTED

    def then_language_was_detected(self, shortname):
        self.assertIsInstance(
            self.detected_language, Locale, "Language was not properly detected"
        )
        self.assertEqual(shortname, self.detected_language.shortname)

    def then_no_language_was_detected(self):
        self.assertIs(self.detected_language, self.NOT_DETECTED)


class TestExactLanguages(BaseLanguageDetectorTestCase):
    __test__ = True

    @parameterized.expand(
        [
            param("01-01-12", ["en", "fr"]),
            param("01-01-12", ["tr", "ar"]),
            param("01-01-12", ["ru", "fr", "en", "pl"]),
            param("01-01-12", ["en"]),
        ]
    )
    def test_exact_languages(self, datetime_string, shortnames):
        self.given_settings()
        self.given_string(datetime_string)
        self.given_known_languages(shortnames)
        self.given_detector()
        self.when_using_exact_languages()
        self.then_exact_languages_were_filtered(shortnames)

    def test_none_raises_value_error(self):
        with self.assertRaisesRegex(
            ValueError, r"language cannot be None for ExactLanguages"
        ):
            ExactLanguages(None)

    @apply_settings
    def given_settings(self, settings=None):
        self.settings = settings

    def given_known_languages(self, shortnames):
        self.known_languages = [
            default_loader.get_locale(shortname) for shortname in shortnames
        ]

    def given_detector(self):
        self.assertIsInstance(
            self.known_languages, list, "Require a list of languages to initialize"
        )
        self.assertGreaterEqual(
            len(self.known_languages),
            1,
            "Could only be initialized with one or more languages",
        )
        self.detector = ExactLanguages(languages=self.known_languages)

    def when_using_exact_languages(self):
        self.exact_languages = self.detector.iterate_applicable_languages(
            self.datetime_string, modify=True, settings=self.settings
        )

    def then_exact_languages_were_filtered(self, shortnames):
        self.assertEqual(
            set(shortnames), {lang.shortname for lang in self.exact_languages}
        )


class BaseAutoDetectLanguageDetectorTestCase(BaseLanguageDetectorTestCase):
    allow_redetection = NotImplemented

    def given_detector(self):
        self.detector = AutoDetectLanguage(
            languages=self.known_languages, allow_redetection=self.allow_redetection
        )

    @apply_settings
    def given_settings(self, settings=None):
        self.settings = settings


class TestAutoDetectLanguageDetectorWithoutRedetection(
    BaseAutoDetectLanguageDetectorTestCase
):
    __test__ = True
    allow_redetection = False


class TestAutoDetectLanguageDetectorWithRedetection(
    BaseAutoDetectLanguageDetectorTestCase
):
    __test__ = True
    allow_redetection = True


class TestLanguageValidatorWhenInvalid(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.validator = LanguageValidator
        self.captured_logs = StringIO()
        self.validator.get_logger()
        self.sh = logging.StreamHandler(self.captured_logs)
        self.validator.logger.addHandler(self.sh)
        self.log_list = self.captured_logs.getvalue().split("\n")[0]

    def get_log_str(self):
        return self.captured_logs.getvalue().split("\n")[0]

    @parameterized.expand(
        [
            param(
                "en",
                "string instead of dict",
                log_msg="Language 'en' info expected to be dict, but have got str",
            ),
        ]
    )
    def test_validate_info_when_invalid_type(self, lang_id, lang_info, log_msg):
        result = self.validator.validate_info(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param("en", {}, log_msg="Language 'en' does not have a name"),
            param("en", {"name": 22}, log_msg="Language 'en' does not have a name"),
            param("en", {"name": ""}, log_msg="Language 'en' does not have a name"),
        ]
    )
    def test_validate_name_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_name(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                "en",
                {"no_word_spacing": "string instead of bool"},
                log_msg="Invalid 'no_word_spacing' value 'string instead of bool' for 'en' language: "
                "expected boolean",
            ),
        ]
    )
    def test_validate_word_spacing_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_word_spacing(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                "en",
                {"skip": "string instead of list"},
                log_msg="Invalid 'skip' list for 'en' language: "
                "expected list type but have got str",
            ),
            param(
                "en",
                {"skip": [""]},
                log_msg="Invalid 'skip' token '' for 'en' language: "
                "expected not empty string",
            ),
        ]
    )
    def test_validate_skip_list_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_skip_list(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param("en", {}),
        ]
    )
    def test_validate_skip_list_when_absent(self, lang_id, lang_info):
        result = self.validator._validate_skip_list(lang_id, lang_info)
        self.assertTrue(result)

    @parameterized.expand(
        [
            param(
                "en",
                {"pertain": "it is a string", "skip": [""]},
                log_msg="Invalid 'pertain' token '' for 'en' language: expected not empty string",
            ),
            param(
                "en",
                {"pertain": [""], "skip": "it is a string"},
                log_msg="Invalid 'pertain' list for 'en' language: expected list type but have got str",
            ),
        ]
    )
    def test_validate_pertain_list_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_pertain_list(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param("en", {}),
        ]
    )
    def test_validate_pertain_list_when_absent(self, lang_id, lang_info):
        result = self.validator._validate_pertain_list(lang_id, lang_info)
        self.assertTrue(result)

    @parameterized.expand(
        [
            param(
                "en",
                {},
                log_msg="No translations for 'monday' provided for 'en' language",
            ),
            param(
                "en",
                {
                    "monday": 1,
                    "tuesday": 2,
                    "wednesday": 3,
                    "thursday": 4,
                    "friday": 5,
                    "saturday": 6,
                    "sunday": 7,
                },
                log_msg="Invalid 'monday' translations list for 'en' language: expected list type but have got int",
            ),
            param(
                "en",
                {
                    "monday": [1],
                    "tuesday": [2],
                    "wednesday": [3],
                    "thursday": [4],
                    "friday": [5],
                    "saturday": [6],
                    "sunday": [7],
                },
                log_msg="Invalid 'monday' translation 1 for 'en' language: expected not empty string",
            ),
        ]
    )
    def test_validate_weekdays_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_weekdays(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                "en",
                {},
                log_msg="No translations for 'january' provided for 'en' language",
            ),
            param(
                "en",
                {
                    "january": 1,
                    "february": 2,
                    "march": 3,
                    "april": 4,
                    "may": 5,
                    "june": 6,
                    "july": 7,
                    "august": 8,
                    "september": 9,
                    "october": 10,
                    "november": 11,
                    "december": 12,
                },
                log_msg="Invalid 'january' translations list for 'en' language: expected list type but have got int",
            ),
            param(
                "en",
                {
                    "january": [1],
                    "february": [2],
                    "march": [3],
                    "april": [4],
                    "may": [5],
                    "june": [6],
                    "july": [7],
                    "august": [8],
                    "september": [9],
                    "october": [10],
                    "november": [11],
                    "december": [12],
                },
                log_msg="Invalid 'january' translation 1 for 'en' language: expected not empty string",
            ),
        ]
    )
    def test_validate_months_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_months(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                "en",
                {},
                log_msg="No translations for 'year' provided for 'en' language",
            ),
            param(
                "en",
                {
                    "year": 1,
                    "month": 2,
                    "week": 3,
                    "day": 4,
                    "hour": 5,
                    "minute": 6,
                    "second": 7,
                },
                log_msg="Invalid 'year' translations list for 'en' language: expected list type but have got int",
            ),
            param(
                "en",
                {
                    "year": [1],
                    "month": [2],
                    "week": [3],
                    "day": [4],
                    "hour": [5],
                    "minute": [6],
                    "second": [7],
                },
                log_msg="Invalid 'year' translation 1 for 'en' language: expected not empty string",
            ),
        ]
    )
    def test_validate_units_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_units(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                "en", {}, log_msg="No translations for 'ago' provided for 'en' language"
            ),
            param(
                "en",
                {"ago": 1},
                log_msg="Invalid 'ago' translations list for 'en' language: "
                "expected list type but have got int",
            ),
            param(
                "en",
                {"ago": []},
                log_msg="No translations for 'ago' provided for 'en' language",
            ),
            param(
                "en",
                {"ago": [""]},
                log_msg="Invalid 'ago' translation '' for 'en' language: expected not empty string",
            ),
        ]
    )
    def test_validate_other_words_when_invalid(self, lang_id, lang_info, log_msg="na"):
        result = self.validator._validate_other_words(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param("en", {}),
        ]
    )
    def test_validate_simplifications_when_absent(self, lang_id, lang_info):
        result = self.validator._validate_simplifications(lang_id, lang_info)
        self.assertTrue(result)

    @parameterized.expand(
        [
            param(
                "en",
                {"simplifications": "string instead of list"},
                log_msg="Invalid 'simplifications' list for 'en' language: expected list type but have got str",
            ),
            param(
                "en",
                {"simplifications": [{}]},
                log_msg="Invalid simplification {} for 'en' language: eash simplification suppose "
                "to be one-to-one mapping",
            ),
            param(
                "en",
                {"simplifications": [{28: []}]},
                log_msg="Invalid simplification {28: []} for 'en' language: each simplification suppose "
                "to be string-to-string-or-int mapping",
            ),
            param(
                "en",
                {"simplifications": [{"simplification": []}]},
                log_msg="Invalid simplification {'simplification': []} for 'en' language: each simplification suppose "
                "to be string-to-string-or-int mapping",
            ),
            param(
                "en",
                {"simplifications": [{r"(\d+)\s*hr(s?)\g<(.+?)>": r"\1 hour\2"}]},
                log_msg="Invalid simplification {'(\\\\d+)\\\\s*hr(s?)\\\\g<(.+?)>': '\\\\1 hour\\\\2'} "
                "for 'en' language: groups 3 were not used",
            ),
            param(
                "en",
                {"simplifications": [{"(one)(two)(three)": r"\1\3\2\4"}]},
                log_msg="Invalid simplification {'(one)(two)(three)': '\\\\1\\\\3\\\\2\\\\4'} for 'en' language:"
                " unknown groups 4",
            ),
            param(
                "en",
                {"simplifications": [{r"(?P<A>\w+)(?P<B>\w+)": "\\g<A>"}]},
                log_msg="Invalid simplification {'(?P<A>\\\\w+)(?P<B>\\\\w+)': '\\\\g<A>'} for 'en' language:"
                " groups 2 were not used",
            ),
            param(
                "en",
                {"simplifications": [{r"(?P<A>\w+)": "\\g<B>(.*?)"}]},
                log_msg="Invalid simplification {'(?P<A>\\\\w+)': '\\\\g<B>(.*?)'} for 'en' language: unknown group B",
            ),
        ]
    )
    def test_validate_simplifications_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_simplifications(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                "en",
                {"sentence_splitter_group": "string instead of int"},
                log_msg="Invalid 'sentence_splitter_group' for 'en' language: "
                "expected int type but have got str",
            ),
            param(
                "en",
                {"sentence_splitter_group": 48},
                log_msg="Invalid 'sentence_splitter_group' number 48 for 'en' language: "
                "expected number from 1 to 6",
            ),
        ]
    )
    def test_validate_sentence_splitter_group_when_invalid(
        self, lang_id, lang_info, log_msg
    ):
        result = self.validator._validate_sentence_splitter_group(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                "en",
                {"invalid_key": ""},
                log_msg="Extra keys found for 'en' language: 'invalid_key'",
            ),
        ]
    )
    def test_validate_extra_keys_when_invalid(self, lang_id, lang_info, log_msg):
        result = self.validator._validate_extra_keys(lang_id, lang_info)
        self.assertEqual(log_msg, self.get_log_str())
        self.assertFalse(result)

    @parameterized.expand(
        [
            param(
                date_string="3 de marzo 2019",
                languages=["en"],
                settings={"DEFAULT_LANGUAGES": ["es"]},
                expected=datetime(2019, 3, 3, 0, 0),
            ),
        ]
    )
    def test_parse_settings_default_languages(
        self, date_string, languages, settings, expected
    ):
        result = parse(date_string, languages=languages, settings=settings)
        assert result == expected

    @parameterized.expand(
        [
            param(
                date_string="3 de marzo 2019",
                languages=["en"],
                settings={"DEFAULT_LANGUAGES": ["es"]},
                expected=datetime(2019, 3, 3, 0, 0),
            ),
        ]
    )
    def test_date_data_parser_settings_default_languages(
        self, date_string, languages, settings, expected
    ):
        ddp = DateDataParser(languages=languages, settings=settings)
        result = ddp.get_date_data(date_string)
        assert result.date_obj == expected

    @parameterized.expand(
        [
            param(
                date_string="3 de marzo 2019",
                settings={"DEFAULT_LANGUAGES": ["es"]},
                expected=[("3 de marzo 2019", datetime(2019, 3, 3, 0, 0))],
            ),
        ]
    )
    def test_search_dates_settings_default_languages(
        self, date_string, settings, expected
    ):
        result = search_dates(date_string, settings=settings)
        assert result == expected

    @parameterized.expand(
        [param(date_string="RANDOM_WORD ", settings={"DEFAULT_LANGUAGES": ["en"]})]
    )
    def test_parse_settings_default_languages_no_language_detect(
        self, date_string, settings
    ):
        result = parse(date_string, settings=settings)
        assert result is None

    @parameterized.expand(
        [
            param(
                date_string="29 mai 2021",
                languages=["fr"],
                expected=datetime(2021, 5, 29, 0, 0),
                settings={"DEFAULT_LANGUAGES": ["en", "es"]},
            ),
        ]
    )
    def test_parse_settings_default_languages_with_detected_language(
        self, date_string, languages, expected, settings
    ):
        result = parse(date_string, languages=languages, settings=settings)
        assert result == expected
