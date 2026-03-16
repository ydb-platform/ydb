import datetime
from datetime import timedelta

import pytz
from parameterized import param, parameterized

from dateparser.conf import Settings, apply_settings
from dateparser.search import search_dates
from dateparser.search.search import DateSearchWithDetection
from dateparser.timezone_parser import StaticTzInfo
from dateparser_data.settings import default_parsers
from tests import BaseTestCase

today = datetime.datetime.today()


class TestTranslateSearch(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.search_with_detection = DateSearchWithDetection()
        self.exact_language_search = self.search_with_detection.search

    def run_search_dates_function_invalid_languages(self, text, languages, error_type):
        try:
            search_dates(text=text, languages=languages)
        except Exception as error:
            self.error = error
            self.assertIsInstance(self.error, error_type)

    def check_error_message(self, message):
        self.assertEqual(str(self.error), message)

    @parameterized.expand(
        [
            # English
            param("en", "Sep 03 2014"),
            param("en", "friday, 03 september 2014"),
            param("en", "Aug 06, 2018 05:05 PM CDT"),
            # Chinese
            param("zh", "1年11个月"),
            param("zh", "1年11個月"),
            param("zh", "2015年04月08日10点05"),
            param("zh", "2015年04月08日10:05"),
            param("zh", "2013年04月08日"),
            param("zh", "周一"),
            param("zh", "礼拜一"),
            param("zh", "周二"),
            param("zh", "礼拜二"),
            param("zh", "周三"),
            param("zh", "礼拜三"),
            param("zh", "星期日 2015年04月08日10:05"),
            param("zh", "周六 2013年04月08日"),
            param("zh", "下午3:30"),
            param("zh", "凌晨3:30"),
            param("zh", "中午"),
            # French
            param("fr", "20 Février 2012"),
            param("fr", "Mercredi 19 Novembre 2013"),
            param("fr", "18 octobre 2012 à 19 h 21 min"),
            # German
            param("de", "29. Juni 2007"),
            param("de", "Montag 5 Januar, 2015"),
            # Hungarian
            param("hu", "2016 augusztus 11"),
            param("hu", "2016-08-13 szombat 10:21"),
            param("hu", "2016. augusztus 14. vasárnap 10:21"),
            param("hu", "hétfő"),
            param("hu", "tegnapelőtt"),
            param("hu", "ma"),
            param("hu", "2 hónappal ezelőtt"),
            param("hu", "2016-08-13 szombat 10:21 GMT"),
            # Spanish
            param("es", "Miércoles 31 Diciembre 2014"),
            # Italian
            param("it", "Giovedi Maggio 29 2013"),
            param("it", "19 Luglio 2013"),
            # Portuguese
            param("pt", "22 de dezembro de 2014 às 02:38"),
            # Russian
            param("ru", "5 августа 2014 г в 12:00"),
            # Real: param('ru', "5 августа 2014 г. в 12:00"),
            # Turkish
            param("tr", "2 Ocak 2015 Cuma, 16:49"),
            # Czech
            param("cs", "22. prosinec 2014 v 2:38"),
            # Dutch
            param("nl", "maandag 22 december 2014 om 2:38"),
            # Romanian
            param("ro", "22 Decembrie 2014 la 02:38"),
            # Polish
            param("pl", "4 stycznia o 13:50"),
            param("pl", "29 listopada 2014 o 08:40"),
            # Ukrainian
            param("uk", "30 листопада 2013 о 04:27"),
            # Belarusian
            param("be", "5 снежня 2015 г у 12:00"),
            # Real: param('be', "5 снежня 2015 г. у 12:00"), Issue: Abbreviation segmentation.
            param("be", "11 верасня 2015 г у 12:11"),
            # Real: param('be', "11 верасня 2015 г. у 12:11"),
            param("be", "3 стд 2015 г у 10:33"),
            # Real: param('be', "3 стд 2015 г. у 10:33"),
            # Arabic
            param("ar", "6 يناير، 2015، الساعة 05:16 مساءً"),
            param("ar", "7 يناير، 2015، الساعة 11:00 صباحاً"),
            # Vietnamese
            # Disabled - wrong segmentation at "Thứ Năm"
            # param('vi', "Thứ Năm, ngày 8 tháng 1 năm 2015"),
            # Disabled - wrong segmentation at "Thứ Tư"
            # param('vi', "Thứ Tư, 07/01/2015 | 22:34"),
            param("vi", "9 Tháng 1 2015 lúc 15:08"),
            # Thai
            # Disabled - spacing differences
            # param('th', "เมื่อ กุมภาพันธ์ 09, 2015, 09:27:57 AM"),
            # param('th', "เมื่อ กรกฎาคม 05, 2012, 01:18:06 AM"),
            # Tagalog
            param("tl", "Biyernes Hulyo 3, 2015"),
            param("tl", "Pebrero 5, 2015 7:00 pm"),
            # Indonesian
            param("id", "06 Sep 2015"),
            param("id", "07 Feb 2015 20:15"),
            # Miscellaneous
            param("en", "2014-12-12T12:33:39-08:00"),
            param("en", "2014-10-15T16:12:20+00:00"),
            param("en", "28 Oct 2014 16:39:01 +0000"),
            # Disabled - wrong split at "a las".
            # param('es', "13 Febrero 2015 a las 23:00"),
            # Danish
            param("da", "Sep 03 2014"),
            param("da", "fredag, 03 september 2014"),
            param("da", "fredag d. 3 september 2014"),
            # Finnish
            param("fi", "maanantai tammikuu 16, 2015"),
            param("fi", "ma tammi 16, 2015"),
            param("fi", "tiistai helmikuu 16, 2015"),
            param("fi", "ti helmi 16, 2015"),
            param("fi", "keskiviikko maaliskuu 16, 2015"),
            param("fi", "ke maalis 16, 2015"),
            param("fi", "torstai huhtikuu 16, 2015"),
            param("fi", "to huhti 16, 2015"),
            param("fi", "perjantai toukokuu 16, 2015"),
            param("fi", "pe touko 16, 2015"),
            param("fi", "lauantai kesäkuu 16, 2015"),
            param("fi", "la kesä 16, 2015"),
            param("fi", "sunnuntai heinäkuu 16, 2015"),
            param("fi", "su heinä 16, 2015"),
            param("fi", "su elokuu 16, 2015"),
            param("fi", "su elo 16, 2015"),
            param("fi", "su syyskuu 16, 2015"),
            param("fi", "su syys 16, 2015"),
            param("fi", "su lokakuu 16, 2015"),
            param("fi", "su loka 16, 2015"),
            param("fi", "su marraskuu 16, 2015"),
            param("fi", "su marras 16, 2015"),
            param("fi", "su joulukuu 16, 2015"),
            param("fi", "su joulu 16, 2015"),
            param("fi", "1. tammikuuta, 2016"),
            param("fi", "tiistaina, 27. lokakuuta 2015"),
            # Japanese
            param("ja", "午後3時"),
            param("ja", "2時"),
            param("ja", "11時42分"),
            param("ja", "3ヶ月"),
            param("ja", "約53か月前"),
            param("ja", "3月"),
            param("ja", "十二月"),
            param("ja", "2月10日"),
            param("ja", "2013年2月"),
            param("ja", "2013年04月08日"),
            param("ja", "2016年03月24日 木曜日 10時05分"),
            param("ja", "2016年3月20日 21時40分"),
            param("ja", "2016年03月21日 23時05分11秒"),
            param("ja", "2016年3月21日(月) 14時48分"),
            param("ja", "2016年3月20日(日) 21時40分"),
            param("ja", "2016年3月20日 (日) 21時40分"),
            param("ja", "正午"),
            param("ja", "明後日"),
            param("ja", "明後日の正午"),
            # Hebrew
            param("he", "20 לאפריל 2012"),
            param("he", "יום רביעי ה-19 בנובמבר 2013"),
            param("he", "18 לאוקטובר 2012 בשעה 19:21"),
            # Disabled - wrong split at "יום ה'".
            # param('he', "יום ה' 6/10/2016"),
            param("he", "חצות"),
            param("he", "1 אחר חצות"),
            param("he", "3 לפנות בוקר"),
            param("he", "3 בבוקר"),
            param("he", "3 בצהריים"),
            param("he", "6 לפנות ערב"),
            param("he", "6 אחרי הצהריים"),
            param("he", "6 אחרי הצהרים"),
            # Bangla
            param("bn", "সেপ্টেম্বর 03 2014"),
            param("bn", "শুক্রবার, 03 সেপ্টেম্বর 2014"),
            # Hindi
            param("hi", "सोमवार 13 जून 1998"),
            param("hi", "मंगल 16 1786 12:18"),
            param("hi", "शनि 11 अप्रैल 2002 03:09"),
            # Swedish
            param("sv", "Sept 03 2014"),
            param("sv", "fredag, 03 september 2014"),
        ]
    )
    def test_search_date_string(self, shortname, datetime_string):
        result = self.exact_language_search.search(
            shortname, datetime_string, settings=Settings()
        )[1][0]
        self.assertEqual(result, datetime_string)

    @parameterized.expand(
        [
            # Arabic
            param(
                "ar",
                "في 29 يوليو 1938 غزت القوات اليابانية الاتحاد"
                " السوفييتي ووقعت أولى المعارك والتي انتصر فيها السوفييت، وعلى الرغم من ذلك رفضت"
                " اليابان الاعتراف بذلك وقررت في 11 مايو 1939 تحريك الحدود المنغولية حتى نهر غول،"
                " حيث وقعت معركة خالخين غول والتي انتصر فيها الجيش الأحمر على جيش كوانتونغ",
                [
                    ("في 29 يوليو 1938", datetime.datetime(1938, 7, 29, 0, 0)),
                    ("في 11 مايو 1939", datetime.datetime(1939, 5, 11, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Belarusian
            param(
                "be",
                "Пасля апублікавання Патсдамскай дэкларацыі 26 ліпеня 1945 года і адмовы Японіі капітуляваць "
                "на яе ўмовах ЗША скінулі атамныя бомбы.",
                [("26 ліпеня 1945 года і", datetime.datetime(1945, 7, 26, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Bulgarian
            param(
                "bg",
                "На 16 юни 1944 г. започват въздушни "
                "бомбардировки срещу Япония, използувайки новозавладените острови като бази.",
                [("На 16 юни 1944 г", datetime.datetime(1944, 6, 16, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Chinese
            param(
                "zh",
                "不過大多數人仍多把第二次世界大戰的爆發定為1939年9月1日德國入侵波蘭開始，這次入侵行動隨即導致英國與法國向德國宣戰。",
                [("1939年9月1", datetime.datetime(1939, 9, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Czech
            param(
                "cs",
                "V roce 1920 byla proto vytvořena Společnost národů, jež měla fungovat jako fórum, "
                "na němž měly národy mírovým způsobem urovnávat svoje spory.",
                [("1920", datetime.datetime(1920, 1, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Danish
            param(
                "da",
                "Krigen i Europa begyndte den 1. september 1939, da Nazi-Tyskland invaderede Polen, "
                "og endte med Nazi-Tysklands betingelsesløse overgivelse den 8. maj 1945.",
                [
                    ("1. september 1939", datetime.datetime(1939, 9, 1, 0, 0)),
                    ("8. maj 1945", datetime.datetime(1945, 5, 8, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Dutch
            param(
                "nl",
                " De meest dramatische uitbreiding van het conflict vond plaats op 22 juni 1941 met de "
                "Duitse aanval op de Sovjet-Unie.",
                [("22 juni 1941", datetime.datetime(1941, 6, 22, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # English
            param(
                "en",
                "I will meet you tomorrow at noon",
                [("tomorrow at noon", datetime.datetime(2000, 1, 2, 12, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            param(
                "en",
                "in a minute",
                [("in a minute", datetime.datetime(2000, 1, 1, 0, 1))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            param(
                "en",
                "last decade",
                [("last decade", datetime.datetime(1990, 1, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            param(
                "en",
                "July 13th.\r\n July 14th",
                [
                    ("July 13th", datetime.datetime(2000, 7, 13, 0, 0)),
                    ("July 14th", datetime.datetime(2000, 7, 14, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            param(
                "en",
                "last updated Aug 06, 2018 05:05 PM CDT",
                [
                    (
                        "Aug 06, 2018 05:05 PM CDT",
                        datetime.datetime(
                            2018,
                            8,
                            6,
                            17,
                            5,
                            tzinfo=StaticTzInfo(
                                "CDT", datetime.timedelta(seconds=-18000)
                            ),
                        ),
                    )
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            param(
                "en",
                "25th march 2015 , i need this report today.",
                [("25th march 2015", datetime.datetime(2015, 3, 25))],
                settings={
                    "PARSERS": [
                        parser
                        for parser in default_parsers
                        if parser != "relative-time"
                    ]
                },
            ),
            param(
                "en",
                "25th march 2015 , i need this report today.",
                [
                    ("25th march 2015", datetime.datetime(2015, 3, 25)),
                    ("today", datetime.datetime(2000, 1, 1)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            param(
                "en",
                "The employee has not submitted their documents till date",
                [("till date", datetime.datetime(2000, 1, 1))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Filipino / Tagalog
            param(
                "tl",
                "Maraming namatay sa mga Hapon hanggang sila'y sumuko noong Agosto 15, 1945.",
                [("noong Agosto 15, 1945", datetime.datetime(1945, 8, 15, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Finnish
            param(
                "fi",
                "Iso-Britannia ja Ranska julistivat sodan Saksalle 3. syyskuuta 1939.",
                [("3. syyskuuta 1939", datetime.datetime(1939, 9, 3, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # French
            param(
                "fr",
                "La 2e Guerre mondiale, ou Deuxième Guerre mondiale4, est un conflit armé à "
                "l'échelle planétaire qui dura du 1 septembre 1939 au 2 septembre 1945.",
                [
                    ("1 septembre 1939", datetime.datetime(1939, 9, 1, 0, 0)),
                    ("2 septembre 1945", datetime.datetime(1945, 9, 2, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Hebrew
            param(
                "he",
                'במרץ 1938 "אוחדה" אוסטריה עם גרמניה (אנשלוס). ',
                [("במרץ 1938", datetime.datetime(1938, 3, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Hindi
            param(
                "hi",
                "जुलाई 1937 में, मार्को-पोलो ब्रिज हादसे का बहाना लेकर जापान ने चीन पर हमला कर दिया और चीनी साम्राज्य "
                "की राजधानी बीजिंग पर कब्जा कर लिया,",
                [("जुलाई 1937 में", datetime.datetime(1937, 7, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Hungarian
            param(
                "hu",
                "A háború Európában 1945. május 8-án Németország feltétel nélküli megadásával, "
                "míg Ázsiában szeptember 2-án, Japán kapitulációjával fejeződött be.",
                [
                    ("1945. május 8-án", datetime.datetime(1945, 5, 8, 0, 0)),
                    ("szeptember 2-án", datetime.datetime(2000, 9, 2, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Georgian
            param(
                "ka",
                "1937 წელს დაიწყო იაპონია-ჩინეთის მეორე ომი.",
                [("1937", datetime.datetime(1937, 1, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # German
            param(
                "de",
                "Die UdSSR blieb gemäß dem Neutralitätspakt "
                "vom 13. April 1941 gegenüber Japan vorerst neutral.",
                [
                    ("Die", datetime.datetime(1999, 12, 28, 0, 0)),
                    ("13. April 1941", datetime.datetime(1941, 4, 13, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Indonesian
            param(
                "id",
                "Kekaisaran Jepang menyerah pada tanggal 15 Agustus 1945, sehingga mengakhiri perang "
                "di Asia dan memperkuat kemenangan total Sekutu atas Poros.",
                [("tanggal 15 Agustus 1945", datetime.datetime(1945, 8, 15, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Italian
            param(
                "it",
                " Con questo il 2 ottobre 1935 prese il via la campagna "
                "d'Etiopia. Il 9 maggio 1936 venne proclamato l'Impero. ",
                [
                    ("2 ottobre 1935", datetime.datetime(1935, 10, 2, 0, 0)),
                    ("9 maggio 1936", datetime.datetime(1936, 5, 9, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Japanese
            param(
                "ja",
                "1939年9月1日、ドイツ軍がポーランドへ侵攻したことが第二次世界大戦の始まりとされている。",
                [("1939年9月1", datetime.datetime(1939, 9, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Persian
            param(
                "fa",
                "نگ جهانی دوم جنگ جدی بین سپتامبر 1939 و 2 سپتامبر 1945 بود.",
                [
                    ("سپتامبر 1939", datetime.datetime(1939, 9, 1, 0, 0)),
                    ("2 سپتامبر 1945", datetime.datetime(1945, 9, 2, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Polish
            param(
                "pl",
                "II wojna światowa – największa wojna światowa w historii, "
                "trwająca od 1 września 1939 do 2 września 1945 (w Europie do 8 maja 1945)",
                [
                    ("1 września 1939", datetime.datetime(1939, 9, 1, 0, 0)),
                    ("2 września 1945 (w", datetime.datetime(1945, 9, 2, 0, 0)),
                    ("8 maja 1945", datetime.datetime(1945, 5, 8, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Portuguese
            param(
                "pt",
                "Em outubro de 1936, Alemanha e Itália formaram o Eixo Roma-Berlim.",
                [("Em outubro de 1936", datetime.datetime(1936, 10, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Romanian
            param(
                "ro",
                "Pe 17 septembrie 1939, după semnarea unui acord de încetare a focului cu Japonia, "
                "sovieticii au invadat Polonia dinspre est.",
                [("17 septembrie 1939", datetime.datetime(1939, 9, 17, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Russian
            param(
                "ru",
                "Втора́я мирова́я война́ (1 сентября 1939 — 2 сентября 1945) — "
                "война двух мировых военно-политических коалиций, ставшая крупнейшим вооружённым "
                "конфликтом в истории человечества.",
                [
                    ("1 сентября 1939", datetime.datetime(1939, 9, 1, 0, 0)),
                    ("2 сентября 1945", datetime.datetime(1945, 9, 2, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Spanish
            param(
                "es",
                "Desde finales de 1939 hasta inicios de 1941 Alemania conquistó o sometió "
                "gran parte de la Europa continental.",
                [
                    ("de 1939", datetime.datetime(1939, 1, 1, 0, 0)),
                    ("de 1941", datetime.datetime(1941, 1, 1, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            param(
                "es",
                "¡¡Ay!! En Madrid, a 17 de marzo de 1615. ¿Vos bueno?",
                [("a 17 de marzo de 1615", datetime.datetime(1615, 3, 17, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Swedish
            param(
                "sv",
                "Efter kommunisternas seger 1922 drog de allierade och Japan bort sina trupper.",
                [("1922", datetime.datetime(1922, 1, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Thai
            param(
                "th",
                "และเมื่อวันที่ 11 พฤษภาคม 1939 "
                "ญี่ปุ่นตัดสินใจขยายพรมแดนญี่ปุ่น-มองโกเลียขึ้นไปถึงแม่น้ำคัลคินกอลด้วยกำลัง",
                [("11 พฤษภาคม 1939", datetime.datetime(1939, 5, 11, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Turkish
            param(
                "tr",
                "Almanya’nın Polonya’yı işgal ettiği 1 Eylül 1939 savaşın başladığı "
                "tarih olarak genel kabul görür.",
                [("1 Eylül 1939", datetime.datetime(1939, 9, 1, 0, 0))],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Ukrainian
            param(
                "uk",
                "Інші дати, що розглядаються деякими авторами як дати початку війни: початок японської "
                "інтервенції в Маньчжурію 13 вересня 1931, початок другої японсько-китайської війни 7 "
                "липня 1937 року та початок угорсько-української війни 14 березня 1939 року.",
                [
                    ("13 вересня 1931", datetime.datetime(1931, 9, 13, 0, 0)),
                    ("7 липня 1937 року", datetime.datetime(1937, 7, 7, 0, 0)),
                    ("14 березня 1939 року", datetime.datetime(1939, 3, 14, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
            # Vietnamese
            param(
                "vi",
                "Ý theo gương Đức, đã tiến hành xâm lược Ethiopia năm 1935 và sát "
                "nhập Albania vào ngày 12 tháng 4 năm 1939.",
                [
                    ("năm 1935", datetime.datetime(1935, 1, 1, 0, 0)),
                    ("ngày 12 tháng 4 năm 1939", datetime.datetime(1939, 4, 12, 0, 0)),
                ],
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
            ),
        ]
    )
    @apply_settings
    def test_search_and_parse(self, shortname, string, expected, settings=None):
        result = self.exact_language_search.search_parse(
            shortname, string, settings=settings
        )
        self.assertEqual(result, expected)

    @parameterized.expand(
        [
            # English
            param(
                "en",
                "January 3, 2017 - February 1st",
                [
                    ("January 3, 2017", datetime.datetime(2017, 1, 3, 0, 0)),
                    ("February 1st", datetime.datetime(2017, 2, 1, 0, 0)),
                ],
            ),
            param(
                "en",
                "2014 was good! October was excellent! Friday, 21 was especially good!",
                [
                    ("2014", datetime.datetime(2014, today.month, today.day, 0, 0)),
                    ("October", datetime.datetime(2014, 10, today.day, 0, 0)),
                    ("Friday, 21", datetime.datetime(2014, 10, 21, 0, 0)),
                ],
            ),
            param(
                "en",
                """May 2020
                    July 2020
                    2023
                    January UTC
                    June 5 am utc
                    June 23th 5 pm EST
                    May 31, 8am UTC""",
                [
                    (
                        "May 2020",
                        datetime.datetime(
                            2020,
                            5,
                            datetime.datetime.now(tz=datetime.timezone.utc).day,
                            0,
                            0,
                        ),
                    ),
                    (
                        "July 2020",
                        datetime.datetime(
                            2020,
                            7,
                            datetime.datetime.now(tz=datetime.timezone.utc).day,
                            0,
                            0,
                        ),
                    ),
                    (
                        "2023",
                        datetime.datetime(
                            2023,
                            7,
                            datetime.datetime.now(tz=datetime.timezone.utc).day,
                            0,
                            0,
                        ),
                    ),
                    (
                        "January UTC",
                        datetime.datetime(
                            2023,
                            1,
                            datetime.datetime.now(tz=datetime.timezone.utc).day,
                            0,
                            0,
                            tzinfo=pytz.utc,
                        ),
                    ),
                    (
                        "June 5 am utc",
                        datetime.datetime(2023, 6, 5, 0, 0, tzinfo=pytz.utc),
                    ),
                    (
                        "June 23th 5 pm EST",
                        datetime.datetime(
                            2023,
                            6,
                            23,
                            17,
                            0,
                            tzinfo=datetime.timezone(
                                datetime.timedelta(hours=-5), name="EST"
                            ),
                        ),
                    ),
                    ("May 31", datetime.datetime(2023, 5, 31, 0, 0)),
                    ("8am UTC", datetime.datetime(2023, 8, 31, 0, 0, tzinfo=pytz.utc)),
                ],
            ),
            # Russian
            param(
                "ru",
                "19 марта 2001 был хороший день. 20 марта тоже был хороший день. 21 марта был отличный день.",
                [
                    ("19 марта 2001", datetime.datetime(2001, 3, 19, 0, 0)),
                    ("20 марта", datetime.datetime(2001, 3, 20, 0, 0)),
                    ("21 марта", datetime.datetime(2001, 3, 21, 0, 0)),
                ],
            ),
            param(
                "ru",
                "Андрей Дмитриевич Сахаров скончался 14 декабря в 1989 году от внезапной остановки сердца.",
                [("14 декабря в 1989 году", datetime.datetime(1989, 12, 14, 0, 0))],
            ),
            # relative dates
            param(
                "ru",
                "19 марта 2001. Сегодня был хороший день. 2 дня назад был хороший день. "
                "Вчера тоже был хороший день.",
                [
                    ("19 марта 2001", datetime.datetime(2001, 3, 19, 0, 0)),
                    ("Сегодня", datetime.datetime(2001, 3, 19, 0, 0)),
                    ("2 дня назад", datetime.datetime(2001, 3, 17, 0, 0)),
                    ("Вчера", datetime.datetime(2001, 3, 18, 0, 0)),
                ],
            ),
            param(
                "ru",
                "19 марта 2001. Сегодня был хороший день. Два дня назад был хороший день. Хорошая была неделя. "
                "Думаю, через неделю будет еще лучше.",
                [
                    ("19 марта 2001", datetime.datetime(2001, 3, 19, 0, 0)),
                    ("Сегодня", datetime.datetime(2001, 3, 19, 0, 0)),
                    ("Два дня назад", datetime.datetime(2001, 3, 17, 0, 0)),
                    ("через неделю", datetime.datetime(2001, 3, 26, 0, 0)),
                ],
            ),
            # Hungarian
            param(
                "hu",
                "1962 augusztus 11 Föld körüli pályára bocsátották a szovjet Vosztok-3 űrhajót, "
                "mely páros űrrepülést hajtott végre a másnap föld körüli pályára bocsátott Vosztok-4-gyel."
                "2 hónappal ezelőtt furcsa, nem forgó jellegű szédülést tapasztaltam.",
                [
                    ("1962 augusztus 11", datetime.datetime(1962, 8, 11, 0, 0)),
                    ("2 hónappal ezelőtt", datetime.datetime(1962, 6, 11, 0, 0)),
                ],
            ),
            # Vietnamese
            param(
                "vi",
                "1/1/1940. Vào tháng 8 năm 1940, với lực lượng lớn của Pháp tại Bắc Phi chính thức trung lập "
                "trong cuộc chiến, Ý mở một cuộc tấn công vào thuộc địa Somalia của Anh tại Đông Phi. "
                "Đến tháng 9 quân Ý vào đến Ai Cập (cũng đang dưới sự kiểm soát của Anh). ",
                [
                    ("1/1/1940", datetime.datetime(1940, 1, 1, 0, 0)),
                    ("tháng 8 năm 1940", datetime.datetime(1940, 8, 1, 0, 0)),
                    ("tháng 9", datetime.datetime(1940, 9, 1, 0, 0)),
                ],
            ),
        ]
    )
    @apply_settings
    def test_relative_base_setting(self, shortname, string, expected, settings=None):
        result = self.exact_language_search.search_parse(
            shortname, string, settings=settings
        )
        self.assertEqual(result, expected)

    @parameterized.expand(
        [
            # English
            param(
                "en",
                "July 12th, 2014. July 13th, July 14th",
                [
                    ("July 12th, 2014", datetime.datetime(2014, 7, 12, 0, 0)),
                    ("July 13th", datetime.datetime(2014, 7, 13, 0, 0)),
                    ("July 14th", datetime.datetime(2014, 7, 14, 0, 0)),
                ],
            ),
            param(
                "en",
                "2014. July 13th July 14th",
                [
                    ("2014", datetime.datetime(2014, today.month, today.day, 0, 0)),
                    ("July 13th", datetime.datetime(2014, 7, 13, 0, 0)),
                    ("July 14th", datetime.datetime(2014, 7, 14, 0, 0)),
                ],
            ),
            param(
                "en",
                "July 13th 2014 July 14th 2014",
                [
                    ("July 13th 2014", datetime.datetime(2014, 7, 13, 0, 0)),
                    ("July 14th 2014", datetime.datetime(2014, 7, 14, 0, 0)),
                ],
            ),
            param(
                "en",
                "July 13th 2014 July 14th",
                [
                    ("July 13th 2014", datetime.datetime(2014, 7, 13, 0, 0)),
                    ("July 14th", datetime.datetime(2014, 7, 14, 0, 0)),
                ],
            ),
            param(
                "en",
                "July 13th, 2014 July 14th, 2014",
                [
                    ("July 13th, 2014", datetime.datetime(2014, 7, 13, 0, 0)),
                    ("July 14th, 2014", datetime.datetime(2014, 7, 14, 0, 0)),
                ],
            ),
            param(
                "en",
                "2014. July 12th, July 13th, July 14th",
                [
                    ("2014", datetime.datetime(2014, today.month, today.day, 0, 0)),
                    ("July 12th", datetime.datetime(2014, 7, 12, 0, 0)),
                    ("July 13th", datetime.datetime(2014, 7, 13, 0, 0)),
                    ("July 14th", datetime.datetime(2014, 7, 14, 0, 0)),
                ],
            ),
            # Swedish
            param(
                "sv",
                "1938–1939 marscherade tyska soldater i Österrike samtidigt som "
                "österrikiska soldater marscherade i Berlin.",
                [
                    ("1938", datetime.datetime(1938, today.month, today.day, 0, 0)),
                    ("1939", datetime.datetime(1939, today.month, today.day, 0, 0)),
                ],
            ),
            # German
            param(
                "de",
                "Verteidiger der Stadt kapitulierten am 2. Mai 1945. Am 8. Mai 1945 (VE-Day) trat "
                "bedingungslose Kapitulation der Wehrmacht in Kraft",
                [
                    ("am 2. Mai 1945", datetime.datetime(1945, 5, 2, 0, 0)),
                    ("Am 8. Mai 1945", datetime.datetime(1945, 5, 8, 0, 0)),
                ],
            ),
        ]
    )
    @apply_settings
    def test_splitting_of_not_parsed(self, shortname, string, expected, settings=None):
        result = self.exact_language_search.search_parse(
            shortname, string, settings=settings
        )
        self.assertEqual(result, expected)

    @parameterized.expand(
        [
            # Arabic
            param(
                "ar",
                "في 29 يوليو 1938 غزت القوات اليابانية الاتحاد"
                " السوفييتي ووقعت أولى المعارك والتي انتصر فيها السوفييت، وعلى الرغم من ذلك رفضت"
                " اليابان الاعتراف بذلك وقررت في 11 مايو 1939 تحريك الحدود المنغولية حتى نهر غول،",
            ),
            # Belarusian
            param(
                "be",
                "Пасля апублікавання Патсдамскай дэкларацыі 26 ліпеня 1945 года і адмовы Японіі капітуляваць "
                "на яе ўмовах ЗША скінулі атамныя бомбы.",
            ),
            # Bulgarian
            param(
                "bg",
                "На 16 юни 1944 г. започват въздушни "
                "бомбардировки срещу Япония, използувайки новозавладените острови като бази.",
            ),
            # Chinese
            param(
                "zh",
                "不過大多數人仍多把第二次世界大戰的爆發定為1939年9月1日德國入侵波蘭開始，2015年04月08日10点05。",
            ),
            # Czech
            param(
                "cs",
                "V rok 1920 byla proto vytvořena Společnost národů, jež měla fungovat jako fórum, "
                "na němž měly národy mírovým způsobem urovnávat svoje spory.",
            ),
            # Danish
            param(
                "da",
                "Krigen i Europa begyndte den 1. september 1939, da Nazi-Tyskland invaderede Polen, "
                "og endte med Nazi-Tysklands betingelsesløse overgivelse den 8. marts 1945.",
            ),
            # Dutch
            param(
                "nl",
                " De meest dramatische uitbreiding van het conflict vond plaats op Maandag 22 juni 1941  met de "
                "Duitse aanval op de Sovjet-Unie.",
            ),
            # English
            param("en", "I will meet you tomorrow at noon"),
            # Filipino / Tagalog
            param(
                "tl",
                "Maraming namatay sa mga Hapon hanggang sila'y sumuko noong Agosto 15, 1945.",
            ),
            # Finnish
            param(
                "fi",
                "Iso-Britannia ja Ranska julistivat sodan Saksalle 3. syyskuuta 1939.",
            ),
            # French
            param(
                "fr",
                "La Seconde Guerre mondiale, ou Deuxième Guerre mondiale4, est un conflit armé à "
                "l'échelle planétaire qui dura du 1 septembre 1939 au 2 septembre 1945.",
            ),
            # Hebrew
            param("he", 'במרץ 1938 "אוחדה" אוסטריה עם גרמניה (אנשלוס). '),
            # Hindi
            param(
                "hi",
                "जुलाई 1937 में, मार्को-पोलो ब्रिज हादसे का बहाना लेकर जापान ने चीन पर हमला कर दिया और चीनी साम्राज्य "
                "की राजधानी बीजिंग पर कब्जा कर लिया,",
            ),
            # Hungarian
            param(
                "hu",
                "A háború Európában 1945. május 8-án Németország feltétel nélküli megadásával, "
                "míg Ázsiában szeptember 2-án, Japán kapitulációjával fejeződött be.",
            ),
            # Georgian
            param("ka", "1937 წელს დაიწყო იაპონია-ჩინეთის მეორე ომი."),
            # German
            param(
                "de",
                "Die UdSSR blieb dem Neutralitätspakt "
                "vom 13. April 1941 gegenüber Japan vorerst neutral.",
            ),
            # Indonesian
            param(
                "id",
                "Kekaisaran Jepang menyerah pada tanggal 15 Agustus 1945, sehingga mengakhiri perang "
                "di Asia dan memperkuat kemenangan total Sekutu atas Poros.",
            ),
            # Italian
            param(
                "it",
                " Con questo il 2 ottobre 1935 prese il via la campagna "
                "d'Etiopia. Il 9 maggio 1936 venne proclamato l'Impero. ",
            ),
            # Japanese
            param(
                "ja",
                "1933年（昭和8年）12月23日午前6時39分、宮城（現：皇居）内の産殿にて誕生。",
            ),
            # Persian
            param("fa", "نگ جهانی دوم جنگ جدی بین سپتامبر 1939 و 2 سپتامبر 1945 بود."),
            # Polish
            param(
                "pl",
                "II wojna światowa – największa wojna światowa w historii, "
                "trwająca od 1 września 1939 do 2 września 1945 (w Europie do 8 maja 1945)",
            ),
            # Portuguese
            param(
                "pt",
                "Em outubro de 1936, Alemanha e Itália formaram o Eixo Roma-Berlim.",
            ),
            # Romanian
            param(
                "ro",
                "Pe 17 septembrie 1939, după semnarea unui acord de încetare a focului cu Japonia, "
                "sovieticii au invadat Polonia dinspre est.",
            ),
            # Russian
            param(
                "ru",
                "Втора́я мирова́я война́ (1 сентября 1939 — 2 сентября 1945) — "
                "война двух мировых военно-политических коалиций, ставшая крупнейшим вооружённым "
                "конфликтом в истории человечества.",
            ),
            # Spanish
            param("es", "11 junio 2010"),
            param("es", "¡¡Ay!! En Madrid, a 17 de marzo de 1615. ¿Vos bueno?"),
            # Swedish
            param("sv", " den 15 augusti 1945 då Kejsardömet"),
            # Thai
            param(
                "th",
                "และเมื่อวันที่ 11 พฤษภาคม 1939 "
                "ญี่ปุ่นตัดสินใจขยายพรมแดนญี่ปุ่น-มองโกเลียขึ้นไปถึงแม่น้ำคัลคินกอลด้วยกำลัง",
            ),
            # Turkish
            param(
                "tr",
                "Almanya’nın Polonya’yı işgal ettiği 1 Eylül 1939 savaşın başladığı "
                "tarih olarak genel kabul görür.",
            ),
            # Ukrainian
            param(
                "uk",
                "Інші дати, що розглядаються деякими авторами як дати початку війни: початок японської "
                "інтервенції в Маньчжурію 13 вересня 1931, початок другої японсько-китайської війни 7 "
                "липня 1937 року та початок угорсько-української війни 14 березня 1939 року.",
            ),
            # Vietnamese
            param(
                "vi",
                "Ý theo gương Đức, đã tiến hành xâm lược Ethiopia năm 1935 và sát "
                "nhập Albania vào ngày 12 tháng 4 năm 1939.",
            ),
            # Only digits
            param("en", "2007"),
        ]
    )
    def test_detection(self, shortname, text):
        result = self.search_with_detection.detect_language(text, languages=None)
        self.assertEqual(result, shortname)

    @parameterized.expand(
        [
            param(
                text="19 марта 2001 был хороший день. 20 марта тоже был хороший день. 21 марта был отличный день.",
                languages=["en", "ru"],
                settings=None,
                expected=[
                    ("19 марта 2001", datetime.datetime(2001, 3, 19, 0, 0)),
                    ("20 марта", datetime.datetime(2001, 3, 20, 0, 0)),
                    ("21 марта", datetime.datetime(2001, 3, 21, 0, 0)),
                ],
            ),
            param(
                text="Em outubro de 1936, Alemanha e Itália formaram o Eixo Roma-Berlim.",
                languages=None,
                settings={"RELATIVE_BASE": datetime.datetime(2000, 1, 1)},
                expected=[("Em outubro de 1936", datetime.datetime(1936, 10, 1, 0, 0))],
            ),
            param(
                text="19 марта 2001, 20 марта, 21 марта был отличный день.",
                languages=["en", "ru"],
                settings=None,
                expected=[
                    ("19 марта 2001", datetime.datetime(2001, 3, 19, 0, 0)),
                    ("20 марта", datetime.datetime(2001, 3, 20, 0, 0)),
                    ("21 марта", datetime.datetime(2001, 3, 21, 0, 0)),
                ],
            ),
            # Dates not found
            param(text="", languages=None, settings=None, expected=None),
            # Language not detected
            param(text="Привет", languages=["en"], settings=None, expected=None),
            # ZeroDivisionError
            param(
                text="DECEMBER 21 19.87 87",
                languages=None,
                settings=None,
                expected=[("DECEMBER 21 19", datetime.datetime(2019, 12, 21, 0, 0))],
            ),
            param(
                text="bonjour, pouvez vous me joindre svp par telephone 08 11 58 54 41",
                languages=None,
                settings={"STRICT_PARSING": True},
                expected=None,
            ),
            param(text="a Americ", languages=None, settings=None, expected=None),
            # Date with comma and apostrophe
            param(
                text="9/3/2017  , ",
                languages=["en"],
                settings=None,
                expected=[("9/3/2017", datetime.datetime(2017, 9, 3, 0, 0))],
            ),
            param(
                text="9/3/2017  ' ",
                languages=["en"],
                settings=None,
                expected=[("9/3/2017", datetime.datetime(2017, 9, 3, 0, 0))],
            ),
        ]
    )
    def test_date_search_function(self, text, languages, settings, expected):
        result = search_dates(text, languages=languages, settings=settings)
        self.assertEqual(result, expected)

    @parameterized.expand(
        [
            param(
                text="15 de outubro de 1936",
                add_detected_language=True,
                expected=[
                    (
                        "15 de outubro de 1936",
                        datetime.datetime(1936, 10, 15, 0, 0),
                        "pt",
                    )
                ],
            ),
            param(
                text="15 de outubro de 1936",
                add_detected_language=False,
                expected=[
                    ("15 de outubro de 1936", datetime.datetime(1936, 10, 15, 0, 0))
                ],
            ),
        ]
    )
    def test_search_dates_returning_detected_languages_if_requested(
        self, text, add_detected_language, expected
    ):
        result = search_dates(text, add_detected_language=add_detected_language)
        self.assertEqual(result, expected)

    @parameterized.expand(
        [
            param(text="19 марта 2001", languages="wrong type: str instead of list"),
        ]
    )
    def test_date_search_function_invalid_languages_type(self, text, languages):
        self.run_search_dates_function_invalid_languages(
            text=text, languages=languages, error_type=TypeError
        )
        self.check_error_message(
            "languages argument must be a list (<class 'str'> given)"
        )

    @parameterized.expand(
        [
            param(text="19 марта 2001", languages=["unknown language code"]),
        ]
    )
    def test_date_search_function_invalid_language_code(self, text, languages):
        self.run_search_dates_function_invalid_languages(
            text=text, languages=languages, error_type=ValueError
        )
        self.check_error_message("Unknown language(s): 'unknown language code'")

    def test_search_dates_with_prepositions(self):
        """Test `search_dates` for parsing Russian date ranges with prepositions and language detection."""
        result = search_dates(
            "Сервис будет недоступен с 12 января по 30 апреля.",
            add_detected_language=True,
            languages=["ru"],
        )
        expected = [
            ("12 января", datetime.datetime(today.year, 1, 12, 0, 0), "ru"),
            ("30 апреля", datetime.datetime(today.year, 4, 30, 0, 0), "ru"),
        ]
        assert result == expected

    @parameterized.expand(
        [
            param(
                text="Ужасное событие произошло в тот день. Двадцатое февраля. Вспоминаю тот день с ужасом.",
                expected_text="Двадцатое февраля",
                expected_day=20,
                expected_month=2,
                description="20th February",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать первое февраля. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать первое февраля",
                expected_day=21,
                expected_month=2,
                description="21st February",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать второе февраля. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать второе февраля",
                expected_day=22,
                expected_month=2,
                description="22nd February",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать третье февраля. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать третье февраля",
                expected_day=23,
                expected_month=2,
                description="23rd February",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать четвёртое февраля. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать четвёртое февраля",
                expected_day=24,
                expected_month=2,
                description="24th February (with ё)",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать четвертое февраля. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать четвертое февраля",
                expected_day=24,
                expected_month=2,
                description="24th February (without ё)",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать пятое марта. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать пятое марта",
                expected_day=25,
                expected_month=3,
                description="25th March",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать шестое марта. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать шестое марта",
                expected_day=26,
                expected_month=3,
                description="26th March",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать седьмое марта. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать седьмое марта",
                expected_day=27,
                expected_month=3,
                description="27th March",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать восьмое марта. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать восьмое марта",
                expected_day=28,
                expected_month=3,
                description="28th March",
            ),
            param(
                text="Ужасное событие произошло в тот день. Двадцать девятое марта. Вспоминаю тот день с ужасом.",
                expected_text="Двадцать девятое марта",
                expected_day=29,
                expected_month=3,
                description="29th March",
            ),
            param(
                text="Ужасное событие произошло в тот день. Тридцатое марта. Вспоминаю тот день с ужасом.",
                expected_text="Тридцатое марта",
                expected_day=30,
                expected_month=3,
                description="30th March",
            ),
            param(
                text="Ужасное событие произошло в тот день. Тридцать первое марта. Вспоминаю тот день с ужасом.",
                expected_text="Тридцать первое марта",
                expected_day=31,
                expected_month=3,
                description="31st March",
            ),
        ]
    )
    def test_search_dates_multi_word_expression(
        self, text, expected_text, expected_day, expected_month, description
    ):
        """Test parsing of multi-word date expressions in Russian."""
        result = search_dates(text, languages=["ru"])
        expected = [
            (
                expected_text,
                datetime.datetime(
                    datetime.datetime.now().year, expected_month, expected_day, 0, 0
                ),
            )
        ]
        self.assertEqual(result, expected)

    def test_search_dates_time_span_past_month(self):
        """Test search_dates with 'past month' time span."""
        text = "messages received for the past month"
        settings = {
            "RETURN_TIME_SPAN": True,
            "RELATIVE_BASE": datetime.datetime(2025, 2, 15, 12, 0, 0),
            "DEFAULT_DAYS_IN_MONTH": 30,
        }

        result = search_dates(text, settings=settings)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        if result is not None:
            self.assertGreaterEqual(len(result), 2)

            span_results = [r for r in result if "(start)" in r[0] or "(end)" in r[0]]
            self.assertEqual(len(span_results), 2)

            start_result = next(r for r in span_results if "(start)" in r[0])
            end_result = next(r for r in span_results if "(end)" in r[0])
            self.assertEqual(end_result[1], datetime.datetime(2025, 2, 15, 12, 0, 0))

            expected_start = datetime.datetime(2025, 2, 15, 12, 0, 0) - timedelta(
                days=30
            )
            self.assertEqual(start_result[1], expected_start)

    def test_search_dates_time_span_last_week(self):
        """Test search_dates with 'last week' time span."""
        text = "messages received last week"
        settings = {
            "RETURN_TIME_SPAN": True,
            "RELATIVE_BASE": datetime.datetime(2025, 2, 18, 12, 0, 0),  # Tuesday
            "DEFAULT_START_OF_WEEK": "monday",
        }

        result = search_dates(text, settings=settings)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        if result is not None:
            self.assertGreaterEqual(len(result), 2)

            span_results = [r for r in result if "(start)" in r[0] or "(end)" in r[0]]
            self.assertEqual(len(span_results), 2)

            start_result = next(r for r in span_results if "(start)" in r[0])
            end_result = next(r for r in span_results if "(end)" in r[0])
            expected_start = datetime.datetime(2025, 2, 10, 12, 0, 0)
            expected_end = datetime.datetime(2025, 2, 16, 12, 0, 0)

            self.assertEqual(start_result[1], expected_start)
            self.assertEqual(end_result[1], expected_end)

    def test_search_dates_time_span_custom_start_of_week(self):
        """Test search_dates with custom start_of_week setting."""
        text = "messages received last week"
        settings = {
            "RETURN_TIME_SPAN": True,
            "RELATIVE_BASE": datetime.datetime(2025, 2, 18, 12, 0, 0),  # Tuesday
            "DEFAULT_START_OF_WEEK": "sunday",  # Custom start of week
        }

        result = search_dates(text, settings=settings)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        if result is not None:
            self.assertGreaterEqual(len(result), 2)

            span_results = [r for r in result if "(start)" in r[0] or "(end)" in r[0]]
            self.assertEqual(len(span_results), 2)

            start_result = next(r for r in span_results if "(start)" in r[0])
            end_result = next(r for r in span_results if "(end)" in r[0])
            expected_start = datetime.datetime(2025, 2, 9, 12, 0, 0)
            expected_end = datetime.datetime(2025, 2, 15, 12, 0, 0)

            self.assertEqual(start_result[1], expected_start)
            self.assertEqual(end_result[1], expected_end)

    def test_search_dates_time_span_custom_days_in_month(self):
        """Test search_dates with custom days_in_month setting."""
        text = "messages received for the past month"
        settings = {
            "RETURN_TIME_SPAN": True,
            "RELATIVE_BASE": datetime.datetime(2025, 2, 15, 12, 0, 0),
            "DEFAULT_DAYS_IN_MONTH": 28,  # Custom month length
        }

        result = search_dates(text, settings=settings)

        self.assertIsNotNone(result)
        self.assertIsInstance(result, list)
        if result is not None:
            self.assertGreaterEqual(len(result), 2)

            span_results = [r for r in result if "(start)" in r[0] or "(end)" in r[0]]
            self.assertEqual(len(span_results), 2)

            start_result = next(r for r in span_results if "(start)" in r[0])
            end_result = next(r for r in span_results if "(end)" in r[0])
            self.assertEqual(end_result[1], datetime.datetime(2025, 2, 15, 12, 0, 0))

            expected_start = datetime.datetime(2025, 2, 15, 12, 0, 0) - timedelta(
                days=28
            )
            self.assertEqual(start_result[1], expected_start)

    def test_search_dates_time_span_disabled_by_default(self):
        """Test that time span functionality is disabled by default."""
        text = "messages received for the past month"

        result = search_dates(text)

        if result:
            span_results = [r for r in result if "(start)" in r[0] or "(end)" in r[0]]
            self.assertEqual(len(span_results), 0)
