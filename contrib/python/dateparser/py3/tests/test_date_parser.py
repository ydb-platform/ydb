import unittest
from datetime import datetime, timedelta
from functools import wraps
from unittest.mock import Mock, patch

from parameterized import param, parameterized

import dateparser.timezone_parser
from dateparser import parse
from dateparser.date import DateDataParser, date_parser
from dateparser.date_parser import DateParser
from dateparser.parser import _parse_absolute
from dateparser.timezone_parser import StaticTzInfo
from dateparser.utils import normalize_unicode
from tests import BaseTestCase


class TestDateParser(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.parser = NotImplemented
        self.result = NotImplemented
        self.date_parser = NotImplemented
        self.date_result = NotImplemented

    @parameterized.expand(
        [
            # English dates
            param("[Sept] 04, 2014.", datetime(2014, 9, 4)),
            param("Tuesday Jul 22, 2014", datetime(2014, 7, 22)),
            param("Tues 9th Aug, 2015", datetime(2015, 8, 9)),
            param("10:04am", datetime(2012, 11, 13, 10, 4)),
            param("Friday", datetime(2012, 11, 9)),
            param("November 19, 2014 at noon", datetime(2014, 11, 19, 12, 0)),
            param("December 13, 2014 at midnight", datetime(2014, 12, 13, 0, 0)),
            param("Nov 25 2014 10:17 pm", datetime(2014, 11, 25, 22, 17)),
            param("Wed Aug 05 12:00:00 2015", datetime(2015, 8, 5, 12, 0)),
            param("April 9, 2013 at 6:11 a.m.", datetime(2013, 4, 9, 6, 11)),
            param("Aug. 9, 2012 at 2:57 p.m.", datetime(2012, 8, 9, 14, 57)),
            param("December 10, 2014, 11:02:21 pm", datetime(2014, 12, 10, 23, 2, 21)),
            param("8:25 a.m. Dec. 12, 2014", datetime(2014, 12, 12, 8, 25)),
            param("2:21 p.m., December 11, 2014", datetime(2014, 12, 11, 14, 21)),
            param("Fri, 12 Dec 2014 10:55:50", datetime(2014, 12, 12, 10, 55, 50)),
            param("20 Mar 2013 10h11", datetime(2013, 3, 20, 10, 11)),
            param("10:06am Dec 11, 2014", datetime(2014, 12, 11, 10, 6)),
            param("19 February 2013 year 09:10", datetime(2013, 2, 19, 9, 10)),
            param(
                "21 January 2012 13:11:23.678",
                datetime(2012, 1, 21, 13, 11, 23, 678000),
            ),
            param("1/1/16 9:02:43.1", datetime(2016, 1, 1, 9, 2, 43, 100000)),
            param("29.02.2020 13.12", datetime(2020, 2, 29, 13, 12)),
            param("26. 10.21", datetime(2021, 10, 26, 0, 0)),
            param("26. 10.21 14.12", datetime(2021, 10, 26, 14, 12)),
            param("26 . 10.21", datetime(2021, 10, 26, 0, 0)),
            param("30 . 09 . 22 12.12", datetime(2022, 9, 30, 12, 12)),
            param("1 a.m 20.07.2021", datetime(2021, 7, 20, 1, 0)),
            param(
                "Wednesday, 22nd June, 2016, 12.16 pm.", datetime(2016, 6, 22, 12, 16)
            ),
            # French dates
            param("11 Mai 2014", datetime(2014, 5, 11)),
            param("11 sept. 2014", datetime(2014, 9, 11)),
            param("dimanche, 11 Mai 2014", datetime(2014, 5, 11)),
            param("22 janvier 2015 à 14h40", datetime(2015, 1, 22, 14, 40)),
            param("Dimanche 1er Février à 21:24", datetime(2012, 2, 1, 21, 24)),
            param("vendredi, décembre 5 2014.", datetime(2014, 12, 5, 0, 0)),
            param("le 08 Déc 2014 15:11", datetime(2014, 12, 8, 15, 11)),
            param("Le 11 Décembre 2014 à 09:00", datetime(2014, 12, 11, 9, 0)),
            param("fév 15, 2013", datetime(2013, 2, 15, 0, 0)),
            param("Jeu 15:12", datetime(2012, 11, 8, 15, 12)),
            # Spanish dates
            param("Martes 21 de Octubre de 2014", datetime(2014, 10, 21)),
            param("Miércoles 20 de Noviembre de 2013", datetime(2013, 11, 20)),
            param("12 de junio del 2012", datetime(2012, 6, 12)),
            param("13 Ago, 2014", datetime(2014, 8, 13)),
            param("13 Septiembre, 2014", datetime(2014, 9, 13)),
            param("11 Marzo, 2014", datetime(2014, 3, 11)),
            param("julio 5, 2015 en 1:04 pm", datetime(2015, 7, 5, 13, 4)),
            param("Vi 17:15", datetime(2012, 11, 9, 17, 15)),
            # Dutch dates
            param("11 augustus 2014", datetime(2014, 8, 11)),
            param("14 januari 2014", datetime(2014, 1, 14)),
            param("vr jan 24, 2014 12:49", datetime(2014, 1, 24, 12, 49)),
            # Italian dates
            param("16 giu 2014", datetime(2014, 6, 16)),
            param("26 gennaio 2014", datetime(2014, 1, 26)),
            param("Ven 18:23", datetime(2012, 11, 9, 18, 23)),
            # Portuguese dates
            param(
                "sexta-feira, 10 de junho de 2014 14:52", datetime(2014, 6, 10, 14, 52)
            ),
            param("13 Setembro, 2014", datetime(2014, 9, 13)),
            param("Sab 3:03", datetime(2012, 11, 10, 3, 3)),
            # Russian dates
            param("10 мая", datetime(2012, 5, 10)),  # forum.codenet.ru
            param("26 апреля", datetime(2012, 4, 26)),
            param("20 ноября 2013", datetime(2013, 11, 20)),
            param("28 октября 2014 в 07:54", datetime(2014, 10, 28, 7, 54)),
            param("13 января 2015 г. в 13:34", datetime(2015, 1, 13, 13, 34)),
            param("09 августа 2012", datetime(2012, 8, 9, 0, 0)),
            param("Авг 26, 2015 15:12", datetime(2015, 8, 26, 15, 12)),
            param("2 Декабрь 95 11:15", datetime(1995, 12, 2, 11, 15)),
            param("13 янв. 2005 19:13", datetime(2005, 1, 13, 19, 13)),
            param("13 авг. 2005 19:13", datetime(2005, 8, 13, 19, 13)),
            param("13 авг. 2005г. 19:13", datetime(2005, 8, 13, 19, 13)),
            param("13 авг. 2005 г. 19:13", datetime(2005, 8, 13, 19, 13)),
            param("21 сентября 2021г., вторник", datetime(2021, 9, 21, 0, 0)),
            param("Пнд, 07 янв. 2019 г. 12:15", datetime(2019, 1, 7, 12, 15)),
            param("Срд, 09 янв. 2019 г. 12:15", datetime(2019, 1, 9, 12, 15)),
            param("чтв, 1 сентября 2022 г. 09:00", datetime(2022, 9, 1, 9, 00)),
            param("Птн, 11 янв. 2019 г. 12:15", datetime(2019, 1, 11, 12, 15)),
            param("сбт, 1 окт. 2022 г. 10:22", datetime(2022, 10, 1, 10, 22)),
            param("вск, 2 окт. 2022 г. 11:17", datetime(2022, 10, 2, 11, 17)),
            # Turkish dates
            param("11 Ağustos, 2014", datetime(2014, 8, 11)),
            param(
                "08.Haziran.2014, 11:07", datetime(2014, 6, 8, 11, 7)
            ),  # forum.andronova.net
            param("17.Şubat.2014, 17:51", datetime(2014, 2, 17, 17, 51)),
            param(
                "14-Aralık-2012, 20:56", datetime(2012, 12, 14, 20, 56)
            ),  # forum.ceviz.net
            # Romanian dates
            param("13 iunie 2013", datetime(2013, 6, 13)),
            param("14 aprilie 2014", datetime(2014, 4, 14)),
            param("18 martie 2012", datetime(2012, 3, 18)),
            param("12-Iun-2013", datetime(2013, 6, 12)),
            # German dates
            param("21. Dezember 2013", datetime(2013, 12, 21)),
            param("19. Februar 2012", datetime(2012, 2, 19)),
            param("26. Juli 2014", datetime(2014, 7, 26)),
            param("1. Sept 2000", datetime(2000, 9, 1)),
            param("18.10.14 um 22:56 Uhr", datetime(2014, 10, 18, 22, 56)),
            param("12-Mär-2014", datetime(2014, 3, 12)),
            param("Mit 13:14", datetime(2012, 11, 7, 13, 14)),
            param("23. März 18.37 Uhr", datetime(2012, 3, 23, 18, 37)),
            # Czech dates
            param("pon 16. čer 2014 10:07:43", datetime(2014, 6, 16, 10, 7, 43)),
            param("13 Srpen, 2014", datetime(2014, 8, 13)),
            param("čtv 14. lis 2013 12:38:43", datetime(2013, 11, 14, 12, 38, 43)),
            # Thai dates
            param("ธันวาคม 11, 2014, 08:55:08 PM", datetime(2014, 12, 11, 20, 55, 8)),
            param("22 พฤษภาคม 2012, 22:12", datetime(2012, 5, 22, 22, 12)),
            param("11 กุมภา 2020, 8:13 AM", datetime(2020, 2, 11, 8, 13)),
            param("1 เดือนตุลาคม 2005, 1:00 AM", datetime(2005, 10, 1, 1, 0)),
            param("11 ก.พ. 2020, 1:13 pm", datetime(2020, 2, 11, 13, 13)),
            # Vietnamese dates
            param("Thứ năm", datetime(2012, 11, 8)),  # Thursday
            param("Thứ sáu", datetime(2012, 11, 9)),  # Friday
            param(
                "Tháng Mười Hai 29, 2013, 14:14", datetime(2013, 12, 29, 14, 14)
            ),  # bpsosrcs.wordpress.com  # NOQA
            param("05 Tháng một 2015 - 03:54 AM", datetime(2015, 1, 5, 3, 54)),
            # Belarusian dates
            param("11 траўня", datetime(2012, 5, 11)),
            param("4 мая", datetime(2012, 5, 4)),
            param("Чацвер 06 жніўня 2015", datetime(2015, 8, 6)),
            param(
                "Нд 14 сакавіка 2015 у 7 гадзін 10 хвілін", datetime(2015, 3, 14, 7, 10)
            ),
            param("5 жніўня 2015 года у 13:34", datetime(2015, 8, 5, 13, 34)),
            # Ukrainian dates
            param("2015-кві-12", datetime(2015, 4, 12)),
            param("2020-берез-11", datetime(2020, 3, 11)),
            param("21 чер 2013 3:13", datetime(2013, 6, 21, 3, 13)),
            param("17 верес 2015 6:17", datetime(2015, 9, 17, 6, 17)),
            param("12 лютого 2012, 13:12:23", datetime(2012, 2, 12, 13, 12, 23)),
            param("10 листоп 2017, 10:00:00", datetime(2017, 11, 10, 10, 00, 00)),
            param("вів о 14:04", datetime(2012, 11, 13, 14, 4)),
            # Tagalog dates
            param("12 Hulyo 2003 13:01", datetime(2003, 7, 12, 13, 1)),
            param("1978, 1 Peb, 7:05 PM", datetime(1978, 2, 1, 19, 5)),
            param("2 hun", datetime(2012, 6, 2)),
            param("Lin 16:16", datetime(2012, 11, 11, 16, 16)),
            # Japanese dates
            param("2016年3月20日(日) 21時40分", datetime(2016, 3, 20, 21, 40)),
            param("2016年3月20日 21時40分", datetime(2016, 3, 20, 21, 40)),
            # Numeric dates
            param("06-17-2014", datetime(2014, 6, 17)),
            param("13/03/2014", datetime(2014, 3, 13)),
            param("11. 12. 2014, 08:45:39", datetime(2014, 11, 12, 8, 45, 39)),
            # Miscellaneous dates
            param("1 Ni 2015", datetime(2015, 4, 1, 0, 0)),
            param("1 Mar 2015", datetime(2015, 3, 1, 0, 0)),
            param("1 сер 2015", datetime(2015, 8, 1, 0, 0)),
            # Chinese dates
            param("2015年04月08日10:05", datetime(2015, 4, 8, 10, 5)),
            param("2012年12月20日10:35", datetime(2012, 12, 20, 10, 35)),
            param("2016年06月30日09时30分", datetime(2016, 6, 30, 9, 30)),
            param("2016年6月2911:30", datetime(2016, 6, 29, 11, 30)),
            param("2016年6月29", datetime(2016, 6, 29, 0, 0)),
            param("2016年 2月 5日", datetime(2016, 2, 5, 0, 0)),
            param("2016年9月14日晚8:00", datetime(2016, 9, 14, 20, 0)),
            # Bulgarian
            param("25 ян 2016", datetime(2016, 1, 25, 0, 0)),
            param("23 декември 2013 15:10:01", datetime(2013, 12, 23, 15, 10, 1)),
            # Bangla dates
            param("[সেপ্টেম্বর] 04, 2014.", datetime(2014, 9, 4)),
            param("মঙ্গলবার জুলাই 22, 2014", datetime(2014, 7, 22)),
            param("শুক্রবার", datetime(2012, 11, 9)),
            param("শুক্র, 12 ডিসেম্বর 2014 10:55:50", datetime(2014, 12, 12, 10, 55, 50)),
            param("1লা জানুয়ারী 2015", datetime(2015, 1, 1)),
            param("25শে মার্চ 1971", datetime(1971, 3, 25)),
            param("8ই মে 2002", datetime(2002, 5, 8)),
            param("10:06am ডিসেম্বর 11, 2014", datetime(2014, 12, 11, 10, 6)),
            param("19 ফেব্রুয়ারী 2013 সাল 09:10", datetime(2013, 2, 19, 9, 10)),
            # Hindi dates
            param("11 जुलाई 1994, 11:12", datetime(1994, 7, 11, 11, 12)),
            param("१७ अक्टूबर २०१८", datetime(2018, 10, 17, 0, 0)),
            param("12 जनवरी  1997 11:08 अपराह्न", datetime(1997, 1, 12, 23, 8)),
            # Georgian dates
            param("2011 წლის 17 მარტი, ოთხშაბათი", datetime(2011, 3, 17, 0, 0)),
            param("2015 წ. 12 ივნ, 15:34", datetime(2015, 6, 12, 15, 34)),
            # Finnish dates
            param("5.7.2018 5.45 ip.", datetime(2018, 7, 5, 17, 45)),
            param("5 .7 .2018 5.45 ip.", datetime(2018, 7, 5, 17, 45)),
            param("28 maalis klo 9:37", datetime(2012, 3, 28, 9, 37)),
            param("28 maalis 9:37", datetime(2012, 3, 28, 9, 37)),
            param("15 tammi klo 14:30", datetime(2012, 1, 15, 14, 30)),
            param("5 kesä klo 18:00", datetime(2012, 6, 5, 18, 0)),
            param("12.5.2020 klo 16:45", datetime(2020, 5, 12, 16, 45)),
            # Croatian dates
            param("06. travnja 2021.", datetime(2021, 4, 6, 0, 0)),
            param("13. svibanj 2022.", datetime(2022, 5, 13, 0, 0)),
            param("24.03.2019. u 22:22", datetime(2019, 3, 24, 22, 22)),
            param("20. studenoga 2010. @ 07:28", datetime(2010, 11, 20, 7, 28)),
            param("13. studenog 1989.", datetime(1989, 11, 13, 0, 0)),
            param("29.01.2008. 00:00", datetime(2008, 1, 29, 0, 0)),
            param("27. 05. 2022. u 14:34", datetime(2022, 5, 27, 14, 34)),
            param("28. u studenom 2017.", datetime(2017, 11, 28, 0, 0)),
            param("13. veljače 1999. u podne", datetime(1999, 2, 13, 12, 0)),
            param("27. siječnja 1994. u ponoć", datetime(1994, 1, 27, 0, 0)),
        ]
    )
    def test_dates_parsing(self, date_string, expected):
        self.given_parser(
            settings={"NORMALIZE": False, "RELATIVE_BASE": datetime(2012, 11, 13)}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("hr", "02/10/2016 u 17:20", datetime(2016, 10, 2, 17, 20)),
        ]
    )
    def test_dates_parsing_with_language(self, language, date_string, expected):
        self.given_parser(
            languages=[language],
            settings={
                "NORMALIZE": False,
                "RELATIVE_BASE": datetime(2012, 11, 13),
            },
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("2016020417:10", datetime(2016, 2, 4, 17, 10)),
        ]
    )
    def test_dates_parsing_no_spaces(self, date_string, expected):
        self.given_parser(
            settings={
                "NORMALIZE": False,
                "RELATIVE_BASE": datetime(2012, 11, 13),
                "PARSERS": ["no-spaces-time"],
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_date_obj_exactly_is(expected)

    def test_stringified_datetime_should_parse_fine(self):
        expected_date = datetime(2012, 11, 13, 10, 15, 5, 330256)
        self.given_parser(settings={"RELATIVE_BASE": expected_date})
        date_string = str(self.parser.get_date_data("today")["date_obj"])
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_date_obj_exactly_is(expected_date)

    @parameterized.expand(
        [
            # English dates
            param("[Sept] 04, 2014.", datetime(2014, 9, 4)),
            param("Tuesday Jul 22, 2014", datetime(2014, 7, 22)),
            param("10:04am", datetime(2012, 11, 13, 10, 4)),
            param("Friday", datetime(2012, 11, 9)),
            param("November 19, 2014 at noon", datetime(2014, 11, 19, 12, 0)),
            param("December 13, 2014 at midnight", datetime(2014, 12, 13, 0, 0)),
            param("Nov 25 2014 10:17 pm", datetime(2014, 11, 25, 22, 17)),
            param("Wed Aug 05 12:00:00 2015", datetime(2015, 8, 5, 12, 0)),
            param("April 9, 2013 at 6:11 a.m.", datetime(2013, 4, 9, 6, 11)),
            param("Aug. 9, 2012 at 2:57 p.m.", datetime(2012, 8, 9, 14, 57)),
            param("December 10, 2014, 11:02:21 pm", datetime(2014, 12, 10, 23, 2, 21)),
            param("8:25 a.m. Dec. 12, 2014", datetime(2014, 12, 12, 8, 25)),
            param("2:21 p.m., December 11, 2014", datetime(2014, 12, 11, 14, 21)),
            param("Fri, 12 Dec 2014 10:55:50", datetime(2014, 12, 12, 10, 55, 50)),
            param("20 Mar 2013 10h11", datetime(2013, 3, 20, 10, 11)),
            param("10:06am Dec 11, 2014", datetime(2014, 12, 11, 10, 6)),
            param("19 February 2013 year 09:10", datetime(2013, 2, 19, 9, 10)),
            # French dates
            param("11 Mai 2014", datetime(2014, 5, 11)),
            param("dimanche, 11 Mai 2014", datetime(2014, 5, 11)),
            param("22 janvier 2015 à 14h40", datetime(2015, 1, 22, 14, 40)),  # wrong
            param("Dimanche 1er Février à 21:24", datetime(2012, 2, 1, 21, 24)),
            param("vendredi, décembre 5 2014.", datetime(2014, 12, 5, 0, 0)),
            param("le 08 Déc 2014 15:11", datetime(2014, 12, 8, 15, 11)),
            param("Le 11 Décembre 2014 à 09:00", datetime(2014, 12, 11, 9, 0)),
            param("fév 15, 2013", datetime(2013, 2, 15, 0, 0)),
            param("Jeu 15:12", datetime(2012, 11, 8, 15, 12)),
            # Spanish dates
            param("Martes 21 de Octubre de 2014", datetime(2014, 10, 21)),
            param("Miércoles 20 de Noviembre de 2013", datetime(2013, 11, 20)),
            param("12 de junio del 2012", datetime(2012, 6, 12)),
            param("13 Ago, 2014", datetime(2014, 8, 13)),
            param("13 Septiembre, 2014", datetime(2014, 9, 13)),
            param("11 Marzo, 2014", datetime(2014, 3, 11)),
            param("julio 5, 2015 en 1:04 pm", datetime(2015, 7, 5, 13, 4)),
            param("Vi 17:15", datetime(2012, 11, 9, 17, 15)),
            # Dutch dates
            param("11 augustus 2014", datetime(2014, 8, 11)),
            param("14 januari 2014", datetime(2014, 1, 14)),
            param("vr jan 24, 2014 12:49", datetime(2014, 1, 24, 12, 49)),
            # Italian dates
            param("16 giu 2014", datetime(2014, 6, 16)),
            param("26 gennaio 2014", datetime(2014, 1, 26)),
            param("Ven 18:23", datetime(2012, 11, 9, 18, 23)),
            # Portuguese dates
            param(
                "sexta-feira, 10 de junho de 2014 14:52", datetime(2014, 6, 10, 14, 52)
            ),
            param("13 Setembro, 2014", datetime(2014, 9, 13)),
            param("Sab 3:03", datetime(2012, 11, 10, 3, 3)),
            # Russian dates
            param("10 мая", datetime(2012, 5, 10)),  # forum.codenet.ru
            param("26 апреля", datetime(2012, 4, 26)),
            param("20 ноября 2013", datetime(2013, 11, 20)),
            param("28 октября 2014 в 07:54", datetime(2014, 10, 28, 7, 54)),
            param("13 января 2015 г. в 13:34", datetime(2015, 1, 13, 13, 34)),
            param("09 августа 2012", datetime(2012, 8, 9, 0, 0)),
            param("Авг 26, 2015 15:12", datetime(2015, 8, 26, 15, 12)),
            param("2 Декабрь 95 11:15", datetime(1995, 12, 2, 11, 15)),
            param("13 янв. 2005 19:13", datetime(2005, 1, 13, 19, 13)),
            param("13 авг. 2005 19:13", datetime(2005, 8, 13, 19, 13)),
            param("13 авг. 2005г. 19:13", datetime(2005, 8, 13, 19, 13)),
            param("13 авг. 2005 г. 19:13", datetime(2005, 8, 13, 19, 13)),
            # Turkish dates
            param("11 Ağustos, 2014", datetime(2014, 8, 11)),
            param(
                "08.Haziran.2014, 11:07", datetime(2014, 6, 8, 11, 7)
            ),  # forum.andronova.net
            param("17.Şubat.2014, 17:51", datetime(2014, 2, 17, 17, 51)),
            param(
                "14-Aralık-2012, 20:56", datetime(2012, 12, 14, 20, 56)
            ),  # forum.ceviz.net
            # Romanian dates
            param("13 iunie 2013", datetime(2013, 6, 13)),
            param("14 aprilie 2014", datetime(2014, 4, 14)),
            param("18 martie 2012", datetime(2012, 3, 18)),
            param("S 14:14", datetime(2012, 11, 10, 14, 14)),
            param("12-Iun-2013", datetime(2013, 6, 12)),
            # German dates
            param("21. Dezember 2013", datetime(2013, 12, 21)),
            param("19. Februar 2012", datetime(2012, 2, 19)),
            param("26. Juli 2014", datetime(2014, 7, 26)),
            param("18.10.14 um 22:56 Uhr", datetime(2014, 10, 18, 22, 56)),
            param("12-Mär-2014", datetime(2014, 3, 12)),
            param("Mit 13:14", datetime(2012, 11, 7, 13, 14)),
            # Czech dates
            param("pon 16. čer 2014 10:07:43", datetime(2014, 6, 16, 10, 7, 43)),
            param("13 Srpen, 2014", datetime(2014, 8, 13)),
            param("čtv 14. lis 2013 12:38:43", datetime(2013, 11, 14, 12, 38, 43)),
            param("14Unr'21", datetime(2021, 2, 14)),
            param("02Bre'21", datetime(2021, 3, 2)),
            # Thai dates
            param("ธันวาคม 11, 2014, 08:55:08 PM", datetime(2014, 12, 11, 20, 55, 8)),
            param("22 พฤษภาคม 2012, 22:12", datetime(2012, 5, 22, 22, 12)),
            param("11 กุมภา 2020, 8:13 AM", datetime(2020, 2, 11, 8, 13)),
            param("1 เดือนตุลาคม 2005, 1:00 AM", datetime(2005, 10, 1, 1, 0)),
            param("11 ก.พ. 2020, 1:13 pm", datetime(2020, 2, 11, 13, 13)),
            # Vietnamese dates
            param("Thứ năm", datetime(2012, 11, 8)),  # Thursday
            param("Thứ sáu", datetime(2012, 11, 9)),  # Friday
            param(
                "Tháng Mười Hai 29, 2013, 14:14", datetime(2013, 12, 29, 14, 14)
            ),  # bpsosrcs.wordpress.com  # NOQA
            param("05 Tháng một 2015 - 03:54 AM", datetime(2015, 1, 5, 3, 54)),
            # Belarusian dates
            param("11 траўня", datetime(2012, 5, 11)),
            param("4 мая", datetime(2012, 5, 4)),
            param("Чацвер 06 жніўня 2015", datetime(2015, 8, 6)),
            param(
                "Нд 14 сакавіка 2015 у 7 гадзін 10 хвілін", datetime(2015, 3, 14, 7, 10)
            ),
            param("5 жніўня 2015 года у 13:34", datetime(2015, 8, 5, 13, 34)),
            # Ukrainian dates
            param("2015-кві-12", datetime(2015, 4, 12)),
            param("2020-берез-11", datetime(2020, 3, 11)),
            param("21 чер 2013 3:13", datetime(2013, 6, 21, 3, 13)),
            param("17 верес 2015 6:17", datetime(2015, 9, 17, 6, 17)),
            param("12 лютого 2012, 13:12:23", datetime(2012, 2, 12, 13, 12, 23)),
            param("10 листоп 2017, 10:00:00", datetime(2017, 11, 10, 10, 00, 00)),
            param("вів о 14:04", datetime(2012, 11, 13, 14, 4)),
            # Filipino dates
            param("12 Hulyo 2003 13:01", datetime(2003, 7, 12, 13, 1)),
            param("1978, 1 Peb, 7:05 PM", datetime(1978, 2, 1, 19, 5)),
            param("2 hun", datetime(2012, 6, 2)),
            param("Lin 16:16", datetime(2012, 11, 11, 16, 16)),
            # Japanese dates
            param("2016年3月20日(日) 21時40分", datetime(2016, 3, 20, 21, 40)),
            param("2016年3月20日 21時40分", datetime(2016, 3, 20, 21, 40)),
            # Bangla dates
            param("[সেপ্টেম্বর] 04, 2014.", datetime(2014, 9, 4)),
            param("মঙ্গলবার জুলাই 22, 2014", datetime(2014, 7, 22)),
            param("শুক্রবার", datetime(2012, 11, 9)),
            param("শুক্র, 12 ডিসেম্বর 2014 10:55:50", datetime(2014, 12, 12, 10, 55, 50)),
            param("1লা জানুয়ারী 2015", datetime(2015, 1, 1)),
            param("25শে মার্চ 1971", datetime(1971, 3, 25)),
            param("8ই মে 2002", datetime(2002, 5, 8)),
            param("10:06am ডিসেম্বর 11, 2014", datetime(2014, 12, 11, 10, 6)),
            param("19 ফেব্রুয়ারী 2013 সাল 09:10", datetime(2013, 2, 19, 9, 10)),
            # Numeric dates
            param("06-17-2014", datetime(2014, 6, 17)),
            param("13/03/2014", datetime(2014, 3, 13)),
            param("11. 12. 2014, 08:45:39", datetime(2014, 11, 12, 8, 45, 39)),
            # Miscellaneous dates
            param("1 Ni 2015", datetime(2015, 4, 1, 0, 0)),
            param("1 Mar 2015", datetime(2015, 3, 1, 0, 0)),
            param("1 сер 2015", datetime(2015, 8, 1, 0, 0)),
            # Bulgarian
            param("24 ян 2015г.", datetime(2015, 1, 24, 0, 0)),
            # Hindi dates
            param("बुधवार 24 मई 1997 12:09", datetime(1997, 5, 24, 12, 9)),
            param("28 दिसम्बर 2000 , 01:09:08", datetime(2000, 12, 28, 1, 9, 8)),
            param("१६ दिसम्बर १९७१", datetime(1971, 12, 16, 0, 0)),
            param("सन् 1989 11 फ़रवरी 09:43", datetime(1989, 2, 11, 9, 43)),
        ]
    )
    def test_dates_parsing_with_normalization(self, date_string, expected):
        self.given_local_tz_offset(0)
        self.given_parser(
            settings={"NORMALIZE": True, "RELATIVE_BASE": datetime(2012, 11, 13)}
        )
        self.when_date_is_parsed(normalize_unicode(date_string))
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("Sep 03 2014 | 4:32 pm EDT", datetime(2014, 9, 3, 20, 32)),
            param("17th October, 2034 @ 01:08 am PDT", datetime(2034, 10, 17, 8, 8)),
            param("15 May 2004 23:24 EDT", datetime(2004, 5, 16, 3, 24)),
            param("08/17/14 17:00 (PDT)", datetime(2014, 8, 18, 0, 0)),
        ]
    )
    def test_parsing_with_time_zones_and_converting_to_UTC(self, date_string, expected):
        self.given_parser(settings={"TO_TIMEZONE": "UTC"})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_timezone_parsed_is("UTC")
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("Sep 03 2014 | 4:32 pm EDT", "EDT", datetime(2014, 9, 3, 16, 32)),
            param(
                "17th October, 2034 @ 01:08 am PDT", "PDT", datetime(2034, 10, 17, 1, 8)
            ),
            param("15 May 2004 23:24 EDT", "EDT", datetime(2004, 5, 15, 23, 24)),
            param("08/17/14 17:00 (PDT)", "PDT", datetime(2014, 8, 17, 17, 0)),
            param("15 May 2004 16:10 -0400", "-04:00", datetime(2004, 5, 15, 16, 10)),
            param("1999-12-31 19:00:00 -0500", "-05:00", datetime(1999, 12, 31, 19, 0)),
            param("1999-12-31 19:00:00 +0500", "+05:00", datetime(1999, 12, 31, 19, 0)),
            param(
                "Fri, 09 Sep 2005 13:51:39 -0700",
                "-07:00",
                datetime(2005, 9, 9, 13, 51, 39),
            ),
            param(
                "Fri, 09 Sep 2005 13:51:39 +0000",
                "+00:00",
                datetime(2005, 9, 9, 13, 51, 39),
            ),
            param(
                "Mon, 25 Jun 2018 10:37:47 +0530 ",
                "+05:30",
                datetime(2018, 6, 25, 10, 37, 47),
            ),  # Trailing space
        ]
    )
    def test_dateparser_should_return_tzaware_date_when_tz_info_present_in_date_string(
        self, date_string, timezone_str, expected
    ):
        self.given_parser()
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_timezone_parsed_is(timezone_str)
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("15 May 2004 16:10 -0400", "UTC", datetime(2004, 5, 15, 20, 10)),
            param("1999-12-31 19:00:00 -0500", "UTC", datetime(2000, 1, 1, 0, 0)),
            param("1999-12-31 19:00:00 +0500", "UTC", datetime(1999, 12, 31, 14, 0)),
            param(
                "Fri, 09 Sep 2005 13:51:39 -0700",
                "GMT",
                datetime(2005, 9, 9, 20, 51, 39),
            ),
            param(
                "Fri, 09 Sep 2005 13:51:39 +0000",
                "GMT",
                datetime(2005, 9, 9, 13, 51, 39),
            ),
        ]
    )
    def test_dateparser_should_return_date_in_setting_timezone_if_timezone_info_present_in_datestring_and_in_settings(
        self, date_string, setting_timezone, expected
    ):
        self.given_parser(settings={"TIMEZONE": setting_timezone})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_timezone_parsed_is(setting_timezone)
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("15 May 2004 16:10 -0400", datetime(2004, 5, 15, 20, 10)),
            param("1999-12-31 19:00:00 -0500", datetime(2000, 1, 1, 0, 0)),
            param("1999-12-31 19:00:00 +0500", datetime(1999, 12, 31, 14, 0)),
            param("Fri, 09 Sep 2005 13:51:39 -0700", datetime(2005, 9, 9, 20, 51, 39)),
            param("Fri, 09 Sep 2005 13:51:39 +0000", datetime(2005, 9, 9, 13, 51, 39)),
            param(
                "Fri Sep 23 2016 10:34:51 GMT+0800 (CST)",
                datetime(2016, 9, 23, 2, 34, 51),
            ),
        ]
    )
    def test_parsing_with_utc_offsets(self, date_string, expected):
        self.given_parser(settings={"TO_TIMEZONE": "utc"})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_period_is("day")
        self.then_timezone_parsed_is("UTC")
        self.then_date_obj_exactly_is(expected)

    def test_empty_dates_string_is_not_parsed(self):
        self.when_date_is_parsed_by_date_parser("")
        self.then_error_was_raised(ValueError, ["Empty string"])

    @parameterized.expand(
        [
            param("invalid date string", "Unable to parse: invalid"),
            param("Aug 7, 2014Aug 7, 2014", "Unable to parse: Aug"),
            param("24h ago", "Unable to parse: h"),
            param(
                "2015-03-17t16:37:51+00:002015-03-17t15:24:37+00:00",
                "Unable to parse: 00:002015",
            ),
            param(
                "8 enero 2013 martes 7:03 AM EST 8 enero 2013 martes 7:03 AM EST",
                "Unable to parse: 8",
            ),
            param("12/09/18567", "Unable to parse: 18567"),
        ]
    )
    def test_dates_not_parsed(self, date_string, message):
        self.when_date_is_parsed_by_date_parser(date_string)
        self.then_error_was_raised(ValueError, message)

    @parameterized.expand(
        [
            param("10 December", datetime(2014, 12, 10)),
            param("March", datetime(2014, 3, 15)),
            param("Friday", datetime(2015, 2, 13)),
            param("Monday", datetime(2015, 2, 9)),
            param("Mon", datetime(2015, 2, 9)),
            param("Sunday", datetime(2015, 2, 8)),  # current day
            param("10:00PM", datetime(2015, 2, 14, 22, 0)),
            param("16:10", datetime(2015, 2, 14, 16, 10)),
            param("14:05", datetime(2015, 2, 15, 14, 5)),
            param("15 february 15:00", datetime(2015, 2, 15, 15, 0)),
            param("3/3/50", datetime(1950, 3, 3)),
            param("3/3/94", datetime(1994, 3, 3)),
        ]
    )
    def test_preferably_past_dates(self, date_string, expected):
        self.given_parser(
            settings={
                "PREFER_DATES_FROM": "past",
                "RELATIVE_BASE": datetime(2015, 2, 15, 15, 30),
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("10 December", datetime(2015, 12, 10)),
            param("March", datetime(2015, 3, 15)),
            param("Friday", datetime(2015, 2, 20)),
            param("Sunday", datetime(2015, 2, 22)),  # current day
            param("Monday", datetime(2015, 2, 16)),
            param("10:00PM", datetime(2015, 2, 15, 22, 0)),
            param("16:10", datetime(2015, 2, 15, 16, 10)),
            param("14:05", datetime(2015, 2, 16, 14, 5)),
            param("3/3/50", datetime(2050, 3, 3)),
            param("3/3/94", datetime(2094, 3, 3)),
        ]
    )
    def test_preferably_future_dates(self, date_string, expected):
        self.given_local_tz_offset(0)
        self.given_parser(
            settings={
                "PREFER_DATES_FROM": "future",
                "RELATIVE_BASE": datetime(2015, 2, 15, 15, 30),
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("Monday", datetime(2015, 3, 2)),
        ]
    )
    def test_preferably_future_dates_relative_last_week_of_month(
        self, date_string, expected
    ):
        self.given_local_tz_offset(0)
        self.given_parser(
            settings={
                "PREFER_DATES_FROM": "future",
                "RELATIVE_BASE": datetime(2015, 2, 24, 15, 30),
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("10 December", datetime(2015, 12, 10)),
            param("March", datetime(2015, 3, 15)),
            param("Friday", datetime(2015, 2, 13)),
            param("Sunday", datetime(2015, 2, 15)),  # current weekday
            param("10:00PM", datetime(2015, 2, 15, 22, 00)),
            param("16:10", datetime(2015, 2, 15, 16, 10)),
            param("14:05", datetime(2015, 2, 15, 14, 5)),
        ]
    )
    def test_dates_without_preference(self, date_string, expected):
        self.given_local_tz_offset(0)
        self.given_parser(
            settings={
                "PREFER_DATES_FROM": "current_period",
                "RELATIVE_BASE": datetime(2015, 2, 15, 15, 30),
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("29 Feb", datetime(2020, 1, 1), datetime(2016, 2, 29)),
            param("29/02", datetime(2020, 3, 30), datetime(2020, 2, 29)),
            param("02/29", datetime(1702, 1, 1), datetime(1696, 2, 29)),
        ]
    )
    def test_preferably_past_dates_leap_year(
        self, date_string, relative_base, expected
    ):
        self.given_parser(
            settings={"PREFER_DATES_FROM": "past", "RELATIVE_BASE": relative_base}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("29 Feb", datetime(2020, 1, 1), datetime(2020, 2, 29)),
            param("29/02", datetime(2020, 3, 30), datetime(2024, 2, 29)),
            param("02/29", datetime(1696, 3, 1), datetime(1704, 2, 29)),
        ]
    )
    def test_preferably_future_dates_leap_year(
        self, date_string, relative_base, expected
    ):
        self.given_parser(
            settings={"PREFER_DATES_FROM": "future", "RELATIVE_BASE": relative_base}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("29 Feb", datetime(2020, 1, 1), datetime(2020, 2, 29)),
            param("29/02", datetime(2020, 3, 30), datetime(2020, 2, 29)),
            param("29 Feb", datetime(1702, 3, 1), datetime(1704, 2, 29)),
            param("02/29", datetime(1699, 3, 1), datetime(1696, 2, 29)),
        ]
    )
    def test_dates_without_preference_leap_year(
        self, date_string, relative_base, expected
    ):
        self.given_local_tz_offset(0)
        self.given_parser(
            settings={
                "PREFER_DATES_FROM": "current_period",
                "RELATIVE_BASE": relative_base,
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(
                "February 2015",
                today=datetime(2015, 1, 31),
                expected=datetime(2015, 2, 28),
            ),
            param(
                "February 2012",
                today=datetime(2015, 1, 31),
                expected=datetime(2012, 2, 29),
            ),
            param(
                "March 2015",
                today=datetime(2015, 1, 25),
                expected=datetime(2015, 3, 25),
            ),
            param(
                "April 2015",
                today=datetime(2015, 1, 31),
                expected=datetime(2015, 4, 30),
            ),
            param(
                "April 2015",
                today=datetime(2015, 2, 28),
                expected=datetime(2015, 4, 28),
            ),
            param(
                "December 2014",
                today=datetime(2015, 2, 15),
                expected=datetime(2014, 12, 15),
            ),
        ]
    )
    def test_dates_with_day_missing_preferring_current_day_of_month(
        self, date_string, today=None, expected=None
    ):
        self.given_parser(
            settings={"PREFER_DAY_OF_MONTH": "current", "RELATIVE_BASE": today}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(
                "February 2015",
                today=datetime(2015, 1, 1),
                expected=datetime(2015, 2, 28),
            ),
            param(
                "February 2012",
                today=datetime(2015, 1, 1),
                expected=datetime(2012, 2, 29),
            ),
            param(
                "March 2015",
                today=datetime(2015, 1, 25),
                expected=datetime(2015, 3, 31),
            ),
            param(
                "April 2015",
                today=datetime(2015, 1, 15),
                expected=datetime(2015, 4, 30),
            ),
            param(
                "April 2015",
                today=datetime(2015, 2, 28),
                expected=datetime(2015, 4, 30),
            ),
            param(
                "December 2014",
                today=datetime(2015, 2, 15),
                expected=datetime(2014, 12, 31),
            ),
        ]
    )
    def test_dates_with_day_missing_preferring_last_day_of_month(
        self, date_string, today=None, expected=None
    ):
        self.given_parser(
            settings={"PREFER_DAY_OF_MONTH": "last", "RELATIVE_BASE": today}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(
                "February 2015",
                today=datetime(2015, 1, 8),
                expected=datetime(2015, 2, 1),
            ),
            param(
                "February 2012",
                today=datetime(2015, 1, 7),
                expected=datetime(2012, 2, 1),
            ),
            param(
                "March 2015", today=datetime(2015, 1, 25), expected=datetime(2015, 3, 1)
            ),
            param(
                "April 2015", today=datetime(2015, 1, 15), expected=datetime(2015, 4, 1)
            ),
            param(
                "April 2015", today=datetime(2015, 2, 28), expected=datetime(2015, 4, 1)
            ),
            param(
                "December 2014",
                today=datetime(2015, 2, 15),
                expected=datetime(2014, 12, 1),
            ),
        ]
    )
    def test_dates_with_day_missing_preferring_first_day_of_month(
        self, date_string, today=None, expected=None
    ):
        self.given_parser(
            settings={"PREFER_DAY_OF_MONTH": "first", "RELATIVE_BASE": today}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(prefer_day_of_month="current"),
            param(prefer_day_of_month="last"),
            param(prefer_day_of_month="first"),
        ]
    )
    def test_that_day_preference_does_not_affect_dates_with_explicit_day(
        self, prefer_day_of_month=None
    ):
        self.given_parser(
            settings={
                "PREFER_DAY_OF_MONTH": prefer_day_of_month,
                "RELATIVE_BASE": datetime(2015, 2, 12),
            }
        )
        self.when_date_is_parsed("24 April 2012")
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(datetime(2012, 4, 24))

    def test_date_is_parsed_when_skip_tokens_are_supplied(self):
        self.given_parser(
            settings={"SKIP_TOKENS": ["de"], "RELATIVE_BASE": datetime(2015, 2, 12)}
        )
        self.when_date_is_parsed("24 April 2012 de")
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(datetime(2012, 4, 24))

    @parameterized.expand(
        [
            param("29 February 2015", "day must be in 1..28"),
            param("32 January 2015", "day must be in 1..31"),
            param("31 April 2015", "day must be in 1..30"),
            param("31 June 2015", "day must be in 1..30"),
            param("31 September 2015", "day must be in 1..30"),
        ]
    )
    def test_error_should_be_raised_for_invalid_dates_with_too_large_day_number(
        self, date_string, message
    ):
        self.when_date_is_parsed_by_date_parser(date_string)
        self.then_error_was_raised(
            ValueError, ["day is out of range for month", "must be in range", message]
        )

    @parameterized.expand(
        [
            param(
                "2015-05-02T10:20:19+0000",
                languages=["fr"],
                expected=datetime(2015, 5, 2, 10, 20, 19),
            ),
            param(
                "2015-05-02T10:20:19+0000",
                languages=["en"],
                expected=datetime(2015, 5, 2, 10, 20, 19),
            ),
        ]
    )
    def test_iso_datestamp_format_should_always_parse(
        self, date_string, languages, expected
    ):
        self.given_local_tz_offset(0)
        self.given_parser(
            languages=languages, settings={"PREFER_LOCALE_DATE_ORDER": False}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.result["date_obj"] = self.result["date_obj"].replace(tzinfo=None)
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            # Epoch timestamps.
            param("1484823450", expected=datetime(2017, 1, 19, 10, 57, 30)),
            param("1436745600000", expected=datetime(2015, 7, 13, 0, 0)),
            param("1015673450", expected=datetime(2002, 3, 9, 11, 30, 50)),
            param(
                "2016-09-23T02:54:32.845Z",
                expected=datetime(
                    2016,
                    9,
                    23,
                    2,
                    54,
                    32,
                    845000,
                    tzinfo=StaticTzInfo("Z", timedelta(0)),
                ),
            ),
        ]
    )
    def test_parse_timestamp(self, date_string, expected):
        self.given_local_tz_offset(0)
        self.given_parser(settings={"TO_TIMEZONE": "UTC"})
        self.when_date_is_parsed(date_string)
        self.then_date_obj_exactly_is(expected)
        self.then_period_is("day")

    @parameterized.expand(
        [
            param("-1484823450", expected=datetime(1922, 12, 13, 13, 2, 30)),
            param("-1436745600000", expected=datetime(1924, 6, 22, 0, 0)),
            param("-1015673450000001", expected=datetime(1937, 10, 25, 12, 29, 10, 1)),
        ]
    )
    def test_parse_negative_timestamp(self, date_string, expected):
        self.given_local_tz_offset(0)
        self.given_parser(
            settings={"TO_TIMEZONE": "UTC", "PARSERS": ["negative-timestamp"]}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_obj_exactly_is(expected)
        self.then_period_is("day")

    @parameterized.expand(
        [
            # Epoch timestamps.
            param("1484823450", expected=datetime(2017, 1, 19, 10, 57, 30)),
            param("1436745600000", expected=datetime(2015, 7, 13, 0, 0)),
            param("1015673450", expected=datetime(2002, 3, 9, 11, 30, 50)),
            param(
                "2016-09-23T02:54:32.845Z",
                expected=datetime(
                    2016,
                    9,
                    23,
                    2,
                    54,
                    32,
                    845000,
                    tzinfo=StaticTzInfo("Z", timedelta(0)),
                ),
            ),
        ]
    )
    def test_parse_timestamp_with_time_as_period(self, date_string, expected):
        self.given_local_tz_offset(0)
        self.given_parser(
            settings={"TO_TIMEZONE": "UTC", "RETURN_TIME_AS_PERIOD": True}
        )
        self.when_date_is_parsed(date_string)
        self.then_date_obj_exactly_is(expected)

        self.then_period_is("time")

    @parameterized.expand(
        [
            param("10 December", expected=datetime(2015, 12, 10), period="day"),
            param("March", expected=datetime(2015, 3, 15), period="month"),
            param("April", expected=datetime(2015, 4, 15), period="month"),
            param("December", expected=datetime(2015, 12, 15), period="month"),
            param("Friday", expected=datetime(2015, 2, 13), period="day"),
            param("Monday", expected=datetime(2015, 2, 9), period="day"),
            param("10:00PM", expected=datetime(2015, 2, 15, 22, 00), period="day"),
            param("16:10", expected=datetime(2015, 2, 15, 16, 10), period="day"),
            param("1000", expected=datetime(1000, 2, 15), period="year"),
            param("2008", expected=datetime(2008, 2, 15), period="year"),
            param("2014", expected=datetime(2014, 2, 15), period="year"),
            # subscript and superscript dates
            param("²⁰¹⁵", expected=datetime(2015, 2, 15), period="year"),
            param("²⁹/⁰⁵/²⁰¹⁵", expected=datetime(2015, 5, 29), period="day"),
            param("₁₅/₀₂/₂₀₂₀", expected=datetime(2020, 2, 15), period="day"),
            param("₃₁ December", expected=datetime(2015, 12, 31), period="day"),
            # Russian
            param("1000 год", expected=datetime(1000, 2, 15), period="year"),
            param("1001 год", expected=datetime(1001, 2, 15), period="year"),
            param("2000 год", expected=datetime(2000, 2, 15), period="year"),
            param("2000год", expected=datetime(2000, 2, 15), period="year"),
            param("год2000", expected=datetime(2000, 2, 15), period="year"),
            param("год 2000", expected=datetime(2000, 2, 15), period="year"),
            param("2000г.", expected=datetime(2000, 2, 15), period="year"),
            param("2000 г.", expected=datetime(2000, 2, 15), period="year"),
            param("2001 год", expected=datetime(2001, 2, 15), period="year"),
            param("2001год", expected=datetime(2001, 2, 15), period="year"),
        ]
    )
    def test_extracted_period(self, date_string, expected=None, period=None):
        self.given_local_tz_offset(0)
        self.given_parser(settings={"RELATIVE_BASE": datetime(2015, 2, 15, 15, 30)})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)
        self.then_period_is(period)

    @parameterized.expand(
        [
            param(
                "12th December 2019 19:00",
                expected=datetime(2019, 12, 12, 19, 0),
                period="time",
            ),
            param("9 Jan 11 0:00", expected=datetime(2011, 1, 9, 0, 0), period="time"),
        ]
    )
    def test_period_is_time_if_return_time_as_period_setting_applied_and_time_component_present(
        self, date_string, expected=None, period=None
    ):
        self.given_parser(settings={"RETURN_TIME_AS_PERIOD": True})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)
        self.then_period_is(period)

    @parameterized.expand(
        [
            param("16:00", expected=datetime(2018, 12, 13, 16, 0), period="time"),
            param(
                "Monday 7:15 AM", expected=datetime(2018, 12, 10, 7, 15), period="time"
            ),
            param("Mon 19:43", expected=datetime(2018, 12, 10, 19, 43), period="time"),
        ]
    )
    def test_period_is_time_if_return_time_as_period_and_relative_base_settings_applied_and_time_component_present(
        self, date_string, expected=None, period=None
    ):
        self.given_parser(
            settings={
                "RETURN_TIME_AS_PERIOD": True,
                "RELATIVE_BASE": datetime(2018, 12, 13, 15, 15),
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)
        self.then_period_is(period)

    @parameterized.expand(
        [
            param(
                "12th March 2010", expected=datetime(2010, 3, 12, 0, 0), period="day"
            ),
            param("21-12-19", expected=datetime(2019, 12, 21, 0, 0), period="day"),
        ]
    )
    def test_period_is_day_if_return_time_as_period_setting_applied_and_time_component_is_not_present(
        self, date_string, expected=None, period=None
    ):
        self.given_parser(settings={"RETURN_TIME_AS_PERIOD": True})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)
        self.then_period_is(period)

    @parameterized.expand(
        [
            param("16:00", expected=datetime(2017, 1, 10, 16, 0), period="day"),
            param("Monday 7:15 AM", expected=datetime(2017, 1, 9, 7, 15), period="day"),
        ]
    )
    def test_period_is_day_if_return_time_as_period_setting_not_applied(
        self, date_string, expected=None, period=None
    ):
        self.given_parser(
            settings={
                "RETURN_TIME_AS_PERIOD": False,
                "RELATIVE_BASE": datetime(2017, 1, 10, 15, 15),
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)
        self.then_period_is(period)

    @parameterized.expand(
        [
            param("15-12-18 06:00", expected=datetime(2015, 12, 18, 6, 0), order="YMD"),
            param("15-18-12 06:00", expected=datetime(2015, 12, 18, 6, 0), order="YDM"),
            param("10-11-12 06:00", expected=datetime(2012, 10, 11, 6, 0), order="MDY"),
            param("10-11-12 06:00", expected=datetime(2011, 10, 12, 6, 0), order="MYD"),
            param("10-11-12 06:00", expected=datetime(2011, 12, 10, 6, 0), order="DYM"),
            param("15-12-18 06:00", expected=datetime(2018, 12, 15, 6, 0), order="DMY"),
            param(
                "12/09/08 04:23:15.567",
                expected=datetime(2008, 9, 12, 4, 23, 15, 567000),
                order="DMY",
            ),
            param(
                "10/9/1914 03:07:09.788888 pm",
                expected=datetime(1914, 10, 9, 15, 7, 9, 788888),
                order="MDY",
            ),
            param(
                "1-8-09 07:12:49 AM",
                expected=datetime(2009, 1, 8, 7, 12, 49),
                order="MDY",
            ),
            param("2016 july 13.", expected=datetime(2016, 7, 13, 0, 0), order="YMD"),
            param("16 july 13.", expected=datetime(2016, 7, 13, 0, 0), order="YMD"),
            param(
                "Sunday 23 May 1856 12:09:08 AM",
                expected=datetime(1856, 5, 23, 0, 9, 8),
                order="DMY",
            ),
        ]
    )
    def test_order(self, date_string, expected=None, order=None):
        self.given_parser(settings={"DATE_ORDER": order})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("201508", expected=datetime(2015, 8, 20, 0, 0), order="DYM"),
            param("201508", expected=datetime(2020, 8, 15, 0, 0), order="YDM"),
            param("201108", expected=datetime(2008, 11, 20, 0, 0), order="DMY"),
        ]
    )
    def test_order_no_spaces(self, date_string, expected=None, order=None):
        self.given_parser(settings={"DATE_ORDER": order, "PARSERS": ["no-spaces-time"]})
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(
                "10.1.2019",
                expected=datetime(2019, 1, 10, 0, 0),
                languages=["de"],
                settings={"PREFER_DAY_OF_MONTH": "first"},
            ),
            param("10.1.2019", expected=datetime(2019, 1, 10, 0, 0), languages=["de"]),
            param(
                "10.1.2019",
                expected=datetime(2019, 10, 1, 0, 0),
                settings={"DATE_ORDER": "MDY"},
            ),
            param(
                "03/11/2559 05:13",
                datetime(2559, 3, 11, 5, 13),
                languages=["th"],
                settings={"DATE_ORDER": "MDY"},
            ),
            param(
                "03/15/2559 05:13",
                datetime(2559, 3, 15, 5, 13),
                languages=["th"],
                settings={"DATE_ORDER": "MDY"},
            ),
        ]
    )
    def test_if_settings_provided_date_order_is_retained(
        self, date_string, expected=None, languages=None, settings=None
    ):
        self.given_parser(languages=languages, settings=settings)
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param("::", None),
            param("..", None),
            param("  ", None),
            param("--", None),
            param("//", None),
            param("++", None),
        ]
    )
    def test_parsing_strings_containing_only_separator_tokens(
        self, date_string, expected
    ):
        self.given_parser()
        self.when_date_is_parsed(date_string)
        self.then_period_is("day")
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(
                "4pm EDT",
                datetime(2021, 10, 19, 20, 0),
                {"PREFER_DATES_FROM": "future"},
            ),
            param(
                "10pm EDT",
                datetime(2021, 10, 20, 2, 0),
                {"PREFER_DATES_FROM": "future"},
            ),
            param(
                "8am AEDT",
                datetime(2021, 10, 18, 21, 0),
                {"PREFER_DATES_FROM": "past"},
            ),
            param(
                "11pm AEDT",
                datetime(2021, 10, 19, 12, 0),
                {"PREFER_DATES_FROM": "past"},
            ),
            param(
                "4pm",
                datetime(2021, 10, 19, 20, 0),
                {"PREFER_DATES_FROM": "future", "TIMEZONE": "EDT"},
            ),
            param(
                "10pm",
                datetime(2021, 10, 20, 2, 0),
                {"PREFER_DATES_FROM": "future", "TIMEZONE": "EDT"},
            ),
            param(
                "8am",
                datetime(2021, 10, 18, 21, 0),
                {"PREFER_DATES_FROM": "past", "TIMEZONE": "AEDT"},
            ),
            param(
                "11pm",
                datetime(2021, 10, 19, 12, 0),
                {"PREFER_DATES_FROM": "past", "TIMEZONE": "AEDT"},
            ),
        ]
    )
    def test_prefer_dates_from_with_timezone(
        self, date_string, expected, test_settings
    ):
        self.given_parser(
            settings={
                "TO_TIMEZONE": "etc/utc",
                "RETURN_AS_TIMEZONE_AWARE": False,
                "RELATIVE_BASE": datetime(2021, 10, 19, 18, 0),
                **test_settings,
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(
                "2015",
                prefer_day="current",
                prefer_month="current",
                today=datetime(2010, 2, 10),
                expected=datetime(2015, 2, 10),
            ),
            param(
                "2015",
                prefer_day="last",
                prefer_month="current",
                today=datetime(2010, 2, 10),
                expected=datetime(2015, 2, 28),
            ),
            param(
                "2015",
                prefer_day="first",
                prefer_month="current",
                today=datetime(2010, 2, 10),
                expected=datetime(2015, 2, 1),
            ),
            param(
                "2015",
                prefer_day="current",
                prefer_month="last",
                today=datetime(2010, 2, 10),
                expected=datetime(2015, 12, 10),
            ),
            param(
                "2015",
                prefer_day="last",
                prefer_month="last",
                today=datetime(2010, 2, 10),
                expected=datetime(2015, 12, 31),
            ),
            param(
                "2020",  # Leap year last day test
                prefer_day="last",
                prefer_month="current",
                today=datetime(2010, 2, 10),
                expected=datetime(2020, 2, 29),
            ),
        ]
    )
    def test_dates_with_no_day_or_month(
        self, date_string, prefer_day, prefer_month, today=None, expected=None
    ):
        self.given_parser(
            settings={
                "PREFER_DAY_OF_MONTH": prefer_day,
                "PREFER_MONTH_OF_YEAR": prefer_month,
                "RELATIVE_BASE": today,
            }
        )
        self.when_date_is_parsed(date_string)
        self.then_date_was_parsed_by_date_parser()
        self.then_date_obj_exactly_is(expected)

    @parameterized.expand(
        [
            param(
                "yesterday +1h",
                lambda base: base - timedelta(days=1) + timedelta(hours=1),
                "Yesterday plus 1 hour",
            ),
            param(
                "yesterday +2h",
                lambda base: base - timedelta(days=1) + timedelta(hours=2),
                "Yesterday plus 2 hours",
            ),
            param(
                "yesterday +30m",
                lambda base: base - timedelta(days=1) + timedelta(minutes=30),
                "Yesterday plus 30 minutes",
            ),
            param(
                "yesterday -1h",
                lambda base: base - timedelta(days=1) - timedelta(hours=1),
                "Yesterday minus 1 hour",
            ),
            param(
                "yesterday -2h",
                lambda base: base - timedelta(days=1) - timedelta(hours=2),
                "Yesterday minus 2 hours",
            ),
            param(
                "yesterday -30m",
                lambda base: base - timedelta(days=1) - timedelta(minutes=30),
                "Yesterday minus 30 minutes",
            ),
            param(
                "tomorrow +1h",
                lambda base: base + timedelta(days=1) + timedelta(hours=1),
                "Tomorrow plus 1 hour",
            ),
            param(
                "tomorrow +3h",
                lambda base: base + timedelta(days=1) + timedelta(hours=3),
                "Tomorrow plus 3 hours",
            ),
            param(
                "tomorrow -1h",
                lambda base: base + timedelta(days=1) - timedelta(hours=1),
                "Tomorrow minus 1 hour",
            ),
            param(
                "tomorrow -2h",
                lambda base: base + timedelta(days=1) - timedelta(hours=2),
                "Tomorrow minus 2 hours",
            ),
        ]
    )
    def test_relative_date_with_time_offset(
        self, date_string, offset_calculator, description
    ):
        """Ensure +/- signs in time offsets are parsed correctly."""
        base_date = datetime(2026, 1, 19, 12, 0, 0)
        expected = offset_calculator(base_date)

        result = parse(
            date_string,
            settings={
                "RELATIVE_BASE": base_date,
                "RETURN_AS_TIMEZONE_AWARE": False,
            },
        )

        self.assertIsNotNone(result, f"Failed to parse: {description}")
        self.assertEqual(
            expected,
            result,
            f"{description}: Expected {expected}, got {result}",
        )

    def given_local_tz_offset(self, offset):
        self.add_patch(
            patch.object(
                dateparser.timezone_parser,
                "local_tz_offset",
                new=timedelta(seconds=3600 * offset),
            )
        )

    def test_yesterday_plus_and_minus_expected_values(self):
        """Verify correct time offset calculations for yesterday."""
        # Base: 2026-01-08 21:38:10
        base_date = datetime(2026, 1, 8, 21, 38, 10)

        plus_result = parse(
            "yesterday +1h",
            settings={"RELATIVE_BASE": base_date, "RETURN_AS_TIMEZONE_AWARE": False},
        )
        minus_result = parse(
            "yesterday -1h",
            settings={"RELATIVE_BASE": base_date, "RETURN_AS_TIMEZONE_AWARE": False},
        )

        # Expected: 2026-01-07 22:38:10 (yesterday at same time, plus 1 hour)
        expected_plus = datetime(2026, 1, 7, 22, 38, 10)
        # Expected: 2026-01-07 20:38:10 (yesterday at same time, minus 1 hour)
        expected_minus = datetime(2026, 1, 7, 20, 38, 10)

        self.assertEqual(
            expected_plus,
            plus_result,
            f"'yesterday +1h' should be {expected_plus}, got {plus_result}",
        )
        self.assertEqual(
            expected_minus,
            minus_result,
            f"'yesterday -1h' should be {expected_minus}, got {minus_result}",
        )

    def given_parser(self, *args, **kwds):
        def collecting_get_date_data(parse):
            @wraps(parse)
            def wrapped(*args, **kwargs):
                self.date_result = parse(*args, **kwargs)
                return self.date_result

            return wrapped

        self.add_patch(
            patch.object(
                date_parser, "parse", collecting_get_date_data(date_parser.parse)
            )
        )

        self.date_parser = Mock(wraps=date_parser)
        self.add_patch(patch("dateparser.date.date_parser", new=self.date_parser))
        self.parser = DateDataParser(*args, **kwds)

    def when_date_is_parsed(self, date_string):
        self.result = self.parser.get_date_data(date_string)

    def when_date_is_parsed_by_date_parser(self, date_string):
        try:
            self.result = DateParser().parse(date_string, parse_method=_parse_absolute)
        except Exception as error:
            self.error = error

    def then_period_is(self, period):
        self.assertEqual(period, self.result["period"])

    def then_date_obj_exactly_is(self, expected):
        self.assertEqual(expected, self.result["date_obj"])

    def then_date_was_parsed_by_date_parser(self):
        self.assertNotEqual(NotImplemented, self.date_result, "Date was not parsed")
        self.assertEqual(self.result["date_obj"], self.date_result[0])

    def then_timezone_parsed_is(self, tzstr):
        self.assertTrue(tzstr in repr(self.result["date_obj"].tzinfo))
        self.result["date_obj"] = self.result["date_obj"].replace(tzinfo=None)


if __name__ == "__main__":
    unittest.main()
