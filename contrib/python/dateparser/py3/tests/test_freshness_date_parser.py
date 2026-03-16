import unittest
from datetime import date, datetime, time, timedelta
from functools import wraps

import pytz

from zoneinfo import ZoneInfo

from unittest.mock import Mock, patch

from dateutil.relativedelta import relativedelta
from parameterized import param, parameterized

import dateparser
from dateparser.conf import settings
from dateparser.date import DateDataParser, freshness_date_parser
from dateparser.utils import normalize_unicode
from tests import BaseTestCase


class TestFreshnessDateDataParser(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.now = datetime(2014, 9, 1, 10, 30)
        self.date_string = NotImplemented
        self.parser = NotImplemented
        self.result = NotImplemented
        self.freshness_parser = NotImplemented
        self.freshness_result = NotImplemented
        self.date = NotImplemented
        self.time = NotImplemented

        settings.TIMEZONE = "utc"

    def now_with_timezone(self, tz):
        now = self.now
        return datetime(now.year, now.month, now.day, now.hour, now.minute, tzinfo=tz)

    @parameterized.expand(
        [
            # English dates
            param("yesterday", ago={"days": 1}, period="day"),
            param("yesterday at 11:30", ago={"hours": 23}, period="time"),
        ]
    )
    def test_relative_past_dates_with_time_as_period(self, date_string, ago, period):
        self.given_parser(settings={"NORMALIZE": False, "RETURN_TIME_AS_PERIOD": True})
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.then_date_was_parsed_by_freshness_parser()
        self.then_date_obj_is_exactly_this_time_ago(ago)
        self.then_period_is(period)

    @parameterized.expand(
        [
            # English dates
            param("1 decade", ago={"years": 10}, period="year"),
            param("1 decade 2 years", ago={"years": 12}, period="year"),
            param(
                "1 decade 12 months", ago={"years": 10, "months": 12}, period="month"
            ),
            param(
                "1 decade and 11 months",
                ago={"years": 10, "months": 11},
                period="month",
            ),
            param("last decade", ago={"years": 10}, period="year"),
            param("a decade ago", ago={"years": 10}, period="year"),
            param("100 decades", ago={"years": 1000}, period="year"),
            param("yesterday", ago={"days": 1}, period="day"),
            param("the day before yesterday", ago={"days": 2}, period="day"),
            param("4 days before", ago={"days": 4}, period="day"),
            param("today", ago={"days": 0}, period="day"),
            param("till date", ago={"days": 0}, period="day"),
            param("an hour ago", ago={"hours": 1}, period="day"),
            param("about an hour ago", ago={"hours": 1}, period="day"),
            param("a day ago", ago={"days": 1}, period="day"),
            param("1d ago", ago={"days": 1}, period="day"),
            param("a week ago", ago={"weeks": 1}, period="week"),
            param("2 hours ago", ago={"hours": 2}, period="day"),
            param("about 23 hours ago", ago={"hours": 23}, period="day"),
            param("1 year 2 months", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 year, 09 months,01 weeks",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 year 11 months", ago={"years": 1, "months": 11}, period="month"),
            param("1 year 12 months", ago={"years": 1, "months": 12}, period="month"),
            param("15 hr", ago={"hours": 15}, period="day"),
            param("15 hrs", ago={"hours": 15}, period="day"),
            param("2 min", ago={"minutes": 2}, period="day"),
            param("2 mins", ago={"minutes": 2}, period="day"),
            param("3 sec", ago={"seconds": 3}, period="day"),
            param("1000 years ago", ago={"years": 1000}, period="year"),
            param(
                "2013 years ago", ago={"years": 2013}, period="year"
            ),  # We've fixed .now in setUp
            param("5000 months ago", ago={"years": 416, "months": 8}, period="month"),
            param(
                "{} months ago".format(2013 * 12 + 8),
                ago={"years": 2013, "months": 8},
                period="month",
            ),
            param(
                "1 year, 1 month, 1 week, 1 day, 1 hour and 1 minute ago",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            param("just now", ago={"seconds": 0}, period="day"),
            # Fix for #291, work till one to twelve only
            param("nine hours ago", ago={"hours": 9}, period="day"),
            param("three week ago", ago={"weeks": 3}, period="week"),
            param("eight months ago", ago={"months": 8}, period="month"),
            param("six days ago", ago={"days": 6}, period="day"),
            param("five years ago", ago={"years": 5}, period="year"),
            param("2y ago", ago={"years": 2}, period="year"),
            # Fractional units
            param("2.5 hours", ago={"hours": 2.5}, period="day"),
            param("10.75 minutes", ago={"minutes": 10.75}, period="day"),
            param("1.5 days", ago={"days": 1.5}, period="day"),
            # French dates
            param("Aujourd'hui", ago={"days": 0}, period="day"),
            param("Aujourd’hui", ago={"days": 0}, period="day"),
            param("Aujourdʼhui", ago={"days": 0}, period="day"),
            param("Aujourdʻhui", ago={"days": 0}, period="day"),
            param("Aujourd՚hui", ago={"days": 0}, period="day"),
            param("Aujourdꞌhui", ago={"days": 0}, period="day"),
            param("Aujourd＇hui", ago={"days": 0}, period="day"),
            param("Aujourd′hui", ago={"days": 0}, period="day"),
            param("Aujourd‵hui", ago={"days": 0}, period="day"),
            param("Aujourdʹhui", ago={"days": 0}, period="day"),
            param("Aujourd＇hui", ago={"days": 0}, period="day"),
            param("moins de 21s", ago={"seconds": 21}, period="day"),
            param("moins de 21m", ago={"minutes": 21}, period="day"),
            param("moins de 21h", ago={"hours": 21}, period="day"),
            param("moins de 21 minute", ago={"minutes": 21}, period="day"),
            param("moins de 21 heure", ago={"hours": 21}, period="day"),
            param("Hier", ago={"days": 1}, period="day"),
            param("Avant-hier", ago={"days": 2}, period="day"),
            param("Il ya un jour", ago={"days": 1}, period="day"),
            param("Il ya une heure", ago={"hours": 1}, period="day"),
            param("Il ya 2 heures", ago={"hours": 2}, period="day"),
            param("Il ya environ 23 heures", ago={"hours": 23}, period="day"),
            param("1 an 2 mois", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 année, 09 mois, 01 semaines",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 an 11 mois", ago={"years": 1, "months": 11}, period="month"),
            param(
                "Il ya 1 an, 1 mois, 1 semaine, 1 jour, 1 heure et 1 minute",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            param("Il y a 40 min", ago={"minutes": 40}, period="day"),
            # German dates
            param("Heute", ago={"days": 0}, period="day"),
            param("Gestern", ago={"days": 1}, period="day"),
            param("vorgestern", ago={"days": 2}, period="day"),
            param("vor einem Tag", ago={"days": 1}, period="day"),
            param("vor einer Stunden", ago={"hours": 1}, period="day"),
            param("Vor 2 Stunden", ago={"hours": 2}, period="day"),
            param("vor etwa 23 Stunden", ago={"hours": 23}, period="day"),
            param("1 Jahr 2 Monate", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 Jahr, 09 Monate, 01 Wochen",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 Jahr 11 Monate", ago={"years": 1, "months": 11}, period="month"),
            param("vor 29h", ago={"hours": 29}, period="day"),
            param("vor 29m", ago={"minutes": 29}, period="day"),
            param(
                "1 Jahr, 1 Monat, 1 Woche, 1 Tag, 1 Stunde und 1 Minute",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Italian dates
            param("oggi", ago={"days": 0}, period="day"),
            param("ieri", ago={"days": 1}, period="day"),
            param("2 ore fa", ago={"hours": 2}, period="day"),
            param("circa 23 ore fa", ago={"hours": 23}, period="day"),
            param("1 anno 2 mesi", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 anno, 09 mesi, 01 settimane",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 anno 11 mesi", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 anno, 1 mese, 1 settimana, 1 giorno, 1 ora e 1 minuto fa",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Portuguese dates
            param("ontem", ago={"days": 1}, period="day"),
            param("anteontem", ago={"days": 2}, period="day"),
            param("hoje", ago={"days": 0}, period="day"),
            param("uma hora atrás", ago={"hours": 1}, period="day"),
            param("1 segundo atrás", ago={"seconds": 1}, period="day"),
            param("um dia atrás", ago={"days": 1}, period="day"),
            param("uma semana atrás", ago={"weeks": 1}, period="week"),
            param("2 horas atrás", ago={"hours": 2}, period="day"),
            param("cerca de 23 horas atrás", ago={"hours": 23}, period="day"),
            param("1 ano 2 meses", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 ano, 09 meses, 01 semanas",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 ano 11 meses", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 ano, 1 mês, 1 semana, 1 dia, 1 hora e 1 minuto atrás",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Turkish dates
            param("Dün", ago={"days": 1}, period="day"),
            param("Bugün", ago={"days": 0}, period="day"),
            param("2 saat önce", ago={"hours": 2}, period="day"),
            param("yaklaşık 23 saat önce", ago={"hours": 23}, period="day"),
            param("1 yıl 2 ay", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 yıl, 09 ay, 01 hafta",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 yıl 11 ay", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 yıl, 1 ay, 1 hafta, 1 gün, 1 saat ve 1 dakika önce",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Russian dates
            param("сегодня", ago={"days": 0}, period="day"),
            param("Вчера в", ago={"days": 1}, period="day"),
            param("вчера", ago={"days": 1}, period="day"),
            param("2 часа назад", ago={"hours": 2}, period="day"),
            param("час назад", ago={"hours": 1}, period="day"),
            param("минуту назад", ago={"minutes": 1}, period="day"),
            param("2 ч. 21 мин. назад", ago={"hours": 2, "minutes": 21}, period="day"),
            param("около 23 часов назад", ago={"hours": 23}, period="day"),
            param("1 год 2 месяца", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 год, 09 месяцев, 01 недель",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 год 11 месяцев", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 год, 1 месяц, 1 неделя, 1 день, 1 час и 1 минуту назад",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            param("991 год", ago={"years": 991}, period="year"),
            param("2000 лет", ago={"years": 2000}, period="year"),
            param(
                "2001 год 2 месяца", ago={"years": 2001, "months": 2}, period="month"
            ),
            param("2001 год назад", ago={"years": 2001}, period="year"),
            # Czech dates
            param("Dnes", ago={"days": 0}, period="day"),
            param("Včera", ago={"days": 1}, period="day"),
            param("Předevčírem", ago={"days": 2}, period="day"),
            param("Před 2 hodinami", ago={"hours": 2}, period="day"),
            param("před přibližně 23 hodin", ago={"hours": 23}, period="day"),
            param("1 rok 2 měsíce", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 rok, 09 měsíců, 01 týdnů",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 rok 11 měsíců", ago={"years": 1, "months": 11}, period="month"),
            param("3 dny", ago={"days": 3}, period="day"),
            param("3 hodiny", ago={"hours": 3}, period="day"),
            param(
                "2 roky, 2 týdny, 1 den, 1 hodinu, 5 vteřin před",
                ago={"years": 2, "weeks": 2, "days": 1, "hours": 1, "seconds": 5},
                period="day",
            ),
            param(
                "1 rok, 1 měsíc, 1 týden, 1 den, 1 hodina, 1 minuta před",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Spanish dates
            param("anteayer", ago={"days": 2}, period="day"),
            param("ayer", ago={"days": 1}, period="day"),
            param("hoy", ago={"days": 0}, period="day"),
            param("hace una hora", ago={"hours": 1}, period="day"),
            param("Hace un día", ago={"days": 1}, period="day"),
            param("Hace una semana", ago={"weeks": 1}, period="week"),
            param("Hace 2 horas", ago={"hours": 2}, period="day"),
            param("Hace cerca de 23 horas", ago={"hours": 23}, period="day"),
            param("1 año 2 meses", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 año, 09 meses, 01 semanas",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 año 11 meses", ago={"years": 1, "months": 11}, period="month"),
            param(
                "Hace 1 año, 1 mes, 1 semana, 1 día, 1 hora y 1 minuto",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Chinese dates
            param("昨天", ago={"days": 1}, period="day"),
            param("前天", ago={"days": 2}, period="day"),
            param("2小时前", ago={"hours": 2}, period="day"),
            param("约23小时前", ago={"hours": 23}, period="day"),
            param("1年2个月", ago={"years": 1, "months": 2}, period="month"),
            param("1年2個月", ago={"years": 1, "months": 2}, period="month"),
            param("1年11个月", ago={"years": 1, "months": 11}, period="month"),
            param("1年11個月", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1年，1月，1周，1天，1小时，1分钟前",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Arabic dates
            param("اليوم", ago={"days": 0}, period="day"),
            param("يوم أمس", ago={"days": 1}, period="day"),
            param("منذ يومين", ago={"days": 2}, period="day"),
            param("منذ 3 أيام", ago={"days": 3}, period="day"),
            param("منذ 21 أيام", ago={"days": 21}, period="day"),
            param(
                "1 عام, 1 شهر, 1 أسبوع, 1 يوم, 1 ساعة, 1 دقيقة",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Thai dates
            param("วันนี้", ago={"days": 0}, period="day"),
            param("เมื่อวานนี้", ago={"days": 1}, period="day"),
            param("2 วัน", ago={"days": 2}, period="day"),
            param("2 ชั่วโมง", ago={"hours": 2}, period="day"),
            param("23 ชม.", ago={"hours": 23}, period="day"),
            param("2 สัปดาห์ 3 วัน", ago={"weeks": 2, "days": 3}, period="day"),
            param(
                "1 ปี 9 เดือน 1 สัปดาห์",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param(
                "1 ปี 1 เดือน 1 สัปดาห์ 1 วัน 1 ชั่วโมง 1 นาที",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Vietnamese dates
            param("Hôm nay", ago={"days": 0}, period="day"),
            param("Hôm qua", ago={"days": 1}, period="day"),
            param("2 giờ", ago={"hours": 2}, period="day"),
            param("2 tuần 3 ngày", ago={"weeks": 2, "days": 3}, period="day"),
            # following test unsupported, refer to discussion at:
            # http://github.com/scrapinghub/dateparser/issues/33
            # param('1 năm 1 tháng 1 tuần 1 ngày 1 giờ 1 chút',
            #      ago={'years': 1, 'months': 1, 'weeks': 1, 'days': 1, 'hours': 1, 'minutes': 1},
            #      period='day'),
            # Belarusian dates
            param("сёння", ago={"days": 0}, period="day"),
            param("учора ў", ago={"days": 1}, period="day"),
            param("ўчора", ago={"days": 1}, period="day"),
            param("пазаўчора", ago={"days": 2}, period="day"),
            param("2 гадзіны таму назад", ago={"hours": 2}, period="day"),
            param("2 гадзіны таму", ago={"hours": 2}, period="day"),
            param("гадзіну назад", ago={"hours": 1}, period="day"),
            param("хвіліну таму", ago={"minutes": 1}, period="day"),
            param(
                "2 гадзіны 21 хвіл. назад",
                ago={"hours": 2, "minutes": 21},
                period="day",
            ),
            param("каля 23 гадзін назад", ago={"hours": 23}, period="day"),
            param("1 год 2 месяцы", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 год, 09 месяцаў, 01 тыдзень",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("2 гады 3 месяцы", ago={"years": 2, "months": 3}, period="month"),
            param(
                "5 гадоў, 1 месяц, 6 тыдняў, 3 дні, 5 гадзін 1 хвіліну і 3 секунды таму назад",
                ago={
                    "years": 5,
                    "months": 1,
                    "weeks": 6,
                    "days": 3,
                    "hours": 5,
                    "minutes": 1,
                    "seconds": 3,
                },
                period="day",
            ),
            # Polish dates
            param("wczoraj", ago={"days": 1}, period="day"),
            param(
                "1 godz. 2 minuty temu", ago={"hours": 1, "minutes": 2}, period="day"
            ),
            param(
                "2 lata, 3 miesiące, 1 tydzień, 2 dni, 4 godziny, 15 minut i 25 sekund temu",
                ago={
                    "years": 2,
                    "months": 3,
                    "weeks": 1,
                    "days": 2,
                    "hours": 4,
                    "minutes": 15,
                    "seconds": 25,
                },
                period="day",
            ),
            param("2 minuty temu", ago={"minutes": 2}, period="day"),
            param("15 minut temu", ago={"minutes": 15}, period="day"),
            # Bulgarian dates
            param("преди 3 дни", ago={"days": 3}, period="day"),
            param("преди час", ago={"hours": 1}, period="day"),
            param("преди година", ago={"years": 1}, period="year"),
            param("вчера", ago={"days": 1}, period="day"),
            param("онзи ден", ago={"days": 2}, period="day"),
            param("днес", ago={"days": 0}, period="day"),
            param("преди час", ago={"hours": 1}, period="day"),
            param("преди един ден", ago={"days": 1}, period="day"),
            param("преди седмица", ago={"weeks": 1}, period="week"),
            param("преди 2 часа", ago={"hours": 2}, period="day"),
            param("преди около 23 часа", ago={"hours": 23}, period="day"),
            # Bangla dates
            # param('গতকাল', ago={'days': 1}, period='day'),
            # param('আজ', ago={'days': 0}, period='day'),
            param("1 ঘন্টা আগে", ago={"hours": 1}, period="day"),
            param("প্রায় 1 ঘন্টা আগে", ago={"hours": 1}, period="day"),
            param("1 দিন আগে", ago={"days": 1}, period="day"),
            param("1 সপ্তাহ আগে", ago={"weeks": 1}, period="week"),
            param("2 ঘন্টা আগে", ago={"hours": 2}, period="day"),
            param("প্রায় 23 ঘন্টা আগে", ago={"hours": 23}, period="day"),
            param("1 বছর 2 মাস", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 বছর, 09 মাস,01 সপ্তাহ",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 বছর 11 মাস", ago={"years": 1, "months": 11}, period="month"),
            param("1 বছর 12 মাস", ago={"years": 1, "months": 12}, period="month"),
            param("15 ঘন্টা", ago={"hours": 15}, period="day"),
            param("2 মিনিট", ago={"minutes": 2}, period="day"),
            param("3 সেকেন্ড", ago={"seconds": 3}, period="day"),
            param("1000 বছর আগে", ago={"years": 1000}, period="year"),
            param("5000 মাস আগে", ago={"years": 416, "months": 8}, period="month"),
            param(
                "{} মাস আগে".format(2013 * 12 + 8),
                ago={"years": 2013, "months": 8},
                period="month",
            ),
            param(
                "1 বছর, 1 মাস, 1 সপ্তাহ, 1 দিন, 1 ঘন্টা এবং 1 মিনিট আগে",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # param('এখন', ago={'seconds': 0}, period='day'),
            # Hindi dates
            param("1 घंटे पहले", ago={"hours": 1}, period="day"),
            param("15 मिनट पहले", ago={"minutes": 15}, period="day"),
            param("25 सेकंड पूर्व", ago={"seconds": 25}, period="day"),
            param(
                "1 वर्ष, 8 महीने, 2 सप्ताह",
                ago={"years": 1, "months": 8, "weeks": 2},
                period="week",
            ),
            param("1 वर्ष 7 महीने", ago={"years": 1, "months": 7}, period="month"),
            param("आज", ago={"days": 0}, period="day"),
            param("1 दशक", ago={"years": 10}, period="year"),
            param("1 दशक पहले", ago={"years": 10}, period="year"),
            # af
            param("2 uur gelede", ago={"hours": 2}, period="day"),
            param("verlede maand", ago={"months": 1}, period="month"),
            # agq
            param("ā zūɛɛ", ago={"days": 1}, period="day"),
            # ak
            param("ndeda", ago={"days": 1}, period="day"),
            # am
            param("ከ8 ወራት በፊት", ago={"months": 8}, period="month"),
            param("ያለፈው ሳምንት", ago={"weeks": 1}, period="week"),
            # as
            param("কালি", ago={"days": 1}, period="day"),
            param("আজি", ago={"days": 0}, period="day"),
            # asa
            param("ighuo", ago={"days": 1}, period="day"),
            # ast
            param(
                "hai 2 selmanes hai 3 díes", ago={"weeks": 2, "days": 3}, period="day"
            ),
            param(
                "l'añu pas el mes pasáu", ago={"years": 1, "months": 1}, period="month"
            ),
            # az-Latn
            param(
                "1 il öncə 2 ay öncə 3 həftə öncə",
                ago={"years": 1, "months": 2, "weeks": 3},
                period="week",
            ),
            param(
                "6 saat öncə 5 dəqiqə öncə 4 saniyə öncə",
                ago={"hours": 6, "minutes": 5, "seconds": 4},
                period="day",
            ),
            # az
            param(
                "2 gün öncə 23 saat öncə", ago={"days": 2, "hours": 23}, period="day"
            ),
            param(
                "5 dəqiqə öncə 27 saniyə öncə",
                ago={"minutes": 5, "seconds": 27},
                period="day",
            ),
            # be
            param(
                "2 гадзіны таму 10 хвіліны таму",
                ago={"hours": 2, "minutes": 10},
                period="day",
            ),
            # bg
            param(
                "преди 3 месеца преди 2 седм",
                ago={"months": 3, "weeks": 2},
                period="week",
            ),
            # bn
            param(
                "8 মিনিট আগে 42 সেকেন্ড পূর্বে",
                ago={"minutes": 8, "seconds": 42},
                period="day",
            ),
            # br
            param(
                "7 eur zo 15 min zo 25 s zo",
                ago={"hours": 7, "minutes": 15, "seconds": 25},
                period="day",
            ),
            param("14 sizhun zo 3 deiz zo", ago={"weeks": 14, "days": 3}, period="day"),
            # bs-Cyrl
            param(
                "пре 5 година пре 7 месеци",
                ago={"years": 5, "months": 7},
                period="month",
            ),
            param(
                "пре 5 сати пре 25 секунди",
                ago={"hours": 5, "seconds": 25},
                period="day",
            ),
            # bs-Latn
            param(
                "prije 20 sat 5 minuta", ago={"hours": 20, "minutes": 5}, period="day"
            ),
            param(
                "prije 13 godina prije 3 sed",
                ago={"years": 13, "weeks": 3},
                period="week",
            ),
            # bs
            param(
                "prije 3 mjeseci prije 12 sati",
                ago={"months": 3, "hours": 12},
                period="month",
            ),
            param(
                "prije 3 god 4 mj 5 sed 7 dan",
                ago={"years": 3, "months": 4, "weeks": 5, "days": 7},
                period="day",
            ),
            # ca
            param(
                "fa 4 setmanes fa 5 segon",
                ago={"weeks": 4, "seconds": 5},
                period="week",
            ),
            param(
                "fa 1 hora 2 minut 3 segon",
                ago={"hours": 1, "minutes": 2, "seconds": 3},
                period="day",
            ),
            # ce
            param(
                "10 кӏир хьалха 3 д хьалха", ago={"weeks": 10, "days": 3}, period="day"
            ),
            param(
                "12 сахь 30 мин 30 сек хьалха",
                ago={"hours": 12, "minutes": 30, "seconds": 30},
                period="day",
            ),
            # chr
            param(
                "ᎾᎿ 10 ᏒᎾᏙᏓᏆᏍᏗ ᏥᎨᏒ 5 ᎢᎦ ᏥᎨᏒ", ago={"weeks": 10, "days": 5}, period="day"
            ),
            # cs
            param(
                "před 3 rok 4 měsíc 5 den",
                ago={"years": 3, "months": 4, "days": 5},
                period="day",
            ),
            param(
                "před 2 minutou před 45 sekundou",
                ago={"minutes": 2, "seconds": 45},
                period="day",
            ),
            # cy
            param("5 wythnos yn ôl", ago={"weeks": 5}, period="week"),
            param(
                "7 munud 8 eiliad yn ôl", ago={"minutes": 7, "seconds": 8}, period="day"
            ),
            # dsb
            param(
                "pśed 15 góźinu 10 minuta 5 sekunda",
                ago={"hours": 15, "minutes": 10, "seconds": 5},
                period="day",
            ),
            param(
                "pśed 5 lětom, pśed 7 mjasecom",
                ago={"years": 5, "months": 7},
                period="month",
            ),
            # ee
            param("ŋkeke 12 si wo va yi", ago={"days": 12}, period="day"),
            param(
                "ƒe 6 si va yi ɣleti 5 si va yi",
                ago={"years": 6, "months": 5},
                period="month",
            ),
            # el
            param(
                "πριν από 5 ώρα 6 λεπτό 7 δευτερόλεπτο",
                ago={"hours": 5, "minutes": 6, "seconds": 7},
                period="day",
            ),
            param("προηγούμενος μήνας", ago={"months": 1}, period="month"),
            # es
            param(
                "hace 5 hora 2 minuto 3 segundo",
                ago={"hours": 5, "minutes": 2, "seconds": 3},
                period="day",
            ),
            # et
            param(
                "5 minut 12 sekundi eest",
                ago={"minutes": 5, "seconds": 12},
                period="day",
            ),
            param(
                "11 aasta 11 kuu eest", ago={"years": 11, "months": 11}, period="month"
            ),
            # eu
            param("duela 3 minutu", ago={"minutes": 3}, period="day"),
            # fil
            param("10 oras ang nakalipas", ago={"hours": 10}, period="day"),
            # fo
            param(
                "3 tími 12 minutt síðan", ago={"hours": 3, "minutes": 12}, period="day"
            ),
            # fur
            param(
                "10 setemane 12 zornade indaûr",
                ago={"weeks": 10, "days": 12},
                period="day",
            ),
            # fy
            param("6 moannen lyn", ago={"months": 6}, period="month"),
            # ga
            param("12 uair an chloig ó shin", ago={"hours": 12}, period="day"),
            # gd
            param("15 mhionaid air ais", ago={"minutes": 15}, period="day"),
            # gl
            param("hai 5 ano 7 mes", ago={"years": 5, "months": 7}, period="month"),
            # gu
            param("5 કલાક પહેલાં", ago={"hours": 5}, period="day"),
            # hr
            param("prije 3 tjedana", ago={"weeks": 3}, period="week"),
            # hsb
            param(
                "před 12 lětom 15 měsac",
                ago={"years": 12, "months": 15},
                period="month",
            ),
            # hy
            param("15 րոպե առաջ", ago={"minutes": 15}, period="day"),
            # id
            param("4 tahun lalu", ago={"years": 4}, period="year"),
            param("4 thn lalu", ago={"years": 4}, period="year"),
            param("4 bulan lalu", ago={"months": 4}, period="month"),
            param("4 bln lalu", ago={"months": 4}, period="month"),
            # is
            param(
                "fyrir 3 ári fyrir 2 mánuði",
                ago={"years": 3, "months": 2},
                period="month",
            ),
            # it
            param("5 settimana fa", ago={"weeks": 5}, period="week"),
            # jgo
            param("ɛ́ gɛ́ mɔ́ pɛsaŋ 3", ago={"months": 3}, period="month"),
            # ka
            param("4 წლის წინ", ago={"years": 4}, period="year"),
            # kk
            param("5 сағат бұрын", ago={"hours": 5}, period="day"),
            # kl
            param("for 6 ulloq unnuarlu siden", ago={"days": 6}, period="day"),
            # km
            param("11 សប្ដាហ៍​មុន", ago={"weeks": 11}, period="week"),
            # kn
            param("15 ಸೆಕೆಂಡುಗಳ ಹಿಂದೆ", ago={"seconds": 15}, period="day"),
            # ko
            param("12개월 전", ago={"months": 12}, period="month"),
            # ksh
            param("vör 15 johre", ago={"years": 15}, period="year"),
            # ky
            param("18 секунд мурун", ago={"seconds": 18}, period="day"),
            # lb
            param("virun 15 stonn", ago={"hours": 15}, period="day"),
            # lkt
            param("hékta 8-čháŋ k'uŋ héhaŋ", ago={"days": 8}, period="day"),
            # lt
            param("prieš 20 minučių", ago={"minutes": 20}, period="day"),
            # lv
            param("pirms 10 gadiem", ago={"years": 10}, period="year"),
            # mk
            param("пред 13 часа", ago={"hours": 13}, period="day"),
            # ml
            param("3 ആഴ്ച മുമ്പ്", ago={"weeks": 3}, period="week"),
            # mn
            param("15 секундын өмнө", ago={"seconds": 15}, period="day"),
            # mr
            param("25 वर्षापूर्वी", ago={"years": 25}, period="year"),
            # ms
            param("10 minit lalu", ago={"minutes": 10}, period="day"),
            # my
            param("ပြီးခဲ့သည့် 15 နှစ်", ago={"years": 15}, period="year"),
            # nb
            param("for 12 måneder siden", ago={"months": 12}, period="month"),
            # ne
            param("23 हप्ता पहिले", ago={"weeks": 23}, period="week"),
            # nl
            param("32 minuten geleden", ago={"minutes": 32}, period="day"),
            # nn
            param("for 15 sekunder siden", ago={"seconds": 15}, period="day"),
            # os
            param("35 сахаты размӕ", ago={"hours": 35}, period="day"),
            # pa-Guru
            param("23 ਹਫ਼ਤੇ ਪਹਿਲਾਂ", ago={"weeks": 23}, period="week"),
            # pa
            param("7 ਸਾਲ ਪਹਿਲਾਂ", ago={"years": 7}, period="year"),
            # ro
            param("acum 56 de secunde", ago={"seconds": 56}, period="day"),
            # sah
            param("2 нэдиэлэ анараа өттүгэр", ago={"weeks": 2}, period="week"),
            # se
            param("8 jahkki árat", ago={"years": 8}, period="year"),
            # si
            param("මිනිත්තු 6කට පෙර", ago={"minutes": 6}, period="day"),
            # sk
            param(
                "pred 20 hodinami 45 min",
                ago={"hours": 20, "minutes": 45},
                period="day",
            ),
            param("dnes", ago={"days": 0}, period="day"),
            param("včera", ago={"days": 1}, period="day"),
            param("predvčerom", ago={"days": 2}, period="day"),
            param("pred 2 hodinami", ago={"hours": 2}, period="day"),
            param("pred rokom", ago={"years": 1}, period="year"),
            param("pred týždňom", ago={"weeks": 1}, period="week"),
            param("pred 3 dňami", ago={"days": 3}, period="day"),
            param("pred hodinou", ago={"hours": 1}, period="day"),
            # sl
            param("pred 15 tednom 10 dan", ago={"weeks": 15, "days": 10}, period="day"),
            # sq
            param("11 minutë më parë", ago={"minutes": 11}, period="day"),
            # sr-Cyrl
            param(
                "пре 8 године 3 месец", ago={"years": 8, "months": 3}, period="month"
            ),
            # sr-Latn
            param("pre 2 nedelje", ago={"weeks": 2}, period="week"),
            # sr
            param("пре 59 секунди", ago={"seconds": 59}, period="day"),
            # sw
            param("mwezi 2 uliopita", ago={"months": 2}, period="month"),
            # ta
            param("41 நிமிடங்களுக்கு முன்", ago={"minutes": 41}, period="day"),
            # te
            param("36 వారాల క్రితం", ago={"weeks": 36}, period="week"),
            # to
            param("houa 'e 7 kuo'osi", ago={"hours": 7}, period="day"),
            # tr
            param("32 dakika önce", ago={"minutes": 32}, period="day"),
            # uk
            param("3 року тому", ago={"years": 3}, period="year"),
            param("5 років тому", ago={"years": 5}, period="year"),
            # uz-Cyrl
            param("10 ҳафта олдин", ago={"weeks": 10}, period="week"),
            # uz-Latn
            param("3 oy oldin", ago={"months": 3}, period="month"),
            # uz
            param("45 soniya oldin", ago={"seconds": 45}, period="day"),
            # vi
            param("23 ngày trước", ago={"days": 23}, period="day"),
            # wae
            param("vor 15 stunde", ago={"hours": 15}, period="day"),
            # yue
            param("5 分鐘前", ago={"minutes": 5}, period="day"),
            # zh-Hans
            param("3周前", ago={"weeks": 3}, period="week"),
            # zh-Hant
            param("2 年前", ago={"years": 2}, period="year"),
            # zu
            param("21 izinsuku ezedlule", ago={"days": 21}, period="day"),
        ]
    )
    def test_relative_past_dates(self, date_string, ago, period):
        self.given_parser(settings={"NORMALIZE": False})
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.then_date_was_parsed_by_freshness_parser()
        self.then_date_obj_is_exactly_this_time_ago(ago)
        self.then_period_is(period)

    @parameterized.expand(
        [
            # English dates
            param("1 decade", ago={"years": 10}, period="year"),
            param("1 decade 2 years", ago={"years": 12}, period="year"),
            param(
                "1 decade 12 months", ago={"years": 10, "months": 12}, period="month"
            ),
            param(
                "1 decade and 11 months",
                ago={"years": 10, "months": 11},
                period="month",
            ),
            param("last decade", ago={"years": 10}, period="year"),
            param("a decade ago", ago={"years": 10}, period="year"),
            param("100 decades", ago={"years": 1000}, period="year"),
            param("yesterday", ago={"days": 1}, period="day"),
            param("the day before yesterday", ago={"days": 2}, period="day"),
            param("10 days before", ago={"days": 10}, period="day"),
            param("today", ago={"days": 0}, period="day"),
            param("till date", ago={"days": 0}, period="day"),
            param("an hour ago", ago={"hours": 1}, period="day"),
            param("about an hour ago", ago={"hours": 1}, period="day"),
            param("a day ago", ago={"days": 1}, period="day"),
            param("a week ago", ago={"weeks": 1}, period="week"),
            param("2 hours ago", ago={"hours": 2}, period="day"),
            param("about 23 hours ago", ago={"hours": 23}, period="day"),
            param("1 year 2 months", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 year, 09 months,01 weeks",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 year 11 months", ago={"years": 1, "months": 11}, period="month"),
            param("1 year 12 months", ago={"years": 1, "months": 12}, period="month"),
            param("15 hr", ago={"hours": 15}, period="day"),
            param("15 hrs", ago={"hours": 15}, period="day"),
            param("2 min", ago={"minutes": 2}, period="day"),
            param("2 mins", ago={"minutes": 2}, period="day"),
            param("3 sec", ago={"seconds": 3}, period="day"),
            param("1000 years ago", ago={"years": 1000}, period="year"),
            param(
                "2013 years ago", ago={"years": 2013}, period="year"
            ),  # We've fixed .now in setUp
            param("5000 months ago", ago={"years": 416, "months": 8}, period="month"),
            param(
                "{} months ago".format(2013 * 12 + 8),
                ago={"years": 2013, "months": 8},
                period="month",
            ),
            param(
                "1 year, 1 month, 1 week, 1 day, 1 hour and 1 minute ago",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            param("just now", ago={"seconds": 0}, period="day"),
            # Fractional units
            param("2.5 hours", ago={"hours": 2.5}, period="day"),
            param("10.75 minutes", ago={"minutes": 10.75}, period="day"),
            param("1.5 days", ago={"days": 1.5}, period="day"),
            # French dates
            param("Aujourd'hui", ago={"days": 0}, period="day"),
            param("Aujourd’hui", ago={"days": 0}, period="day"),
            param("Aujourdʼhui", ago={"days": 0}, period="day"),
            param("Aujourdʻhui", ago={"days": 0}, period="day"),
            param("Aujourd՚hui", ago={"days": 0}, period="day"),
            param("Aujourdꞌhui", ago={"days": 0}, period="day"),
            param("Aujourd＇hui", ago={"days": 0}, period="day"),
            param("Aujourd′hui", ago={"days": 0}, period="day"),
            param("Aujourd‵hui", ago={"days": 0}, period="day"),
            param("Aujourdʹhui", ago={"days": 0}, period="day"),
            param("Aujourd＇hui", ago={"days": 0}, period="day"),
            param("Hier", ago={"days": 1}, period="day"),
            param("Avant-hier", ago={"days": 2}, period="day"),
            param("Il ya un jour", ago={"days": 1}, period="day"),
            param("Il ya une heure", ago={"hours": 1}, period="day"),
            param("Il ya 2 heures", ago={"hours": 2}, period="day"),
            param("Il ya environ 23 heures", ago={"hours": 23}, period="day"),
            param("1 an 2 mois", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 année, 09 mois, 01 semaines",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 an 11 mois", ago={"years": 1, "months": 11}, period="month"),
            param(
                "Il ya 1 an, 1 mois, 1 semaine, 1 jour, 1 heure et 1 minute",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            param("Il y a 40 min", ago={"minutes": 40}, period="day"),
            # German dates
            param("Heute", ago={"days": 0}, period="day"),
            param("Gestern", ago={"days": 1}, period="day"),
            param("vorgestern", ago={"days": 2}, period="day"),
            param("vor einem Tag", ago={"days": 1}, period="day"),
            param("vor einer Stunden", ago={"hours": 1}, period="day"),
            param("Vor 2 Stunden", ago={"hours": 2}, period="day"),
            param("Vor 2 Stunden", ago={"hours": 2}, period="day"),
            param("vor etwa 23 Stunden", ago={"hours": 23}, period="day"),
            param("1 Jahr 2 Monate", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 Jahr, 09 Monate, 01 Wochen",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 Jahr 11 Monate", ago={"years": 1, "months": 11}, period="month"),
            param("vor 29h", ago={"hours": 29}, period="day"),
            param("vor 29m", ago={"minutes": 29}, period="day"),
            param(
                "1 Jahr, 1 Monat, 1 Woche, 1 Tag, 1 Stunde und 1 Minute",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Italian dates
            param("oggi", ago={"days": 0}, period="day"),
            param("ieri", ago={"days": 1}, period="day"),
            param("2 ore fa", ago={"hours": 2}, period="day"),
            param("circa 23 ore fa", ago={"hours": 23}, period="day"),
            param("1 anno 2 mesi", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 anno, 09 mesi, 01 settimane",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 anno 11 mesi", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 anno, 1 mese, 1 settimana, 1 giorno, 1 ora e 1 minuto fa",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Portuguese dates
            param("ontem", ago={"days": 1}, period="day"),
            param("anteontem", ago={"days": 2}, period="day"),
            param("hoje", ago={"days": 0}, period="day"),
            param("uma hora atrás", ago={"hours": 1}, period="day"),
            param("1 segundo atrás", ago={"seconds": 1}, period="day"),
            param("um dia atrás", ago={"days": 1}, period="day"),
            param("uma semana atrás", ago={"weeks": 1}, period="week"),
            param("2 horas atrás", ago={"hours": 2}, period="day"),
            param("cerca de 23 horas atrás", ago={"hours": 23}, period="day"),
            param("1 ano 2 meses", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 ano, 09 meses, 01 semanas",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 ano 11 meses", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 ano, 1 mês, 1 semana, 1 dia, 1 hora e 1 minuto atrás",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Turkish dates
            param("Dün", ago={"days": 1}, period="day"),
            param("Bugün", ago={"days": 0}, period="day"),
            param("2 saat önce", ago={"hours": 2}, period="day"),
            param("yaklaşık 23 saat önce", ago={"hours": 23}, period="day"),
            param("1 yıl 2 ay", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 yıl, 09 ay, 01 hafta",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 yıl 11 ay", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 yıl, 1 ay, 1 hafta, 1 gün, 1 saat ve 1 dakika önce",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Russian dates
            param("сегодня", ago={"days": 0}, period="day"),
            param("Вчера в", ago={"days": 1}, period="day"),
            param("вчера", ago={"days": 1}, period="day"),
            param("2 часа назад", ago={"hours": 2}, period="day"),
            param("час назад", ago={"hours": 1}, period="day"),
            param("минуту назад", ago={"minutes": 1}, period="day"),
            param("2 ч. 21 мин. назад", ago={"hours": 2, "minutes": 21}, period="day"),
            param("около 23 часов назад", ago={"hours": 23}, period="day"),
            param("1 год 2 месяца", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 год, 09 месяцев, 01 недель",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 год 11 месяцев", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1 год, 1 месяц, 1 неделя, 1 день, 1 час и 1 минуту назад",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Czech dates
            param("Dnes", ago={"days": 0}, period="day"),
            param("Včera", ago={"days": 1}, period="day"),
            param("Předevčírem", ago={"days": 2}, period="day"),
            param("Před 2 hodinami", ago={"hours": 2}, period="day"),
            param("před přibližně 23 hodin", ago={"hours": 23}, period="day"),
            param("1 rok 2 měsíce", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 rok, 09 měsíců, 01 týdnů",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 rok 11 měsíců", ago={"years": 1, "months": 11}, period="month"),
            param("3 dny", ago={"days": 3}, period="day"),
            param("3 hodiny", ago={"hours": 3}, period="day"),
            param(
                "1 rok, 1 měsíc, 1 týden, 1 den, 1 hodina, 1 minuta před",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Spanish dates
            param("anteayer", ago={"days": 2}, period="day"),
            param("ayer", ago={"days": 1}, period="day"),
            param("hoy", ago={"days": 0}, period="day"),
            param("hace una hora", ago={"hours": 1}, period="day"),
            param("Hace un día", ago={"days": 1}, period="day"),
            param("Hace una semana", ago={"weeks": 1}, period="week"),
            param("Hace 2 horas", ago={"hours": 2}, period="day"),
            param("Hace cerca de 23 horas", ago={"hours": 23}, period="day"),
            param("1 año 2 meses", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 año, 09 meses, 01 semanas",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 año 11 meses", ago={"years": 1, "months": 11}, period="month"),
            param(
                "Hace 1 año, 1 mes, 1 semana, 1 día, 1 hora y 1 minuto",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Chinese dates
            param("昨天", ago={"days": 1}, period="day"),
            param("前天", ago={"days": 2}, period="day"),
            param("2小时前", ago={"hours": 2}, period="day"),
            param("约23小时前", ago={"hours": 23}, period="day"),
            param("1年2个月", ago={"years": 1, "months": 2}, period="month"),
            param("1年2個月", ago={"years": 1, "months": 2}, period="month"),
            param("1年11个月", ago={"years": 1, "months": 11}, period="month"),
            param("1年11個月", ago={"years": 1, "months": 11}, period="month"),
            param(
                "1年，1月，1周，1天，1小时，1分钟前",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Arabic dates
            param("اليوم", ago={"days": 0}, period="day"),
            param("يوم أمس", ago={"days": 1}, period="day"),
            param("منذ يومين", ago={"days": 2}, period="day"),
            param("منذ 3 أيام", ago={"days": 3}, period="day"),
            param("منذ 21 أيام", ago={"days": 21}, period="day"),
            param(
                "1 عام, 1 شهر, 1 أسبوع, 1 يوم, 1 ساعة, 1 دقيقة",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Thai dates
            param("วันนี้", ago={"days": 0}, period="day"),
            param("เมื่อวานนี้", ago={"days": 1}, period="day"),
            param("2 วัน", ago={"days": 2}, period="day"),
            param("2 ชั่วโมง", ago={"hours": 2}, period="day"),
            param("23 ชม.", ago={"hours": 23}, period="day"),
            param("2 สัปดาห์ 3 วัน", ago={"weeks": 2, "days": 3}, period="day"),
            param(
                "1 ปี 9 เดือน 1 สัปดาห์",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param(
                "1 ปี 1 เดือน 1 สัปดาห์ 1 วัน 1 ชั่วโมง 1 นาที",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Vietnamese dates
            param("Hôm nay", ago={"days": 0}, period="day"),
            param("Hôm qua", ago={"days": 1}, period="day"),
            param("2 tuần 3 ngày", ago={"weeks": 2, "days": 3}, period="day"),
            # Belarusian dates
            param("сёння", ago={"days": 0}, period="day"),
            param("учора ў", ago={"days": 1}, period="day"),
            param("ўчора", ago={"days": 1}, period="day"),
            param("пазаўчора", ago={"days": 2}, period="day"),
            param("2 гадзіны таму назад", ago={"hours": 2}, period="day"),
            param("2 гадзіны таму", ago={"hours": 2}, period="day"),
            param("гадзіну назад", ago={"hours": 1}, period="day"),
            param("хвіліну таму", ago={"minutes": 1}, period="day"),
            param(
                "2 гадзіны 21 хвіл. назад",
                ago={"hours": 2, "minutes": 21},
                period="day",
            ),
            param("каля 23 гадзін назад", ago={"hours": 23}, period="day"),
            param("1 год 2 месяцы", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 год, 09 месяцаў, 01 тыдзень",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("2 гады 3 месяцы", ago={"years": 2, "months": 3}, period="month"),
            param(
                "5 гадоў, 1 месяц, 6 тыдняў, 3 дні, 5 гадзін 1 хвіліну і 3 секунды таму назад",
                ago={
                    "years": 5,
                    "months": 1,
                    "weeks": 6,
                    "days": 3,
                    "hours": 5,
                    "minutes": 1,
                    "seconds": 3,
                },
                period="day",
            ),
            # Polish dates
            param("wczoraj", ago={"days": 1}, period="day"),
            param(
                "1 godz. 2 minuty temu", ago={"hours": 1, "minutes": 2}, period="day"
            ),
            param(
                "2 lata, 3 miesiące, 1 tydzień, 2 dni, 4 godziny, 15 minut i 25 sekund temu",
                ago={
                    "years": 2,
                    "months": 3,
                    "weeks": 1,
                    "days": 2,
                    "hours": 4,
                    "minutes": 15,
                    "seconds": 25,
                },
                period="day",
            ),
            param("2 minuty temu", ago={"minutes": 2}, period="day"),
            param("15 minut temu", ago={"minutes": 15}, period="day"),
            # Bangla dates
            # param('গতকাল', ago={'days': 1}, period='day'),
            # param('আজ', ago={'days': 0}, period='day'),
            param("1 ঘন্টা আগে", ago={"hours": 1}, period="day"),
            param("প্রায় 1 ঘন্টা আগে", ago={"hours": 1}, period="day"),
            param("1 দিন আগে", ago={"days": 1}, period="day"),
            param("1 সপ্তাহ আগে", ago={"weeks": 1}, period="week"),
            param("2 ঘন্টা আগে", ago={"hours": 2}, period="day"),
            param("প্রায় 23 ঘন্টা আগে", ago={"hours": 23}, period="day"),
            param("1 বছর 2 মাস", ago={"years": 1, "months": 2}, period="month"),
            param(
                "1 বছর, 09 মাস,01 সপ্তাহ",
                ago={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param("1 বছর 11 মাস", ago={"years": 1, "months": 11}, period="month"),
            param("1 বছর 12 মাস", ago={"years": 1, "months": 12}, period="month"),
            param("15 ঘন্টা", ago={"hours": 15}, period="day"),
            param("2 মিনিট", ago={"minutes": 2}, period="day"),
            param("3 সেকেন্ড", ago={"seconds": 3}, period="day"),
            param("1000 বছর আগে", ago={"years": 1000}, period="year"),
            param("5000 মাস আগে", ago={"years": 416, "months": 8}, period="month"),
            param(
                "{} মাস আগে".format(2013 * 12 + 8),
                ago={"years": 2013, "months": 8},
                period="month",
            ),
            param(
                "1 বছর, 1 মাস, 1 সপ্তাহ, 1 দিন, 1 ঘন্টা এবং 1 মিনিট আগে",
                ago={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # param('এখন', ago={'seconds': 0}, period='day'),
            # Hindi dates
            param("1 घंटे पहले", ago={"hours": 1}, period="day"),
            param("15 मिनट पहले", ago={"minutes": 15}, period="day"),
            param("25 सेकंड पूर्व", ago={"seconds": 25}, period="day"),
            param(
                "1 वर्ष, 8 महीने, 2 सप्ताह",
                ago={"years": 1, "months": 8, "weeks": 2},
                period="week",
            ),
            param("1 वर्ष 7 महीने", ago={"years": 1, "months": 7}, period="month"),
            param("आज", ago={"days": 0}, period="day"),
            param("1 दशक पहले", ago={"years": 10}, period="year"),
            # af
            param("2 uur gelede", ago={"hours": 2}, period="day"),
            param("verlede maand", ago={"months": 1}, period="month"),
            # agq
            param("ā zūɛɛ", ago={"days": 1}, period="day"),
            # ak
            param("ndeda", ago={"days": 1}, period="day"),
            # am
            param("ከ8 ወራት በፊት", ago={"months": 8}, period="month"),
            param("ያለፈው ሳምንት", ago={"weeks": 1}, period="week"),
            # as
            param("কালি", ago={"days": 1}, period="day"),
            param("আজি", ago={"days": 0}, period="day"),
            # asa
            param("ighuo", ago={"days": 1}, period="day"),
            # ast
            param(
                "hai 2 selmanes hai 3 díes", ago={"weeks": 2, "days": 3}, period="day"
            ),
            param(
                "l'añu pas el mes pasáu", ago={"years": 1, "months": 1}, period="month"
            ),
            # az-Latn
            param(
                "1 il öncə 2 ay öncə 3 həftə öncə",
                ago={"years": 1, "months": 2, "weeks": 3},
                period="week",
            ),
            param(
                "6 saat öncə 5 dəqiqə öncə 4 saniyə öncə",
                ago={"hours": 6, "minutes": 5, "seconds": 4},
                period="day",
            ),
            # az
            param(
                "2 gün öncə 23 saat öncə", ago={"days": 2, "hours": 23}, period="day"
            ),
            param(
                "5 dəqiqə öncə 27 saniyə öncə",
                ago={"minutes": 5, "seconds": 27},
                period="day",
            ),
            # be
            param(
                "2 гадзіны таму 10 хвіліны таму",
                ago={"hours": 2, "minutes": 10},
                period="day",
            ),
            # bg
            param(
                "преди 3 месеца преди 2 седм",
                ago={"months": 3, "weeks": 2},
                period="week",
            ),
            # bn
            param(
                "8 মিনিট আগে 42 সেকেন্ড পূর্বে",
                ago={"minutes": 8, "seconds": 42},
                period="day",
            ),
            # br
            param(
                "7 eur zo 15 min zo 25 s zo",
                ago={"hours": 7, "minutes": 15, "seconds": 25},
                period="day",
            ),
            param("14 sizhun zo 3 deiz zo", ago={"weeks": 14, "days": 3}, period="day"),
            # bs-Cyrl
            param(
                "пре 5 година пре 7 месеци",
                ago={"years": 5, "months": 7},
                period="month",
            ),
            param(
                "пре 5 сати пре 25 секунди",
                ago={"hours": 5, "seconds": 25},
                period="day",
            ),
            # bs-Latn
            param(
                "prije 20 sat 5 minuta", ago={"hours": 20, "minutes": 5}, period="day"
            ),
            param(
                "prije 13 godina prije 3 sed",
                ago={"years": 13, "weeks": 3},
                period="week",
            ),
            # bs
            param(
                "prije 3 mjeseci prije 12 sati",
                ago={"months": 3, "hours": 12},
                period="month",
            ),
            param(
                "prije 3 god 4 mj 5 sed 7 dan",
                ago={"years": 3, "months": 4, "weeks": 5, "days": 7},
                period="day",
            ),
            # ca
            param(
                "fa 4 setmanes fa 5 segon",
                ago={"weeks": 4, "seconds": 5},
                period="week",
            ),
            param(
                "fa 1 hora 2 minut 3 segon",
                ago={"hours": 1, "minutes": 2, "seconds": 3},
                period="day",
            ),
            # ce
            param(
                "10 кӏир хьалха 3 д хьалха", ago={"weeks": 10, "days": 3}, period="day"
            ),
            param(
                "12 сахь 30 мин 30 сек хьалха",
                ago={"hours": 12, "minutes": 30, "seconds": 30},
                period="day",
            ),
            # chr
            param(
                "ᎾᎿ 10 ᏒᎾᏙᏓᏆᏍᏗ ᏥᎨᏒ 5 ᎢᎦ ᏥᎨᏒ", ago={"weeks": 10, "days": 5}, period="day"
            ),
            # cs
            param(
                "před 3 rok 4 měsíc 5 den",
                ago={"years": 3, "months": 4, "days": 5},
                period="day",
            ),
            param(
                "před 2 minutou před 45 sekundou",
                ago={"minutes": 2, "seconds": 45},
                period="day",
            ),
            # cy
            param("5 wythnos yn ôl", ago={"weeks": 5}, period="week"),
            param(
                "7 munud 8 eiliad yn ôl", ago={"minutes": 7, "seconds": 8}, period="day"
            ),
            # dsb
            param(
                "pśed 15 góźinu 10 minuta 5 sekunda",
                ago={"hours": 15, "minutes": 10, "seconds": 5},
                period="day",
            ),
            param(
                "pśed 5 lětom, pśed 7 mjasecom",
                ago={"years": 5, "months": 7},
                period="month",
            ),
            # ee
            param("ŋkeke 12 si wo va yi", ago={"days": 12}, period="day"),
            param(
                "ƒe 6 si va yi ɣleti 5 si va yi",
                ago={"years": 6, "months": 5},
                period="month",
            ),
            # el
            param(
                "πριν από 5 ώρα 6 λεπτό 7 δευτερόλεπτο",
                ago={"hours": 5, "minutes": 6, "seconds": 7},
                period="day",
            ),
            param("προηγούμενος μήνας", ago={"months": 1}, period="month"),
            # es
            param(
                "hace 5 hora 2 minuto 3 segundo",
                ago={"hours": 5, "minutes": 2, "seconds": 3},
                period="day",
            ),
            # et
            param(
                "5 minut 12 sekundi eest",
                ago={"minutes": 5, "seconds": 12},
                period="day",
            ),
            param(
                "11 aasta 11 kuu eest", ago={"years": 11, "months": 11}, period="month"
            ),
            # eu
            param("duela 3 minutu", ago={"minutes": 3}, period="day"),
            # fil
            param("10 oras ang nakalipas", ago={"hours": 10}, period="day"),
            # fo
            param(
                "3 tími 12 minutt síðan", ago={"hours": 3, "minutes": 12}, period="day"
            ),
            # fur
            param(
                "10 setemane 12 zornade indaûr",
                ago={"weeks": 10, "days": 12},
                period="day",
            ),
            # fy
            param("6 moannen lyn", ago={"months": 6}, period="month"),
            # ga
            param("12 uair an chloig ó shin", ago={"hours": 12}, period="day"),
            # gd
            param("15 mhionaid air ais", ago={"minutes": 15}, period="day"),
            # gl
            param("hai 5 ano 7 mes", ago={"years": 5, "months": 7}, period="month"),
            # gu
            param("5 કલાક પહેલાં", ago={"hours": 5}, period="day"),
            # hr
            param("prije 3 tjedana", ago={"weeks": 3}, period="week"),
            # hsb
            param(
                "před 12 lětom 15 měsac",
                ago={"years": 12, "months": 15},
                period="month",
            ),
            # hy
            param("15 րոպե առաջ", ago={"minutes": 15}, period="day"),
            # is
            param(
                "fyrir 3 ári fyrir 2 mánuði",
                ago={"years": 3, "months": 2},
                period="month",
            ),
            # it
            param("5 settimana fa", ago={"weeks": 5}, period="week"),
            # jgo
            param("ɛ́ gɛ́ mɔ́ pɛsaŋ 3", ago={"months": 3}, period="month"),
            # ka
            param("4 წლის წინ", ago={"years": 4}, period="year"),
            # kk
            param("5 сағат бұрын", ago={"hours": 5}, period="day"),
            # kl
            param("for 6 ulloq unnuarlu siden", ago={"days": 6}, period="day"),
            # km
            param("11 សប្ដាហ៍​មុន", ago={"weeks": 11}, period="week"),
            # kn
            param("15 ಸೆಕೆಂಡುಗಳ ಹಿಂದೆ", ago={"seconds": 15}, period="day"),
            # ko
            param("12개월 전", ago={"months": 12}, period="month"),
            # ksh
            param("vör 15 johre", ago={"years": 15}, period="year"),
            # ky
            param("18 секунд мурун", ago={"seconds": 18}, period="day"),
            # lb
            param("virun 15 stonn", ago={"hours": 15}, period="day"),
            # lkt
            param("hékta 8-čháŋ k'uŋ héhaŋ", ago={"days": 8}, period="day"),
            # lt
            param("prieš 20 minučių", ago={"minutes": 20}, period="day"),
            # lv
            param("pirms 10 gadiem", ago={"years": 10}, period="year"),
            # mk
            param("пред 13 часа", ago={"hours": 13}, period="day"),
            # ml
            param("3 ആഴ്ച മുമ്പ്", ago={"weeks": 3}, period="week"),
            # mn
            param("15 секундын өмнө", ago={"seconds": 15}, period="day"),
            # mr
            param("25 वर्षापूर्वी", ago={"years": 25}, period="year"),
            # ms
            param("10 minit lalu", ago={"minutes": 10}, period="day"),
            # my
            param("ပြီးခဲ့သည့် 15 နှစ်", ago={"years": 15}, period="year"),
            # nb
            param("for 12 måneder siden", ago={"months": 12}, period="month"),
            # ne
            param("23 हप्ता पहिले", ago={"weeks": 23}, period="week"),
            # nl
            param("32 minuten geleden", ago={"minutes": 32}, period="day"),
            # nn
            param("for 15 sekunder siden", ago={"seconds": 15}, period="day"),
            # os
            param("35 сахаты размӕ", ago={"hours": 35}, period="day"),
            # pa-Guru
            param("23 ਹਫ਼ਤੇ ਪਹਿਲਾਂ", ago={"weeks": 23}, period="week"),
            # pa
            param("7 ਸਾਲ ਪਹਿਲਾਂ", ago={"years": 7}, period="year"),
            # ro
            param("acum 56 de secunde", ago={"seconds": 56}, period="day"),
            # sah
            param("2 нэдиэлэ анараа өттүгэр", ago={"weeks": 2}, period="week"),
            # se
            param("8 jahkki árat", ago={"years": 8}, period="year"),
            # si
            param("මිනිත්තු 6කට පෙර", ago={"minutes": 6}, period="day"),
            # sk
            param(
                "pred 20 hodinami 45 min",
                ago={"hours": 20, "minutes": 45},
                period="day",
            ),
            # sl
            param("pred 15 tednom 10 dan", ago={"weeks": 15, "days": 10}, period="day"),
            # sq
            param("11 minutë më parë", ago={"minutes": 11}, period="day"),
            # sr-Cyrl
            param(
                "пре 8 године 3 месец", ago={"years": 8, "months": 3}, period="month"
            ),
            # sr-Latn
            param("pre 2 nedelje", ago={"weeks": 2}, period="week"),
            # sr
            param("пре 59 секунди", ago={"seconds": 59}, period="day"),
            # sw
            param("mwezi 2 uliopita", ago={"months": 2}, period="month"),
            # ta
            param("41 நிமிடங்களுக்கு முன்", ago={"minutes": 41}, period="day"),
            # te
            param("36 వారాల క్రితం", ago={"weeks": 36}, period="week"),
            # to
            param("houa 'e 7 kuo'osi", ago={"hours": 7}, period="day"),
            # tr
            param("32 dakika önce", ago={"minutes": 32}, period="day"),
            # uk
            param("3 року тому", ago={"years": 3}, period="year"),
            param("5 років тому", ago={"years": 5}, period="year"),
            # uz-Cyrl
            param("10 ҳафта олдин", ago={"weeks": 10}, period="week"),
            # uz-Latn
            param("3 oy oldin", ago={"months": 3}, period="month"),
            # uz
            param("45 soniya oldin", ago={"seconds": 45}, period="day"),
            # vi
            param("23 ngày trước", ago={"days": 23}, period="day"),
            # wae
            param("vor 15 stunde", ago={"hours": 15}, period="day"),
            # yue
            param("5 分鐘前", ago={"minutes": 5}, period="day"),
            # zh-Hans
            param("3周前", ago={"weeks": 3}, period="week"),
            # zh-Hant
            param("2 年前", ago={"years": 2}, period="year"),
            # zu
            param("21 izinsuku ezedlule", ago={"days": 21}, period="day"),
        ]
    )
    def test_normalized_relative_dates(self, date_string, ago, period):
        date_string = normalize_unicode(date_string)
        self.given_parser(settings={"NORMALIZE": True})
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.then_date_was_parsed_by_freshness_parser()
        self.then_date_obj_is_exactly_this_time_ago(ago)
        self.then_period_is(period)

    @parameterized.expand(
        [
            # English dates
            param(
                "in 1 decade 2 months",
                in_future={"years": 10, "months": 2},
                period="month",
            ),
            param("in 100 decades", in_future={"years": 1000}, period="year"),
            param("in 1 decade 12 years", in_future={"years": 22}, period="year"),
            param("next decade", in_future={"years": 10}, period="year"),
            param("in a decade", in_future={"years": 10}, period="year"),
            param("tomorrow", in_future={"days": 1}, period="day"),
            param("day after tomorrow", in_future={"days": 2}, period="day"),
            param("after 4 days", in_future={"days": 4}, period="day"),
            param("today", in_future={"days": 0}, period="day"),
            param("till date", in_future={"days": 0}, period="day"),
            param("in an hour", in_future={"hours": 1}, period="day"),
            param("in about an hour", in_future={"hours": 1}, period="day"),
            param("in 1 day", in_future={"days": 1}, period="day"),
            param("in 1d", in_future={"days": 1}, period="day"),
            param("in a week", in_future={"weeks": 1}, period="week"),
            param("in three weeks", in_future={"weeks": 3}, period="week"),
            param("in three weeks time", in_future={"weeks": 3}, period="week"),
            param("in three weeks' time", in_future={"weeks": 3}, period="week"),
            param("in 2 hours", in_future={"hours": 2}, period="day"),
            param("in about 23 hours", in_future={"hours": 23}, period="day"),
            param(
                "in 1 year 2 months",
                in_future={"years": 1, "months": 2},
                period="month",
            ),
            param(
                "in 1 year, 09 months,01 weeks",
                in_future={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param(
                "in 1 year 11 months",
                in_future={"years": 1, "months": 11},
                period="month",
            ),
            param(
                "in 1 year 12 months",
                in_future={"years": 1, "months": 12},
                period="month",
            ),
            param("in 15 hr", in_future={"hours": 15}, period="day"),
            param("in 15 hrs", in_future={"hours": 15}, period="day"),
            param("in 2 min", in_future={"minutes": 2}, period="day"),
            param("in 2 mins", in_future={"minutes": 2}, period="day"),
            param("in 3 sec", in_future={"seconds": 3}, period="day"),
            param("in 1000 years", in_future={"years": 1000}, period="year"),
            param(
                "in 5000 months", in_future={"years": 416, "months": 8}, period="month"
            ),
            param(
                "in {} months".format(2013 * 12 + 8),
                in_future={"years": 2013, "months": 8},
                period="month",
            ),
            param(
                "in 1 year, 1 month, 1 week, 1 day, 1 hour and 1 minute",
                in_future={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            param("just now", in_future={"seconds": 0}, period="day"),
            param("1 decade later", in_future={"years": 10}, period="year"),
            param("2 years later", in_future={"years": 2}, period="year"),
            param("3 months later", in_future={"months": 3}, period="month"),
            param("1 week later", in_future={"weeks": 1}, period="week"),
            param("2 days later", in_future={"days": 2}, period="day"),
            param("3 hours later", in_future={"hours": 3}, period="day"),
            param("4 minutes later", in_future={"minutes": 4}, period="day"),
            param("5 seconds later", in_future={"seconds": 5}, period="day"),
            # Fractional units
            param("in 2.5 hours", in_future={"hours": 2.5}, period="day"),
            param("in 10.75 minutes", in_future={"minutes": 10.75}, period="day"),
            param("in 1.5 days", in_future={"days": 1.5}, period="day"),
            param("in 0,5 hours", in_future={"hours": 0.5}, period="day"),
            param("1.5 days later", in_future={"days": 1.5}, period="day"),
            param("0,5 hours later", in_future={"hours": 0.5}, period="day"),
            # French dates
            param("Aujourd'hui", in_future={"days": 0}, period="day"),
            param("Dans un jour", in_future={"days": 1}, period="day"),
            param("Dans une heure", in_future={"hours": 1}, period="day"),
            param("En 2 heures", in_future={"hours": 2}, period="day"),
            param("Dans environ 23 heures", in_future={"hours": 23}, period="day"),
            param(
                "Dans 1 an 2 mois", in_future={"years": 1, "months": 2}, period="month"
            ),
            param(
                "En 1 année, 09 mois, 01 semaines",
                in_future={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param(
                "Dans 1 an 11 mois",
                in_future={"years": 1, "months": 11},
                period="month",
            ),
            param(
                "Dans 1 année, 1 mois, 1 semaine, 1 jour, 1 heure et 1 minute",
                in_future={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            param("Dans 40 min", in_future={"minutes": 40}, period="day"),
            # German dates
            param("Heute", in_future={"days": 0}, period="day"),
            param("Morgen", in_future={"days": 1}, period="day"),
            param("in einem Tag", in_future={"days": 1}, period="day"),
            param("in einer Stunde", in_future={"hours": 1}, period="day"),
            param("in 2 Stunden", in_future={"hours": 2}, period="day"),
            param("in etwa 23 Stunden", in_future={"hours": 23}, period="day"),
            param(
                "im 1 Jahr 2 Monate",
                in_future={"years": 1, "months": 2},
                period="month",
            ),
            param(
                "im 1 Jahr, 09 Monate, 01 Wochen",
                in_future={"years": 1, "months": 9, "weeks": 1},
                period="week",
            ),
            param(
                "im 1 Jahr 11 Monate",
                in_future={"years": 1, "months": 11},
                period="month",
            ),
            param(
                "im 1 Jahr, 1 Monat, 1 Woche, 1 Tag, 1 Stunde und 1 Minute",
                in_future={
                    "years": 1,
                    "months": 1,
                    "weeks": 1,
                    "days": 1,
                    "hours": 1,
                    "minutes": 1,
                },
                period="day",
            ),
            # Polish dates
            param("jutro", in_future={"days": 1}, period="day"),
            param("pojutrze", in_future={"days": 2}, period="day"),
            param(
                "za 2 lata, 3 miesiące, 1 tydzień, 2 dni, 4 godziny, 15 minut i 25 sekund",
                in_future={
                    "years": 2,
                    "months": 3,
                    "weeks": 1,
                    "days": 2,
                    "hours": 4,
                    "minutes": 15,
                    "seconds": 25,
                },
                period="day",
            ),
            param("za 2 minuty", in_future={"minutes": 2}, period="day"),
            param("za 15 minut", in_future={"minutes": 15}, period="day"),
            # Turkish dates
            param("yarın", in_future={"days": 1}, period="day"),
            param("2 gün içerisinde", in_future={"days": 2}, period="day"),
            param("4 ay içerisinde", in_future={"months": 4}, period="month"),
            param("3 gün sonra", in_future={"days": 3}, period="day"),
            param("2 ay sonra", in_future={"months": 2}, period="month"),
            param("5 yıl 3 gün sonra", in_future={"years": 5, "days": 3}, period="day"),
            param("5 gün içinde", in_future={"days": 5}, period="day"),
            param("6 ay içinde", in_future={"months": 6}, period="month"),
            param("5 yıl içinde", in_future={"years": 5}, period="year"),
            param("5 sene içinde", in_future={"years": 5}, period="year"),
            param("haftaya", in_future={"weeks": 1}, period="week"),
            param("gelecek hafta", in_future={"weeks": 1}, period="week"),
            param("gelecek ay", in_future={"months": 1}, period="month"),
            param("gelecek yıl", in_future={"years": 1}, period="year"),
            # Hindi dates
            param(
                "1 वर्ष 10 महीने में",
                in_future={"years": 1, "months": 10},
                period="month",
            ),
            param("15 घंटे बाद", in_future={"hours": 15}, period="day"),
            param("2 मिनट में", in_future={"minutes": 2}, period="day"),
            param("17 सेकंड बाद", in_future={"seconds": 17}, period="day"),
            param(
                "1 वर्ष, 5 महीने, 1 सप्ताह में",
                in_future={"years": 1, "months": 5, "weeks": 1},
                period="week",
            ),
            param("1 दशक में", in_future={"years": 10}, period="year"),
            # af
            param("oor 10 jaar", in_future={"years": 10}, period="year"),
            param(
                "oor 5 min 3 sek", in_future={"minutes": 5, "seconds": 3}, period="day"
            ),
            # am
            param("በ2 ሳምንታት ውስጥ", in_future={"weeks": 2}, period="week"),
            param("በ16 ቀናት ውስጥ", in_future={"days": 16}, period="day"),
            # ast
            param("en 15 años", in_future={"years": 15}, period="year"),
            param("en 20 minutos", in_future={"minutes": 20}, period="day"),
            # az-Latn
            param("5 saniyə ərzində", in_future={"seconds": 5}, period="day"),
            param(
                "10 saat 20 dəqiqə ərzində",
                in_future={"hours": 10, "minutes": 20},
                period="day",
            ),
            # az
            param(
                "15 il 6 ay ərzində",
                in_future={"years": 15, "months": 6},
                period="month",
            ),
            # be
            param(
                "праз 5 гадзіны 6 хвіліны",
                in_future={"hours": 5, "minutes": 6},
                period="day",
            ),
            # bg
            param(
                "след 12 мин 18 сек",
                in_future={"minutes": 12, "seconds": 18},
                period="day",
            ),
            # bn
            param("10 সেকেন্ডে", in_future={"seconds": 10}, period="day"),
            # br
            param("a-benn 20 vloaz", in_future={"years": 20}, period="year"),
            param(
                "a-benn 15 deiz 20 eur",
                in_future={"days": 15, "hours": 20},
                period="day",
            ),
            # bs-Cyrl
            param(
                "за 5 минут 10 секунд",
                in_future={"minutes": 5, "seconds": 10},
                period="day",
            ),
            param(
                "за 10 годину 11 месец",
                in_future={"years": 10, "months": 11},
                period="month",
            ),
            # bs-Latn
            param("za 7 mjeseci", in_future={"months": 7}, period="month"),
            param("za 6 dan 23 sat", in_future={"days": 6, "hours": 23}, period="day"),
            # bs
            param("za 15 sedmica", in_future={"weeks": 15}, period="week"),
            # ca
            param("d'aquí a 10 anys", in_future={"years": 10}, period="year"),
            param(
                "d'aquí a 15 minut 53 segon",
                in_future={"minutes": 15, "seconds": 53},
                period="day",
            ),
            # ce
            param("20 кӏира даьлча", in_future={"weeks": 20}, period="week"),
            param(
                "10 минот 25 секунд яьлча",
                in_future={"minutes": 10, "seconds": 25},
                period="day",
            ),
            # chr
            param("ᎾᎿ 10 ᎧᎸᎢ", in_future={"months": 10}, period="month"),
            param("ᎾᎿ 24 ᎢᏳᏟᎶᏓ", in_future={"hours": 24}, period="day"),
            # cs
            param("za 12 rok", in_future={"years": 12}, period="year"),
            param(
                "za 10 den 5 hodin", in_future={"days": 10, "hours": 5}, period="day"
            ),
            # cy
            param("ymhen 15 mis", in_future={"months": 15}, period="month"),
            param(
                "ymhen 10 munud 8 eiliad",
                in_future={"minutes": 10, "seconds": 8},
                period="day",
            ),
            # da
            param(
                "om 10 minut 54 sekund",
                in_future={"minutes": 10, "seconds": 54},
                period="day",
            ),
            # de
            param(
                "in 15 jahren 10 monat",
                in_future={"years": 15, "months": 10},
                period="month",
            ),
            # dsb
            param("za 10 mjasec", in_future={"months": 10}, period="month"),
            param(
                "za 30 min 50 sek",
                in_future={"minutes": 30, "seconds": 50},
                period="day",
            ),
            # dz
            param("ལོ་འཁོར་ 4 ནང་", in_future={"years": 4}, period="year"),
            param("སྐར་ཆ་ 20 ནང་", in_future={"seconds": 20}, period="day"),
            # ee
            param("le ƒe 15 si gbɔna me", in_future={"years": 15}, period="year"),
            param("le ŋkeke 2 wo me", in_future={"days": 2}, period="day"),
            # el
            param("σε 5 ώρες", in_future={"hours": 5}, period="day"),
            param(
                "σε 4 λεπτό 45 δευτ",
                in_future={"minutes": 4, "seconds": 45},
                period="day",
            ),
            # et
            param(
                "5 aasta 10 kuu pärast",
                in_future={"years": 5, "months": 10},
                period="month",
            ),
            param("10 nädala pärast", in_future={"weeks": 10}, period="week"),
            # eu
            param("15 hilabete barru", in_future={"months": 15}, period="month"),
            param("20 egun barru", in_future={"days": 20}, period="day"),
            # fil
            param("sa 8 segundo", in_future={"seconds": 8}, period="day"),
            param(
                "sa 2 oras 24 min", in_future={"hours": 2, "minutes": 24}, period="day"
            ),
            # fo
            param("um 12 mánaðir", in_future={"months": 12}, period="month"),
            param("um 10 tímar", in_future={"hours": 10}, period="day"),
            # fur
            param("ca di 15 setemanis", in_future={"weeks": 15}, period="week"),
            param(
                "ca di 15 minût 20 secont",
                in_future={"minutes": 15, "seconds": 20},
                period="day",
            ),
            # fy
            param("oer 10 jier", in_future={"years": 10}, period="year"),
            param("oer 22 deien", in_future={"days": 22}, period="day"),
            # ga
            param("i gceann 23 bliain", in_future={"years": 23}, period="year"),
            param("i gceann 12 scht", in_future={"weeks": 12}, period="week"),
            # gd
            param("an ceann 10 bliadhna", in_future={"years": 10}, period="year"),
            param("an ceann 18 latha", in_future={"days": 18}, period="day"),
            # gl
            param(
                "en 5 anos 26 mes", in_future={"years": 5, "months": 26}, period="month"
            ),
            param("en 14 semanas", in_future={"weeks": 14}, period="week"),
            # gu
            param("10 મહિનામાં", in_future={"months": 10}, period="month"),
            param("8 કલાકમાં", in_future={"hours": 8}, period="day"),
            # hr
            param("za 12 dana", in_future={"days": 12}, period="day"),
            param(
                "za 10 sat 43 min", in_future={"hours": 10, "minutes": 43}, period="day"
            ),
            # hsb
            param("za 6 měsacow", in_future={"months": 6}, period="month"),
            param(
                "za 1 dźeń 12 hodź", in_future={"days": 1, "hours": 12}, period="day"
            ),
            # hy
            param("7 ր-ից", in_future={"minutes": 7}, period="day"),
            param("51 շաբաթից", in_future={"weeks": 51}, period="week"),
            # id
            param("dalam 12 detik", in_future={"seconds": 12}, period="day"),
            param("dalam 10 hari", in_future={"days": 10}, period="day"),
            # is
            param("eftir 11 mínútur", in_future={"minutes": 11}, period="day"),
            param("eftir 12 klukkustundir", in_future={"hours": 12}, period="day"),
            # it
            param("tra 5 minuto", in_future={"minutes": 5}, period="day"),
            param("tra 16 settimane", in_future={"weeks": 16}, period="week"),
            # jgo
            # param("nǔu ŋgu' 10", in_future={'years': 10}, period='year'),
            param("nǔu ŋgap-mbi 11", in_future={"weeks": 11}, period="week"),
            # ka
            param("5 საათში", in_future={"hours": 5}, period="day"),
            param("3 კვირაში", in_future={"weeks": 3}, period="week"),
            # kea
            param("di li 10 anu", in_future={"years": 10}, period="year"),
            param("di li 43 minutu", in_future={"minutes": 43}, period="day"),
            # kk
            param("10 сағаттан кейін", in_future={"hours": 10}, period="day"),
            param("18 айдан кейін", in_future={"months": 18}, period="month"),
            # kl
            param("om 15 sapaatip-akunnera", in_future={"weeks": 15}, period="week"),
            param(
                "om 23 nalunaaquttap-akunnera", in_future={"hours": 23}, period="day"
            ),
            # km
            param("2 នាទីទៀត", in_future={"minutes": 2}, period="day"),
            param("5 សប្ដាហ៍ទៀត", in_future={"weeks": 5}, period="week"),
            # kn
            param("10 ವಾರದಲ್ಲಿ", in_future={"weeks": 10}, period="week"),
            param("15 ನಿಮಿಷಗಳಲ್ಲಿ", in_future={"minutes": 15}, period="day"),
            # ko
            param("5초 후", in_future={"seconds": 5}, period="day"),
            param("7개월 후", in_future={"months": 7}, period="month"),
            # ksh
            param("en 8 johre", in_future={"years": 8}, period="year"),
            # ky
            param("15 мүнөттөн кийин", in_future={"minutes": 15}, period="day"),
            param("11 айд кийин", in_future={"months": 11}, period="month"),
            # lb
            param("an 30 dag", in_future={"days": 30}, period="day"),
            param(
                "an 10 minutt 15 sekonn",
                in_future={"minutes": 10, "seconds": 15},
                period="day",
            ),
            # lkt
            param("letáŋhaŋ okó 20 kiŋháŋ", in_future={"weeks": 20}, period="week"),
            param("letáŋhaŋ ómakȟa 11 kiŋháŋ", in_future={"years": 11}, period="year"),
            # lo
            param("ໃນອີກ 25 ຊົ່ວໂມງ", in_future={"hours": 25}, period="day"),
            param("ໃນອີກ 13 ອາທິດ", in_future={"weeks": 13}, period="week"),
            # lt
            param("po 7 valandos", in_future={"hours": 7}, period="day"),
            param(
                "po 5 min 5 sek", in_future={"minutes": 5, "seconds": 5}, period="day"
            ),
            # lv
            param("pēc 15 sekundēm", in_future={"seconds": 15}, period="day"),
            param("pēc 10 mēneša", in_future={"months": 10}, period="month"),
            # mk
            param("за 16 седмици", in_future={"weeks": 16}, period="week"),
            param("за 2 месеци", in_future={"months": 2}, period="month"),
            # ml
            param("5 ആഴ്ചയിൽ", in_future={"weeks": 5}, period="week"),
            param("8 മിനിറ്റിൽ", in_future={"minutes": 8}, period="day"),
            # mn
            param("10 сарын дараа", in_future={"months": 10}, period="month"),
            param("15 цагийн дараа", in_future={"hours": 15}, period="day"),
            # mr
            param("2 महिन्यांमध्ये", in_future={"months": 2}, period="month"),
            param("15 मिनि मध्ये", in_future={"minutes": 15}, period="day"),
            # ms
            param("dalam 6 jam", in_future={"hours": 6}, period="day"),
            param("dalam 11 thn", in_future={"years": 11}, period="year"),
            # my
            param("12 လအတွင်း", in_future={"months": 12}, period="month"),
            param("8 နာရီအတွင်း", in_future={"hours": 8}, period="day"),
            # nb
            param("om 1 måneder", in_future={"months": 1}, period="month"),
            param("om 5 minutter", in_future={"minutes": 5}, period="day"),
            # ne
            param("10 वर्षमा", in_future={"years": 10}, period="year"),
            param("15 घण्टामा", in_future={"hours": 15}, period="day"),
            # nl
            param("over 3 weken", in_future={"weeks": 3}, period="week"),
            param("over 12 seconden", in_future={"seconds": 12}, period="day"),
            # nn
            param("om 7 uker", in_future={"weeks": 7}, period="week"),
            param("om 2 timer", in_future={"hours": 2}, period="day"),
            # os
            param("10 сахаты фӕстӕ", in_future={"hours": 10}, period="day"),
            # pa-Guru
            param("3 ਸਾਲਾਂ ਵਿੱਚ", in_future={"years": 3}, period="year"),
            param("7 ਦਿਨਾਂ ਵਿੱਚ", in_future={"days": 7}, period="day"),
            # pa
            param("8 ਘੰਟਿਆਂ ਵਿੱਚ", in_future={"hours": 8}, period="day"),
            param("16 ਸਕਿੰਟ ਵਿੱਚ", in_future={"seconds": 16}, period="day"),
            # pl
            param("za 12 sekundy", in_future={"seconds": 12}, period="day"),
            param("za 22 tygodnia", in_future={"weeks": 22}, period="week"),
            # pt
            param("dentro de 11 minuto", in_future={"minutes": 11}, period="day"),
            param("dentro de 8 meses", in_future={"months": 8}, period="month"),
            # ro
            param("peste 12 de săptămâni", in_future={"weeks": 12}, period="week"),
            param("peste 6 de ore", in_future={"hours": 6}, period="day"),
            # sah
            param("15 нэдиэлэннэн", in_future={"weeks": 15}, period="week"),
            param("12 мүнүүтэннэн", in_future={"minutes": 12}, period="day"),
            # se
            param("3 mánotbadji maŋŋilit", in_future={"months": 3}, period="month"),
            param("10 sekunda maŋŋilit", in_future={"seconds": 10}, period="day"),
            # si
            param("මිනිත්තු 10කින්", in_future={"minutes": 10}, period="day"),
            param("දින 3න්", in_future={"days": 3}, period="day"),
            # sk
            param("o 23 týždňov", in_future={"weeks": 23}, period="week"),
            # sl
            param("čez 7 leto", in_future={"years": 7}, period="year"),
            param(
                "čez 8 minut 22 sek",
                in_future={"minutes": 8, "seconds": 22},
                period="day",
            ),
            # sq
            param("pas 2 muajsh", in_future={"months": 2}, period="month"),
            param("pas 15 ditësh", in_future={"days": 15}, period="day"),
            # sr-Cyrl
            param(
                "за 10 мин 20 сек",
                in_future={"minutes": 10, "seconds": 20},
                period="day",
            ),
            # sr-Latn
            param(
                "za 2 god 6 mes", in_future={"years": 2, "months": 6}, period="month"
            ),
            param("za 14 nedelja", in_future={"weeks": 14}, period="week"),
            # sr
            param("за 18 недеља", in_future={"weeks": 18}, period="week"),
            param("за 5 месеци", in_future={"months": 5}, period="month"),
            # sv
            param("om 7 veckor", in_future={"weeks": 7}, period="week"),
            param("om 10 timmar", in_future={"hours": 10}, period="day"),
            # sw
            param("baada ya saa 21", in_future={"hours": 21}, period="day"),
            param("baada ya sekunde 16", in_future={"seconds": 16}, period="day"),
            # ta
            param("4 மாதங்களில்", in_future={"months": 4}, period="month"),
            param("14 நாட்களில்", in_future={"days": 14}, period="day"),
            # te
            param("3 వారాల్లో", in_future={"weeks": 3}, period="week"),
            param("15 గంలో", in_future={"hours": 15}, period="day"),
            # th
            param("ในอีก 6 นาที", in_future={"minutes": 6}, period="day"),
            param("ในอีก 3 ปี", in_future={"years": 3}, period="year"),
            # to
            param("'i he māhina 'e 5", in_future={"months": 5}, period="month"),
            param("'i he houa 'e 11", in_future={"hours": 11}, period="day"),
            # tr
            param("15 saniye sonra", in_future={"seconds": 15}, period="day"),
            param(
                "45 saat 234 dakika sonra",
                in_future={"hours": 45, "minutes": 234},
                period="day",
            ),
            # uk
            param("через 8 хвилини", in_future={"minutes": 8}, period="day"),
            param("через 10 тижня", in_future={"weeks": 10}, period="week"),
            param("через 10 днів", in_future={"days": 10}, period="day"),
            # uz-Cyrl
            param("12 кундан сўнг", in_future={"days": 12}, period="day"),
            param("10 дақиқадан сўнг", in_future={"minutes": 10}, period="day"),
            # uz-Latn
            param("3 yildan keyin", in_future={"years": 3}, period="year"),
            param("5 haftadan keyin", in_future={"weeks": 5}, period="week"),
            # uz
            param("12 kundan keyin", in_future={"days": 12}, period="day"),
            param("50 daqiqadan keyin", in_future={"minutes": 50}, period="day"),
            # vi
            param("sau 5 năm nữa", in_future={"years": 5}, period="year"),
            param("sau 2 phút nữa", in_future={"minutes": 2}, period="day"),
            # wae
            param("i 3 stunde", in_future={"hours": 3}, period="day"),
            param("i 5 täg", in_future={"days": 5}, period="day"),
            # yue
            param("3 個星期後", in_future={"weeks": 3}, period="week"),
            param("6 年後", in_future={"years": 6}, period="year"),
            # zh-Hans
            param("5个月后", in_future={"months": 5}, period="month"),
            param("7天后", in_future={"days": 7}, period="day"),
            # zh-Hant
            param("2 分鐘後", in_future={"minutes": 2}, period="day"),
            param("4 週後", in_future={"weeks": 4}, period="week"),
        ]
    )
    def test_relative_future_dates(self, date_string, in_future, period):
        self.given_parser()
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.then_date_was_parsed_by_freshness_parser()
        self.then_date_obj_is_exactly_this_time_in_future(in_future)
        self.then_period_is(period)

    @parameterized.expand(
        [
            param("15th of Aug, 2014 Diane Bennett"),
            param("4 heures ago"),
        ]
    )
    def test_insane_dates(self, date_string):
        self.given_parser()
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.then_date_was_not_parsed()

    @parameterized.expand(
        [
            param("5000 years ago"),
            param("2014 years ago"),  # We've fixed .now in setUp
            param("{} months ago".format(2013 * 12 + 9)),
            param("123456789 hour"),
            param("123456789123 hour"),
            param("1234567 days"),
            param("1234567891 days"),
            param("12345678912 days"),
            param("123455678976543 month"),
        ]
    )
    def test_dates_not_supported_by_date_time(self, date_string):
        self.given_parser()
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.assertEqual(None, self.result["date_obj"])

    @parameterized.expand(
        [
            param("1mon ago"),  # 1116
        ]
    )
    def test_known_issues(self, date_string):
        self.given_parser()
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.assertEqual(None, self.result["date_obj"])

    @parameterized.expand(
        [
            param("несколько секунд назад", boundary={"seconds": 45}, period="day"),
            param("há alguns segundos", boundary={"seconds": 45}, period="day"),
        ]
    )
    def test_inexplicit_dates(self, date_string, boundary, period):
        self.given_parser()
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_error_was_not_raised()
        self.then_date_was_parsed_by_freshness_parser()
        self.then_period_is(period)
        self.then_date_obj_is_between(self.now - timedelta(**boundary), self.now)

    @parameterized.expand(
        [
            param("Today at 9 pm", date(2014, 9, 1), time(21, 0)),
            param("Today at 11:20 am", date(2014, 9, 1), time(11, 20)),
            param("Yesterday 1:20 pm", date(2014, 8, 31), time(13, 20)),
            param("Yesterday by 13:20", date(2014, 8, 31), time(13, 20)),
            param("the day before yesterday 16:50", date(2014, 8, 30), time(16, 50)),
            param("2 Tage 18:50", date(2014, 8, 30), time(18, 50)),
            param("1 day ago at 2 PM", date(2014, 8, 31), time(14, 0)),
            param("one day ago at 2 PM", date(2014, 8, 31), time(14, 0)),
            param("Dnes v 12:40", date(2014, 9, 1), time(12, 40)),
            param("1 week ago at 12:00 am", date(2014, 8, 25), time(0, 0)),
            param("one week ago at 12:00 am", date(2014, 8, 25), time(0, 0)),
            param("tomorrow at 2 PM", date(2014, 9, 2), time(14, 0)),
        ]
    )
    def test_freshness_date_with_time(self, date_string, date, time):
        self.given_parser()
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    def test_freshness_date_with_time_and_timezone(self):
        self.given_parser(settings={"TIMEZONE": "local"})
        self.given_date_string("tomorrow 8:30 CST")
        self.when_date_is_parsed()
        self.then_date_is(date(2014, 9, 2))
        self.then_time_is(time(8, 30))
        self.then_timezone_is("CST")

    @parameterized.expand(
        [
            param("2 hours ago", "Asia/Karachi", date(2014, 9, 1), time(13, 30)),
            param("3 hours ago", "Europe/Paris", date(2014, 9, 1), time(9, 30)),
            param(
                "5 hours ago", "US/Eastern", date(2014, 9, 1), time(1, 30)
            ),  # date in DST range
            param("Today at 9 pm", "Asia/Karachi", date(2014, 9, 1), time(21, 0)),
        ]
    )
    def test_freshness_date_with_pytz_timezones(
        self, date_string, timezone, date, time
    ):
        self.given_parser(settings={"TIMEZONE": timezone})
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    @parameterized.expand(
        [
            param(
                "Today at 4:25 pm", "US/Mountain", "UTC", date(2014, 9, 1), time(22, 25)
            ),
            param(
                "Yesterday at 4:25 pm",
                "US/Mountain",
                "UTC",
                date(2014, 8, 31),
                time(22, 25),
            ),
            param("Yesterday", "US/Mountain", "UTC", date(2014, 8, 31), time(16, 30)),
            param("Today", "US/Mountain", "UTC", date(2014, 9, 1), time(16, 30)),
        ]
    )
    def test_freshness_date_with_timezone_conversion(
        self, date_string, timezone, to_timezone, date, time
    ):
        self.given_parser(
            settings={
                "TIMEZONE": timezone,
                "TO_TIMEZONE": to_timezone,
                "RELATIVE_BASE": datetime(2014, 9, 1, 10, 30),
            }
        )
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    def test_freshness_date_with_to_timezone_setting(self):
        _settings = settings.replace(
            **{
                "TIMEZONE": "local",
                "TO_TIMEZONE": "UTC",
                "RELATIVE_BASE": datetime(2014, 9, 1, 10, 30),
            }
        )

        parser = dateparser.freshness_date_parser.FreshnessDateDataParser()
        timezone = (
            ZoneInfo(key="US/Eastern") if ZoneInfo else pytz.timezone("US/Eastern")
        )
        parser.get_local_tz = Mock(return_value=timezone)
        result = parser.get_date_data("1 minute ago", _settings)
        result = result["date_obj"]
        self.assertEqual(result.date(), date(2014, 9, 1))
        self.assertEqual(result.time(), time(14, 29))

    @parameterized.expand(
        [
            param("2 hours ago", "PKT", date(2014, 9, 1), time(13, 30)),
            param("5 hours ago", "EST", date(2014, 9, 1), time(0, 30)),
            param("3 hours ago", "MET", date(2014, 9, 1), time(8, 30)),
        ]
    )
    def test_freshness_date_with_timezone_abbreviations(
        self, date_string, timezone, date, time
    ):
        self.given_parser(settings={"TIMEZONE": timezone})
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    @parameterized.expand(
        [
            param("2 hours ago", "+05:00", date(2014, 9, 1), time(13, 30)),
            param("5 hours ago", "-05:00", date(2014, 9, 1), time(0, 30)),
            param("3 hours ago", "+01:00", date(2014, 9, 1), time(8, 30)),
        ]
    )
    def test_freshness_date_with_timezone_utc_offset(
        self, date_string, timezone, date, time
    ):
        self.given_parser(settings={"TIMEZONE": timezone})
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    @parameterized.expand(
        [
            param("Today at 9 pm", date(2010, 6, 4), time(21, 0)),
            param("Today at 11:20 am", date(2010, 6, 4), time(11, 20)),
            param("Yesterday 1:20 pm", date(2010, 6, 3), time(13, 20)),
            param("the day before yesterday 16:50", date(2010, 6, 2), time(16, 50)),
            param("2 Tage 18:50", date(2010, 6, 2), time(18, 50)),
            param("1 day ago at 2 PM", date(2010, 6, 3), time(14, 0)),
            param("Dnes v 12:40", date(2010, 6, 4), time(12, 40)),
            param("1 week ago at 12:00 am", date(2010, 5, 28), time(0, 0)),
            param("yesterday", date(2010, 6, 3), time(13, 15)),
            param("the day before yesterday", date(2010, 6, 2), time(13, 15)),
            param("today", date(2010, 6, 4), time(13, 15)),
            param("an hour ago", date(2010, 6, 4), time(12, 15)),
            param("about an hour ago", date(2010, 6, 4), time(12, 15)),
            param("a day ago", date(2010, 6, 3), time(13, 15)),
            param("a week ago", date(2010, 5, 28), time(13, 15)),
            param("2 hours ago", date(2010, 6, 4), time(11, 15)),
            param("about 23 hours ago", date(2010, 6, 3), time(14, 15)),
            param("1 year 2 months", date(2009, 4, 4), time(13, 15)),
            param("1 year, 09 months,01 weeks", date(2008, 8, 28), time(13, 15)),
            param("1 year 11 months", date(2008, 7, 4), time(13, 15)),
            param("1 year 12 months", date(2008, 6, 4), time(13, 15)),
            param("15 hr", date(2010, 6, 3), time(22, 15)),
            param("15 hrs", date(2010, 6, 3), time(22, 15)),
            param("2 min", date(2010, 6, 4), time(13, 13)),
            param("2 mins", date(2010, 6, 4), time(13, 13)),
            param("3 sec", date(2010, 6, 4), time(13, 14, 57)),
            param("1000 years ago", date(1010, 6, 4), time(13, 15)),
            param("2008 years ago", date(2, 6, 4), time(13, 15)),
            param("5000 months ago", date(1593, 10, 4), time(13, 15)),
            param("{} months ago".format(2008 * 12 + 8), date(1, 10, 4), time(13, 15)),
            param(
                "1 year, 1 month, 1 week, 1 day, 1 hour and 1 minute ago",
                date(2009, 4, 26),
                time(12, 14),
            ),
            param("just now", date(2010, 6, 4), time(13, 15)),
            # Fractional units
            param("2.5 hours ago", date(2010, 6, 4), time(10, 45)),
            param("in 10.75 minutes", date(2010, 6, 4), time(13, 25, 45)),
            param("in 1.5 days", date(2010, 6, 6), time(1, 15)),
            param("0,5 hours ago", date(2010, 6, 4), time(12, 45)),
        ]
    )
    def test_freshness_date_with_relative_base(self, date_string, date, time):
        self.given_parser(settings={"RELATIVE_BASE": datetime(2010, 6, 4, 13, 15)})
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    @parameterized.expand(
        [
            param("3 days", date(2010, 6, 1), time(13, 15)),
            param("2 years", date(2008, 6, 4), time(13, 15)),
        ]
    )
    def test_freshness_date_with_relative_base_past(self, date_string, date, time):
        self.given_parser(
            settings={
                "PREFER_DATES_FROM": "past",
                "RELATIVE_BASE": datetime(2010, 6, 4, 13, 15),
            }
        )
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    @parameterized.expand(
        [
            param("3 days", date(2010, 6, 7), time(13, 15)),
            param("2 years", date(2012, 6, 4), time(13, 15)),
        ]
    )
    def test_freshness_date_with_relative_base_future(self, date_string, date, time):
        self.given_parser(
            settings={
                "PREFER_DATES_FROM": "future",
                "RELATIVE_BASE": datetime(2010, 6, 4, 13, 15),
            }
        )
        self.given_date_string(date_string)
        self.when_date_is_parsed()
        self.then_date_is(date)
        self.then_time_is(time)

    def given_date_string(self, date_string):
        self.date_string = date_string

    def given_parser(self, settings=None):
        def collecting_get_date_data(get_date_data):
            @wraps(get_date_data)
            def wrapped(*args, **kwargs):
                self.freshness_result = get_date_data(*args, **kwargs)
                return self.freshness_result

            return wrapped

        self.add_patch(
            patch.object(
                freshness_date_parser,
                "get_date_data",
                collecting_get_date_data(freshness_date_parser.get_date_data),
            )
        )

        self.freshness_parser = Mock(wraps=freshness_date_parser)

        dt_mock = Mock(wraps=dateparser.freshness_date_parser.datetime)
        dt_mock.now = Mock(side_effect=self.now_with_timezone)
        self.add_patch(patch("dateparser.freshness_date_parser.datetime", new=dt_mock))
        self.add_patch(
            patch("dateparser.date.freshness_date_parser", new=self.freshness_parser)
        )
        self.parser = DateDataParser(settings=settings)

    def when_date_is_parsed(self):
        try:
            self.result = self.parser.get_date_data(self.date_string)
        except Exception as error:
            self.error = error

    def then_date_is(self, date):
        self.assertEqual(date, self.result["date_obj"].date())

    def then_time_is(self, time):
        self.assertEqual(time, self.result["date_obj"].time())

    def then_timezone_is(self, timezone):
        self.assertEqual(timezone, self.result["date_obj"].tzname())

    def then_period_is(self, period):
        self.assertEqual(period, self.result["period"])

    def then_date_obj_is_between(self, low_boundary, high_boundary):
        self.assertGreater(self.result["date_obj"], low_boundary)
        self.assertLess(self.result["date_obj"], high_boundary)

    def then_date_obj_is_exactly_this_time_ago(self, ago):
        self.assertEqual(self.now - relativedelta(**ago), self.result["date_obj"])

    def then_date_obj_is_exactly_this_time_in_future(self, in_future):
        self.assertEqual(self.now + relativedelta(**in_future), self.result["date_obj"])

    def then_date_was_not_parsed(self):
        self.assertIsNone(
            self.result["date_obj"], '"%s" should not be parsed' % self.date_string
        )

    def then_date_was_parsed_by_freshness_parser(self):
        self.assertEqual(self.result, self.freshness_result)

    def then_error_was_not_raised(self):
        self.assertEqual(NotImplemented, self.error)


if __name__ == "__main__":
    unittest.main()
