# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from parameterized import parameterized, param
from datetime import datetime

from dateparser.calendars.jalali import JalaliCalendar
from dateparser.calendars.jalali_parser import jalali_parser, PersianDate
from tests import BaseTestCase


class TestPersianDate(BaseTestCase):
    def setUp(self):
        super(TestPersianDate, self).setUp()
        self.persian_date = NotImplemented

    def when_date_is_given(self, year, month, day):
        self.persian_date = PersianDate(year, month, day)

    def then_weekday_is(self, weekday):
        self.assertEqual(self.persian_date.weekday(), weekday)

    @parameterized.expand([
        param(year=1348, month=1, day=3, weekday=0),
        param(year=1348, month=2, day=28, weekday=0),
        param(year=1348, month=3, day=27, weekday=2),
        param(year=1348, month=4, day=11, weekday=3),
    ])
    def test_weekday(self, year, month, day, weekday):
        self.when_date_is_given(year, month, day)
        self.then_weekday_is(weekday)


class TestJalaliParser(BaseTestCase):
    def setUp(self):
        super(TestJalaliParser, self).setUp()
        self.translated = NotImplemented

    def when_date_is_given(self, date_string):
        self.translated = jalali_parser.to_latin(date_string)

    def then_month_is_parsed_as(self, month_name):
        self.assertEqual(month_name, self.translated)

    def then_weekday_is_parsed_as(self, weekday):
        self.assertEqual(weekday, self.translated)

    def then_digit_is_parsed_as(self, digit):
        self.assertEqual(digit, self.translated)

    def then_numeral_parsed_is(self, datetime):
        self.assertEqual(datetime, self.translated)

    @parameterized.expand([
        param(date_string='فروردین', latin='Farvardin'),
        param(date_string='اردیبهشت', latin='Ordibehesht'),
        param(date_string='خرداد', latin='Khordad'),
        param(date_string='تیر', latin='Tir'),
        param(date_string='مرداد', latin='Mordad'),
        param(date_string='شهریور', latin='Shahrivar'),
        param(date_string='مهر', latin='Mehr'),
        param(date_string='آبان', latin='Aban'),
        param(date_string='آذر', latin='Azar'),
        param(date_string='دی', latin='Dey'),
        param(date_string='بهمن', latin='Bahman'),
        param(date_string='اسفند', latin='Esfand'),
    ])
    def test_replace_months(self, date_string, latin):
        self.when_date_is_given(date_string)
        self.then_month_is_parsed_as(latin)

    @parameterized.expand([
        param(date_string='یکشنبه', latin='Sunday'),
        param(date_string='روز شنبه', latin='Saturday'),
        param(date_string='دوشنبه', latin='Monday'),
        param(date_string='سهشنبه', latin='Tuesday'),
        param(date_string='چهار شنبه', latin='Wednesday'),
        param(date_string='پنج شنبه', latin='Thursday'),
        param(date_string='جمعه', latin='Friday'),
    ])
    def test_replace_weekdays(self, date_string, latin):
        self.when_date_is_given(date_string)
        self.then_weekday_is_parsed_as(latin)

    @parameterized.expand([
        param(date_string='۰۱', latin='01'),
        param(date_string='۲۵', latin='25'),
        param(date_string='۶۷۴', latin='674'),
        param(date_string='۰۵۶۱۲۷۳۸۹۴', latin='0561273894')
    ])
    def test_replace_digits(self, date_string, latin):
        self.when_date_is_given(date_string)
        self.then_digit_is_parsed_as(latin)

    @parameterized.expand([
        param(persian='صفر', numeral='0'),
        param(persian='یک', numeral='1'),
        param(persian='اول', numeral='1'),
        param(persian='دو', numeral='2'),
        param(persian='دوم', numeral='2'),
        param(persian='سه', numeral='3'),
        param(persian='سوم', numeral='3'),
        param(persian='چهار', numeral='4'),
        param(persian='چهارم', numeral='4'),
        param(persian='پنج', numeral='5'),
        param(persian='پنجم', numeral='5'),
        param(persian='شش', numeral='6'),
        param(persian='ششم', numeral='6'),
        param(persian='هفت', numeral='7'),
        param(persian='هفتم', numeral='7'),
        param(persian='هشت', numeral='8'),
        param(persian='هشتمین', numeral='8'),
        param(persian='نه', numeral='9'),
        param(persian='نهم', numeral='9'),
        param(persian='ده', numeral='10'),
        param(persian='دهم', numeral='10'),
        param(persian='یازده', numeral='11'),
        param(persian='یازدهم', numeral='11'),
        param(persian='دوازده', numeral='12'),
        param(persian='دوازدهم', numeral='12'),
        param(persian='سیزده', numeral='13'),
        param(persian='سیزدهم', numeral='13'),
        param(persian='چهارده', numeral='14'),
        param(persian='چهاردهم', numeral='14'),
        param(persian='پانزده', numeral='15'),
        param(persian='پانزدهمین', numeral='15'),
        param(persian='شانزده', numeral='16'),
        param(persian='شانزدهم', numeral='16'),
        param(persian='هفده', numeral='17'),
        param(persian='هفدهم', numeral='17'),
        param(persian='هجده', numeral='18'),
        param(persian='هجدهم', numeral='18'),
        param(persian='نوزده', numeral='19'),
        param(persian='نوزدهم', numeral='19'),
        param(persian='بیست', numeral='20'),
        param(persian='بیستم', numeral='20'),
        param(persian='بیست و یک', numeral='21'),
        param(persian='بیست و دو', numeral='22'),
        param(persian='بیست ثانیه', numeral='22'),
        param(persian='بیست و سه', numeral='23'),
        param(persian='بیست و سوم', numeral='23'),
        param(persian='بیست و چهار', numeral='24'),
        param(persian='بیست و چهارم', numeral='24'),
        param(persian='بیست و پنج', numeral='25'),
        param(persian='بیست و شش', numeral='26'),
        param(persian='بیست و هفت', numeral='27'),
        param(persian='بیست و هفتمین', numeral='27'),
        param(persian='بیست و هشت', numeral='28'),
        param(persian='بیست و نه', numeral='29'),
        param(persian='سی', numeral='30'),
        param(persian='سی و یک', numeral='31'),
        param(persian='سی و یکم', numeral='31'),
    ])
    def test_replace_day_literal_with_numerals(self, persian, numeral):
        self.when_date_is_given(persian)
        self.then_numeral_parsed_is(numeral)


class TestJalaliCalendar(BaseTestCase):

    def setUp(self):
        super(TestJalaliCalendar, self).setUp()
        self.result = NotImplemented
        self.date_string = NotImplemented
        self.calendar = NotImplemented

    def when_date_is_given(self, date_string):
        self.date_string = date_string
        self.calendar = JalaliCalendar(date_string)
        self.result = self.calendar.get_date()

    def then_date_parsed_is(self, datetime):
        self.assertEqual(datetime, self.result['date_obj'])

    @parameterized.expand([
        param(date_string=u'سه شنبه سوم شهریور ۱۳۹۴', dt=datetime(2015, 8, 25, 0, 0)),
        param(date_string=u'پنجشنبه چهارم تیر ۱۳۹۴', dt=datetime(2015, 6, 25, 0, 0)),
        param(date_string=u'شنبه یکم فروردین ۱۳۹۴', dt=datetime(2015, 3, 21, 0, 0)),
        param(date_string=u'یکشنبه شانزدهم آذر ۱۳۹۳', dt=datetime(2014, 12, 7, 0, 0)),
        param(date_string=u'دوشنبه دوازدهم آبان ۱۳۹۳', dt=datetime(2014, 11, 3, 0, 0)),
        param(date_string=u'یکشنبه سیزدهم مهر ۱۳۹۳', dt=datetime(2014, 10, 5, 0, 0)),
        param(date_string=u'دوشنبه دوم تیر ۱۳۹۳', dt=datetime(2014, 6, 23, 0, 0)),
        param(date_string=u'یکشنبه سوم فروردین ۱۳۹۳', dt=datetime(2014, 3, 23, 0, 0)),
        param(date_string=u'پنجشنبه دهم بهمن ۱۳۹۲', dt=datetime(2014, 1, 30, 0, 0)),
        param(date_string=u'شنبه چهاردهم دی ۱۳۹۲', dt=datetime(2014, 1, 4, 0, 0)),
        param(date_string=u'جمعه بیستم خرداد ۱۳۹۰', dt=datetime(2011, 6, 10, 0, 0)),
        param(date_string=u'شنبه نهم مرداد ۱۳۸۹', dt=datetime(2010, 7, 31, 0, 0)),
        param(date_string=u'پنجشنبه بیست و سوم اردیبهشت ۱۳۸۹', dt=datetime(2010, 5, 13, 0, 0)),
        param(date_string=u'جمعه سی ام اسفند ۱۳۸۷', dt=datetime(2009, 3, 20, 0, 0)),
        # dates with time component
        param(date_string=u'پنجشنبه ۲۶ شهريور ۱۳۹۴ ساعت ۹:۳۲', dt=datetime(2015, 9, 17, 9, 32)),
        param(date_string=u'دوشنبه ۲۳ شهريور ۱۳۹۴ ساعت ۱۹:۱۱', dt=datetime(2015, 9, 14, 19, 11)),
        param(date_string=u'جمعه سی ام اسفند ۱۳۸۷ساعت 19:47', dt=datetime(2009, 3, 20, 19, 47)),
        param(date_string=u'شنبه چهاردهم دی ۱۳۹۲ساعت 12:1', dt=datetime(2014, 1, 4, 12, 1)),
        param(
            date_string=u'پنجشنبه 26 شهریور 1394 ساعت ساعت 11 و 01 دقیقه و 47 ثانیه',
            dt=datetime(2015, 9, 17, 11, 1, 47)
        ),
        param(
            date_string=u'پنجشنبه 26 شهریور 1394 ساعت ساعت 10 و 58 دقیقه و 04 ثانیه',
            dt=datetime(2015, 9, 17, 10, 58, 4)
        ),
        param(
            date_string=u'سه شنبه 17 شهریور 1394 ساعت ساعت 18 و 21 دقیقه و 44 ثانیه',
            dt=datetime(2015, 9, 8, 18, 21, 44)
        ),
        param(date_string=u'پنجشنبه 11 تیر 1394', dt=datetime(2015, 7, 2, 0, 0)),
        param(
            date_string=u'پنجشنبه 26 شهریور 1394 ساعت ساعت 11 و 01 دقیقه',
            dt=datetime(2015, 9, 17, 11, 1)
        ),
    ])
    def test_get_date(self, date_string, dt):
        self.when_date_is_given(date_string)
        self.then_date_parsed_is(dt)
