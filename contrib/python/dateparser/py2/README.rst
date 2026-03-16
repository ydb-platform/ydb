====================================================
dateparser -- python parser for human readable dates
====================================================

.. image:: https://img.shields.io/travis/scrapinghub/dateparser/master.svg?style=flat-square
   :target: https://travis-ci.org/scrapinghub/dateparser
   :alt: travis build status

.. image:: https://img.shields.io/pypi/v/dateparser.svg?style=flat-square
   :target: https://pypi.python.org/pypi/dateparser
   :alt: pypi version

.. image:: https://readthedocs.org/projects/dateparser/badge/?version=latest
   :target: http://dateparser.readthedocs.org/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://codecov.io/gh/scrapinghub/dateparser/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/scrapinghub/dateparser
   :alt: Code Coverage

.. image:: https://badges.gitter.im/scrapinghub/dateparser.svg
   :alt: Join the chat at https://gitter.im/scrapinghub/dateparser
   :target: https://gitter.im/scrapinghub/dateparser?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge


`dateparser` provides modules to easily parse localized dates in almost
any string formats commonly found on web pages.

.. contents::

Documentation
=============

Documentation is built automatically and can be found on
`Read the Docs <https://dateparser.readthedocs.org/en/latest/>`_.


Features
========

* Generic parsing of dates in over 200 language locales plus numerous formats in a language agnostic fashion.
* Generic parsing of relative dates like: ``'1 min ago'``, ``'2 weeks ago'``, ``'3 months, 1 week and 1 day ago'``, ``'in 2 days'``, ``'tomorrow'``.
* Generic parsing of dates with time zones abbreviations or UTC offsets like: ``'August 14, 2015 EST'``, ``'July 4, 2013 PST'``, ``'21 July 2013 10:15 pm +0500'``.
* Date lookup in longer texts.
* Support for non-Gregorian calendar systems. See `Supported Calendars`_.
* Extensive test coverage.


Usage
=====

The most straightforward way is to use the `dateparser.parse <#dateparser.parse>`_ function,
that wraps around most of the functionality in the module.

.. automodule:: dateparser
   :members: parse


Popular Formats
---------------

    >>> import dateparser
    >>> dateparser.parse('12/12/12')
    datetime.datetime(2012, 12, 12, 0, 0)
    >>> dateparser.parse(u'Fri, 12 Dec 2014 10:55:50')
    datetime.datetime(2014, 12, 12, 10, 55, 50)
    >>> dateparser.parse(u'Martes 21 de Octubre de 2014')  # Spanish (Tuesday 21 October 2014)
    datetime.datetime(2014, 10, 21, 0, 0)
    >>> dateparser.parse(u'Le 11 Décembre 2014 à 09:00')  # French (11 December 2014 at 09:00)
    datetime.datetime(2014, 12, 11, 9, 0)
    >>> dateparser.parse(u'13 января 2015 г. в 13:34')  # Russian (13 January 2015 at 13:34)
    datetime.datetime(2015, 1, 13, 13, 34)
    >>> dateparser.parse(u'1 เดือนตุลาคม 2005, 1:00 AM')  # Thai (1 October 2005, 1:00 AM)
    datetime.datetime(2005, 10, 1, 1, 0)

This will try to parse a date from the given string, attempting to
detect the language each time.

You can specify the language(s), if known, using ``languages`` argument. In this case, given languages are used and language detection is skipped:

    >>> dateparser.parse('2015, Ago 15, 1:08 pm', languages=['pt', 'es'])
    datetime.datetime(2015, 8, 15, 13, 8)

If you know the possible formats of the dates, you can
use the ``date_formats`` argument:

    >>> dateparser.parse(u'22 Décembre 2010', date_formats=['%d %B %Y'])
    datetime.datetime(2010, 12, 22, 0, 0)


Relative Dates
--------------

    >>> parse('1 hour ago')
    datetime.datetime(2015, 5, 31, 23, 0)
    >>> parse(u'Il ya 2 heures')  # French (2 hours ago)
    datetime.datetime(2015, 5, 31, 22, 0)
    >>> parse(u'1 anno 2 mesi')  # Italian (1 year 2 months)
    datetime.datetime(2014, 4, 1, 0, 0)
    >>> parse(u'yaklaşık 23 saat önce')  # Turkish (23 hours ago)
    datetime.datetime(2015, 5, 31, 1, 0)
    >>> parse(u'Hace una semana')  # Spanish (a week ago)
    datetime.datetime(2015, 5, 25, 0, 0)
    >>> parse(u'2小时前')  # Chinese (2 hours ago)
    datetime.datetime(2015, 5, 31, 22, 0)

.. note:: Testing above code might return different values for you depending on your environment's current date and time.

.. note:: Support for relative dates in future needs a lot of improvement, we look forward to community's contribution to get better on that part. See `Contributing`_.


OOTB Language Based Date Order Preference
-----------------------------------------

   >>> # parsing ambiguous date
   >>> parse('02-03-2016')  # assumes english language, uses MDY date order
   datetime.datetime(2016, 2, 3, 0, 0)
   >>> parse('le 02-03-2016')  # detects french, uses DMY date order
   datetime.datetime(2016, 3, 2, 0, 0)

.. note:: Ordering is not locale based, that's why do not expect `DMY` order for UK/Australia English. You can specify date order in that case as follows usings `Settings`_:

    >>> parse('18-12-15 06:00', settings={'DATE_ORDER': 'DMY'})
    datetime.datetime(2015, 12, 18, 6, 0)

For more on date order, please look at `Settings`_.


Timezone and UTC Offset
-----------------------

By default, `dateparser` returns tzaware `datetime` if timezone is present in date string. Otherwise, it returns a naive `datetime` object.

    >>> parse('January 12, 2012 10:00 PM EST')
    datetime.datetime(2012, 1, 12, 22, 0, tzinfo=<StaticTzInfo 'EST'>)

    >>> parse('January 12, 2012 10:00 PM -0500')
    datetime.datetime(2012, 1, 12, 22, 0, tzinfo=<StaticTzInfo 'UTC\-05:00'>)

    >>> parse('2 hours ago EST')
    datetime.datetime(2017, 3, 10, 15, 55, 39, 579667, tzinfo=<StaticTzInfo 'EST'>)

    >>> parse('2 hours ago -0500')
    datetime.datetime(2017, 3, 10, 15, 59, 30, 193431, tzinfo=<StaticTzInfo 'UTC\-05:00'>)

 If date has no timezone name/abbreviation or offset, you can specify it using `TIMEZONE` setting.

    >>> parse('January 12, 2012 10:00 PM', settings={'TIMEZONE': 'US/Eastern'})
    datetime.datetime(2012, 1, 12, 22, 0)

    >>> parse('January 12, 2012 10:00 PM', settings={'TIMEZONE': '+0500'})
    datetime.datetime(2012, 1, 12, 22, 0)

`TIMEZONE` option may not be useful alone as it only attaches given timezone to
resultant `datetime` object. But can be useful in cases where you want conversions from and to different
timezones or when simply want a tzaware date with given timezone info attached.

    >>> parse('January 12, 2012 10:00 PM', settings={'TIMEZONE': 'US/Eastern', 'RETURN_AS_TIMEZONE_AWARE': True})
    datetime.datetime(2012, 1, 12, 22, 0, tzinfo=<DstTzInfo 'US/Eastern' EST-1 day, 19:00:00 STD>)


    >>> parse('10:00 am', settings={'TIMEZONE': 'EST', 'TO_TIMEZONE': 'EDT'})
    datetime.datetime(2016, 9, 25, 11, 0)

Some more use cases for conversion of timezones.

    >>> parse('10:00 am EST', settings={'TO_TIMEZONE': 'EDT'})  # date string has timezone info
    datetime.datetime(2017, 3, 12, 11, 0, tzinfo=<StaticTzInfo 'EDT'>)

    >>> parse('now EST', settings={'TO_TIMEZONE': 'UTC'})  # relative dates
    datetime.datetime(2017, 3, 10, 23, 24, 47, 371823, tzinfo=<StaticTzInfo 'UTC'>)

In case, no timezone is present in date string or defined in `settings`. You can still
return tzaware `datetime`. It is especially useful in case of relative dates when uncertain
what timezone is relative base.

    >>> parse('2 minutes ago', settings={'RETURN_AS_TIMEZONE_AWARE': True})
    datetime.datetime(2017, 3, 11, 4, 25, 24, 152670, tzinfo=<DstTzInfo 'Asia/Karachi' PKT+5:00:00 STD>)

In case, you want to compute relative dates in UTC instead of default system's local timezone, you can use `TIMEZONE` setting.

    >>> parse('4 minutes ago', settings={'TIMEZONE': 'UTC'})
    datetime.datetime(2017, 3, 10, 23, 27, 59, 647248, tzinfo=<StaticTzInfo 'UTC'>)

.. note:: In case, when timezone is present both in string and also specified using `settings`, string is parsed into tzaware representation and then converted to timezone specified in `settings`.

   >>> parse('10:40 pm PKT', settings={'TIMEZONE': 'UTC'})
   datetime.datetime(2017, 3, 12, 17, 40, tzinfo=<StaticTzInfo 'UTC'>)

   >>> parse('20 mins ago EST', settings={'TIMEZONE': 'UTC'})
   datetime.datetime(2017, 3, 12, 21, 16, 0, 885091, tzinfo=<StaticTzInfo 'UTC'>)

For more on timezones, please look at `Settings`_.


Incomplete Dates
----------------

    >>> from dateparser import parse
    >>> parse(u'December 2015')  # default behavior
    datetime.datetime(2015, 12, 16, 0, 0)
    >>> parse(u'December 2015', settings={'PREFER_DAY_OF_MONTH': 'last'})
    datetime.datetime(2015, 12, 31, 0, 0)
    >>> parse(u'December 2015', settings={'PREFER_DAY_OF_MONTH': 'first'})
    datetime.datetime(2015, 12, 1, 0, 0)

    >>> parse(u'March')
    datetime.datetime(2015, 3, 16, 0, 0)
    >>> parse(u'March', settings={'PREFER_DATES_FROM': 'future'})
    datetime.datetime(2016, 3, 16, 0, 0)
    >>> # parsing with preference set for 'past'
    >>> parse('August', settings={'PREFER_DATES_FROM': 'past'})
    datetime.datetime(2015, 8, 15, 0, 0)

You can also ignore parsing incomplete dates altogether by setting `STRICT_PARSING` flag as follows:

    >>> parse(u'December 2015', settings={'STRICT_PARSING': True})
    None

For more on handling incomplete dates, please look at `Settings`_.


Search for Dates in Longer Chunks of Text
-----------------------------------------

You can extract dates from longer strings of text. They are returned as list of tuples with text chunk containing the date and parsed datetime object.

.. automodule:: dateparser.search
   :members: search_dates

Dependencies
============

`dateparser` relies on following libraries in some ways:

  * dateutil_'s module ``relativedelta`` for its freshness parser.
  * jdatetime_ to convert *Jalali* dates to *Gregorian*.
  * umalqurra_ to convert *Hijri* dates to *Gregorian*.
  * tzlocal_ to reliably get local timezone.
  * ruamel.yaml_ (optional) for operations on language files.

.. _dateutil: https://pypi.python.org/pypi/python-dateutil
.. _jdatetime: https://pypi.python.org/pypi/jdatetime
.. _umalqurra: https://pypi.python.org/pypi/umalqurra/
.. _tzlocal: https://pypi.python.org/pypi/tzlocal
.. _ruamel.yaml: https://pypi.python.org/pypi/ruamel.yaml

Supported languages and locales
===============================

============    ================================================================
  Language            Locales
============    ================================================================
en                'en-001', 'en-150', 'en-AG', 'en-AI', 'en-AS', 'en-AT', 'en-AU', 'en-BB', 'en-BE', 'en-BI', 'en-BM', 'en-BS', 'en-BW', 'en-BZ', 'en-CA', 'en-CC', 'en-CH', 'en-CK', 'en-CM', 'en-CX', 'en-CY', 'en-DE', 'en-DG', 'en-DK', 'en-DM', 'en-ER', 'en-FI', 'en-FJ', 'en-FK', 'en-FM', 'en-GB', 'en-GD', 'en-GG', 'en-GH', 'en-GI', 'en-GM', 'en-GU', 'en-GY', 'en-HK', 'en-IE', 'en-IL', 'en-IM', 'en-IN', 'en-IO', 'en-JE', 'en-JM', 'en-KE', 'en-KI', 'en-KN', 'en-KY', 'en-LC', 'en-LR', 'en-LS', 'en-MG', 'en-MH', 'en-MO', 'en-MP', 'en-MS', 'en-MT', 'en-MU', 'en-MW', 'en-MY', 'en-NA', 'en-NF', 'en-NG', 'en-NL', 'en-NR', 'en-NU', 'en-NZ', 'en-PG', 'en-PH', 'en-PK', 'en-PN', 'en-PR', 'en-PW', 'en-RW', 'en-SB', 'en-SC', 'en-SD', 'en-SE', 'en-SG', 'en-SH', 'en-SI', 'en-SL', 'en-SS', 'en-SX', 'en-SZ', 'en-TC', 'en-TK', 'en-TO', 'en-TT', 'en-TV', 'en-TZ', 'en-UG', 'en-UM', 'en-VC', 'en-VG', 'en-VI', 'en-VU', 'en-WS', 'en-ZA', 'en-ZM', 'en-ZW'
zh
zh-Hans           'zh-Hans-HK', 'zh-Hans-MO', 'zh-Hans-SG'
hi
es                'es-419', 'es-AR', 'es-BO', 'es-BR', 'es-BZ', 'es-CL', 'es-CO', 'es-CR', 'es-CU', 'es-DO', 'es-EA', 'es-EC', 'es-GQ', 'es-GT', 'es-HN', 'es-IC', 'es-MX', 'es-NI', 'es-PA', 'es-PE', 'es-PH', 'es-PR', 'es-PY', 'es-SV', 'es-US', 'es-UY', 'es-VE'
ar                'ar-AE', 'ar-BH', 'ar-DJ', 'ar-DZ', 'ar-EG', 'ar-EH', 'ar-ER', 'ar-IL', 'ar-IQ', 'ar-JO', 'ar-KM', 'ar-KW', 'ar-LB', 'ar-LY', 'ar-MA', 'ar-MR', 'ar-OM', 'ar-PS', 'ar-QA', 'ar-SA', 'ar-SD', 'ar-SO', 'ar-SS', 'ar-SY', 'ar-TD', 'ar-TN', 'ar-YE'
bn                'bn-IN'
fr                'fr-BE', 'fr-BF', 'fr-BI', 'fr-BJ', 'fr-BL', 'fr-CA', 'fr-CD', 'fr-CF', 'fr-CG', 'fr-CH', 'fr-CI', 'fr-CM', 'fr-DJ', 'fr-DZ', 'fr-GA', 'fr-GF', 'fr-GN', 'fr-GP', 'fr-GQ', 'fr-HT', 'fr-KM', 'fr-LU', 'fr-MA', 'fr-MC', 'fr-MF', 'fr-MG', 'fr-ML', 'fr-MQ', 'fr-MR', 'fr-MU', 'fr-NC', 'fr-NE', 'fr-PF', 'fr-PM', 'fr-RE', 'fr-RW', 'fr-SC', 'fr-SN', 'fr-SY', 'fr-TD', 'fr-TG', 'fr-TN', 'fr-VU', 'fr-WF', 'fr-YT'
ur                'ur-IN'
pt                'pt-AO', 'pt-CH', 'pt-CV', 'pt-GQ', 'pt-GW', 'pt-LU', 'pt-MO', 'pt-MZ', 'pt-PT', 'pt-ST', 'pt-TL'
ru                'ru-BY', 'ru-KG', 'ru-KZ', 'ru-MD', 'ru-UA'
id
sw                'sw-CD', 'sw-KE', 'sw-UG'
pa-Arab
de                'de-AT', 'de-BE', 'de-CH', 'de-IT', 'de-LI', 'de-LU'
ja
te
mr
vi
fa                'fa-AF'
ta                'ta-LK', 'ta-MY', 'ta-SG'
tr                'tr-CY'
yue
ko                'ko-KP'
it                'it-CH', 'it-SM', 'it-VA'
fil
gu
th
kn
ps
zh-Hant           'zh-Hant-HK', 'zh-Hant-MO'
ml
or
pl
my
pa
pa-Guru
am
om                'om-KE'
ha                'ha-GH', 'ha-NE'
nl                'nl-AW', 'nl-BE', 'nl-BQ', 'nl-CW', 'nl-SR', 'nl-SX'
uk
uz
uz-Latn
yo                'yo-BJ'
ms                'ms-BN', 'ms-SG'
ig
ro                'ro-MD'
mg
ne                'ne-IN'
as
so                'so-DJ', 'so-ET', 'so-KE'
si
km
zu
cs
sv                'sv-AX', 'sv-FI'
hu
el                'el-CY'
sn
kk
rw
ckb               'ckb-IR'
qu                'qu-BO', 'qu-EC'
ak
be
ti                'ti-ER'
az
az-Latn
af                'af-NA'
ca                'ca-AD', 'ca-FR', 'ca-IT'
sr-Latn           'sr-Latn-BA', 'sr-Latn-ME', 'sr-Latn-XK'
ii
he
bg
bm
ki
gsw               'gsw-FR', 'gsw-LI'
sr
sr-Cyrl           'sr-Cyrl-BA', 'sr-Cyrl-ME', 'sr-Cyrl-XK'
ug
zgh
ff                'ff-CM', 'ff-GN', 'ff-MR'
rn
da                'da-GL'
hr                'hr-BA'
sq                'sq-MK', 'sq-XK'
sk
fi
ks
hy
nb                'nb-SJ'
luy
lg
lo
bem
kok
luo
uz-Cyrl
ka
ee                'ee-TG'
mzn
bs-Cyrl
bs
bs-Latn
kln
kam
gl
tzm
dje
kab
bo                'bo-IN'
shi-Latn
shi
shi-Tfng
mn
ln                'ln-AO', 'ln-CF', 'ln-CG'
ky
sg
lt
nyn
guz
cgg
xog
lrc               'lrc-IQ'
mer
lu
sl
teo               'teo-KE'
brx
nd
mk
uz-Arab
mas               'mas-TZ'
nn
kde
mfe
lv
seh
mgh
az-Cyrl
ga
eu
yi
ce
et
ksb
bez
ewo
fy
ebu
nus
ast
asa
ses
os                'os-RU'
br
cy
kea
lag
sah
mt
vun
rof
jmc
lb
dav
dyo
dz
nnh
is
khq
bas
naq
mua
ksh
saq
se                'se-FI', 'se-SE'
dua
rwk
mgo
sbp
to
jgo
ksf
fo                'fo-DK'
gd
kl
rm
fur
agq
haw
chr
hsb
wae
nmg
lkt
twq
dsb
yav
kw
gv
smn
eo
tl
============    ================================================================


Supported Calendars
===================
* Gregorian calendar.

* Persian Jalali calendar. For more information, refer to `Persian Jalali Calendar <https://en.wikipedia.org/wiki/Iranian_calendars#Zoroastrian_calendar>`_.

* Hijri/Islamic Calendar. For more information, refer to `Hijri Calendar <https://en.wikipedia.org/wiki/Islamic_calendar>`_.

    >>> from dateparser.calendars.jalali import JalaliCalendar
    >>> JalaliCalendar(u'جمعه سی ام اسفند ۱۳۸۷').get_date()
    {'date_obj': datetime.datetime(2009, 3, 20, 0, 0), 'period': 'day'}

    >>> from dateparser.calendars.hijri import HijriCalendar
    >>> HijriCalendar(u'17-01-1437 هـ 08:30 مساءً').get_date()
    {'date_obj': datetime.datetime(2015, 10, 30, 20, 30), 'period': 'day'}

.. note:: `HijriCalendar` has some limitations with Python 3.
.. note:: For `Finnish` language, please specify `settings={'SKIP_TOKENS': []}` to correctly parse freshness dates.


Install using following command to use calendars.

.. tip::
   pip install dateparser[calendars]
