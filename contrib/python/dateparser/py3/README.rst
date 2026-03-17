.. Note that we use raw HTML in the header section because centering images and paragraphs is not supported in Github (https://github.com/github/markup/issues/163)

.. raw:: html

    <h1 align="center">
        <br/>
        <a href="https://github.com/scrapinghub/dateparser">
            <img src="https://raw.githubusercontent.com/scrapinghub/dateparser/master/artwork/dateparser-logo.png" alt="Dateparser" width="500">
        </a>
        <br/>
    </h1>

    <h3 align="center">Python parser for human readable dates</h4>

    <p align="center">
        <a href="https://pypi.python.org/pypi/dateparser">
            <img src="https://img.shields.io/pypi/dm/dateparser.svg" alt="PyPI - Downloads">
        </a>
        <a href="https://pypi.python.org/pypi/dateparser">
            <img src="https://img.shields.io/pypi/v/dateparser.svg" alt="PypI - Version">
        </a>
        <a href="https://codecov.io/gh/scrapinghub/dateparser">
            <img src="https://codecov.io/gh/scrapinghub/dateparser/branch/master/graph/badge.svg" alt="Code Coverage">
        </a>
        <a href="https://github.com/scrapinghub/dateparser/actions">
            <img src="https://github.com/scrapinghub/dateparser/workflows/Build/badge.svg" alt="Github - Build">
        </a>
        <a href="https://dateparser.readthedocs.org/en/latest/?badge=latest">
            <img src="https://readthedocs.org/projects/dateparser/badge/?version=latest" alt="Readthedocs - Docs">
        </a>
    </p>

    <p align="center">
        <a href="#key-features">Key Features</a> •
        <a href="#how-to-use">How To Use</a> •
        <a href="#installation">Installation</a> •
        <a href="#common-use-cases">Common use cases</a> •
        <a href="#you-may-also-like">You may also like...</a> •
        <a href="#license">License</a>
    </p>


Key Features
------------

-  Support for almost every existing date format: absolute dates,
   relative dates (``"two weeks ago"`` or ``"tomorrow"``), timestamps,
   etc.
-  Support for more than `200 language
   locales <https://dateparser.readthedocs.io/en/latest/supported_locales.html>`__.
-  Language autodetection
-  Customizable behavior through
   `settings <https://dateparser.readthedocs.io/en/latest/settings.html>`__.
-  Support for `non-Gregorian calendar
   systems <https://dateparser.readthedocs.io/en/latest/introduction.html#supported-calendars>`__.
-  Support for dates with timezones abbreviations or UTC offsets
   (``"August 14, 2015 EST"``, ``"21 July 2013 10:15 pm +0500"``...)
-  `Search
   dates <https://dateparser.readthedocs.io/en/latest/introduction.html#search-for-dates-in-longer-chunks-of-text>`__
   in longer texts.
-  Time span detection for expressions like "past month", "last week".

Online demo
-----------

Do you want to try it out without installing any dependency? Now you can test
it quickly by visiting `this online demo <https://dateparser-demo.netlify.app/>`__!



How To Use
----------

The most straightforward way to parse dates with **dateparser** is to
use the ``dateparser.parse()`` function, that wraps around most of the
functionality of the module.

.. code:: python

    >>> import dateparser

    >>> dateparser.parse('Fri, 12 Dec 2014 10:55:50')
    datetime.datetime(2014, 12, 12, 10, 55, 50)

    >>> dateparser.parse('1991-05-17')
    datetime.datetime(1991, 5, 17, 0, 0)

    >>> dateparser.parse('In two months')  # today is 1st Aug 2020
    datetime.datetime(2020, 10, 1, 11, 12, 27, 764201)

    >>> dateparser.parse('1484823450')  # timestamp
    datetime.datetime(2017, 1, 19, 10, 57, 30)

    >>> dateparser.parse('January 12, 2012 10:00 PM EST')
    datetime.datetime(2012, 1, 12, 22, 0, tzinfo=<StaticTzInfo 'EST'>)

As you can see, **dateparser** works with different date formats, but it
can also be used directly with strings in different languages:

.. code:: python

    >>> dateparser.parse('Martes 21 de Octubre de 2014')  # Spanish (Tuesday 21 October 2014)
    datetime.datetime(2014, 10, 21, 0, 0)

    >>> dateparser.parse('Le 11 Décembre 2014 à 09:00')  # French (11 December 2014 at 09:00)
    datetime.datetime(2014, 12, 11, 9, 0)

    >>> dateparser.parse('13 января 2015 г. в 13:34')  # Russian (13 January 2015 at 13:34)
    datetime.datetime(2015, 1, 13, 13, 34)

    >>> dateparser.parse('1 เดือนตุลาคม 2005, 1:00 AM')  # Thai (1 October 2005, 1:00 AM)
    datetime.datetime(2005, 10, 1, 1, 0)

    >>> dateparser.parse('yaklaşık 23 saat önce')  # Turkish (23 hours ago), current time: 12:46
    datetime.datetime(2019, 9, 7, 13, 46)

    >>> dateparser.parse('2小时前')  # Chinese (2 hours ago), current time: 22:30
    datetime.datetime(2018, 5, 31, 20, 30)

You can control multiple behaviors by using the ``settings`` parameter:

.. code:: python

    >>> dateparser.parse('2014-10-12', settings={'DATE_ORDER': 'YMD'})
    datetime.datetime(2014, 10, 12, 0, 0)

    >>> dateparser.parse('2014-10-12', settings={'DATE_ORDER': 'YDM'})
    datetime.datetime(2014, 12, 10, 0, 0)

    >>> dateparser.parse('1 year', settings={'PREFER_DATES_FROM': 'future'})  # Today is 2020-09-23
    datetime.datetime(2021, 9, 23, 0, 0)

    >>> dateparser.parse('tomorrow', settings={'RELATIVE_BASE': datetime.datetime(1992, 1, 1)})
    datetime.datetime(1992, 1, 2, 0, 0)

To see more examples on how to use the ``settings``, check the `settings
section <https://dateparser.readthedocs.io/en/latest/settings.html>`__
in the docs.

False positives
^^^^^^^^^^^^^^^

**dateparser** will do its best to return a date, dealing with multiple formats and different locales.
For that reason it is important that the input is a valid date, otherwise it could return false positives.

To reduce the possibility of receiving false positives, make sure that:

- The input string is a valid date and doesn't contain any other words or numbers.
- If you know the language or languages beforehand, you add them through the ``languages`` or ``locales`` properties.


On the other hand, if you want to exclude any of the default parsers
(``timestamp``, ``relative-time``...) or change the order in which they
are executed, you can do so through the
`settings PARSERS <https://dateparser.readthedocs.io/en/latest/usage.html#handling-incomplete-dates>`_.

Installation
------------

Dateparser supports Python 3.10+. You can install it by doing:

::

    $ pip install dateparser

If you want to use the jalali or hijri calendar, you need to install the
``calendars`` extra:

::

    $ pip install dateparser[calendars]

Common use cases
----------------

**dateparser** can be used for a wide variety of purposes,
but it stands out when it comes to:

Consuming data from different sources:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  **Scraping**: extract dates from different places with several
   different formats and languages
-  **IoT**: consuming data coming from different sources with different
   date formats
-  **Tooling**: consuming dates from different logs / sources
-  **Format transformations**: when transforming dates coming from
   different files (PDF, CSV, etc.) to other formats (database, etc).

Offering natural interaction with users:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  **Tooling and CLI**: allow users to write “3 days ago” to retrieve
   information.
-  **Search engine**: allow people to search by date in an easy /
   natural format.
-  **Bots**: allow users to interact with a bot easily

You may also like...
--------------------

-  `price-parser <https://github.com/scrapinghub/price-parser/>`__ - A
   small library for extracting price and currency from raw text
   strings.
-  `number-parser <https://github.com/scrapinghub/number-parser/>`__ -
   Library to convert numbers written in the natural language to it's
   equivalent numeric forms.
-  `Scrapy <https://github.com/scrapy/scrapy/>`__ - Web crawling and web
   scraping framework

License
-------

`BSD3-Clause <https://github.com/scrapinghub/dateparser/blob/master/LICENSE>`__
