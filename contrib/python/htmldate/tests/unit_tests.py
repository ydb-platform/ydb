# pylint:disable-msg=W1401
"""
Unit tests for the htmldate library.
"""


import datetime
import io
import logging
import os
import re
import sys

from collections import Counter
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

import pytest

from lxml import html
from lxml.etree import XPathEvalError

from htmldate.cli import cli_examine, main, parse_args, process_args
from htmldate.core import (
    compare_reference,
    examine_date_elements,
    find_date,
    search_page,
    search_pattern,
    select_candidate,
)
from htmldate.extractors import (
    custom_parse,
    discard_unwanted,
    external_date_parser,
    regex_parse,
    try_date_expr,
)
from htmldate.meta import reset_caches
from htmldate.settings import MIN_DATE
from htmldate.utils import (
    Extractor,
    decode_response,
    fetch_url,
    is_dubious_html,
    load_html,
    repair_faulty_html,
)
from htmldate.validators import (
    convert_date,
    get_max_date,
    get_min_date,
    is_valid_date,
    is_valid_format,
)


import yatest.common as yc
TEST_DIR = os.path.abspath(os.path.dirname(yc.source_path(__file__)))
OUTPUTFORMAT = "%Y-%m-%d"

LATEST_POSSIBLE = datetime.datetime.now()

OPTIONS = Extractor(True, LATEST_POSSIBLE, MIN_DATE, True, OUTPUTFORMAT)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def test_input():
    """test if loaded strings/trees are handled properly"""
    assert is_dubious_html("This is a string.") is True

    htmlstring = "<!DOCTYPE html PUBLIC />\n<html></html>"
    beginning = htmlstring[:50].lower()
    assert repair_faulty_html(htmlstring, beginning) == "\n<html></html>"

    htmlstring = "<html>\n</html>"
    beginning = htmlstring[:50].lower()
    assert repair_faulty_html(htmlstring, beginning) == htmlstring

    htmlstring = "<html/>\n</html>"
    beginning = htmlstring[:50].lower()
    assert repair_faulty_html(htmlstring, beginning) == "<html>\n</html>"

    htmlstring = '<!DOCTYPE html>\n<html lang="en-US"/>\n<head/>\n<body/>\n</html>'
    beginning = htmlstring[:50].lower()
    assert (
        repair_faulty_html(htmlstring, beginning)
        == '<!DOCTYPE html>\n<html lang="en-US">\n<head/>\n<body/>\n</html>'
    )

    with pytest.raises(TypeError) as err:
        assert load_html(123) is None
    assert "incompatible" in str(err.value)
    assert load_html("<" * 100) is None
    assert load_html("<xml><body>ABC</body></xml>") is None
    assert load_html("<html><body>XYZ</body></html>") is not None
    assert load_html(b"<html><body>XYZ</body></html>") is not None
    assert load_html(b"<" * 100) is None
    assert load_html(b"<html><body>\x2f\x2e\x9f</body></html>") is not None
    assert (
        load_html("<html><body>\x2f\x2e\x9f</body></html>".encode("latin-1"))
        is not None
    )

    # No internet in CI
    # assert load_html("https://httpbun.com/html") is not None

    # encoding declaration
    assert (
        load_html(
            '<?xml version="1.0" encoding="utf-8"?><!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/><meta http-equiv="Content-Type" content="text/html; charset=utf-8"/></head><body><p>A</p><p>B</p></body></html>'
        )
        is not None
    )
    # response decoding
    assert decode_response(b"\x1f\x8babcdef") is not None
    mock = Mock()
    mock.data = b" "
    assert decode_response(mock) is not None

    # find_date logic
    with pytest.raises(TypeError):
        assert find_date(None) is None
    assert find_date("<" * 100) is None
    assert find_date("<html></html>", verbose=True) is None
    assert find_date("<html><body>\u0008this\xdf\n\u001f+\uffff</body></html>") is None

    # min and max date output
    assert get_min_date("2020-02-20").date() == datetime.date(2020, 2, 20)
    assert get_min_date(None).date() == datetime.date(1995, 1, 1)
    assert get_min_date("3030-30-50").date() == datetime.date(1995, 1, 1)
    assert get_min_date(datetime.datetime(1990, 1, 1)) == datetime.datetime(1990, 1, 1)
    assert get_min_date("2020-02-20T13:30:00") == datetime.datetime(2020, 2, 20, 13, 30)

    assert get_max_date("2020-02-20").date() == datetime.date(2020, 2, 20)
    assert get_max_date(None).date() == datetime.date.today()
    assert get_max_date("3030-30-50").date() == datetime.date.today()
    assert get_max_date(datetime.datetime(3000, 1, 1)) == datetime.datetime(3000, 1, 1)
    assert get_max_date("2020-02-20T13:30:00") == datetime.datetime(2020, 2, 20, 13, 30)


def test_sanity():
    """Test if function arguments are interpreted and processed correctly."""
    # XPath looking for date elements
    mytree = html.fromstring("<html><body><p>Test.</p></body></html>")
    with pytest.raises(XPathEvalError):
        examine_date_elements(mytree, ".//[Error", OPTIONS)
    result = examine_date_elements(mytree, ".//p", OPTIONS)
    assert result is None
    mytree = html.fromstring("<html><body><p>1999/03/05</p></body></html>")
    result = examine_date_elements(mytree, ".//p", OPTIONS)
    assert result is not None
    # wrong field values in output format
    assert is_valid_format("%Y-%m-%d") is True
    assert is_valid_format("%M-%Y") is True
    assert is_valid_format("ABC") is False
    assert is_valid_format(123) is False
    assert is_valid_format(("a", "b")) is False
    _, discarded = discard_unwanted(
        html.fromstring(
            '<html><body><div id="wm-ipp">000</div><div>AAA</div></body></html>'
        )
    )
    assert len(discarded) == 1
    # reset caches: examine_date_elements used above
    old_values = try_date_expr.cache_info()
    reset_caches()
    assert try_date_expr.cache_info() != old_values


def test_no_date():
    """These pages should not return any date"""
    assert find_date("<html><body>XYZ</body></html>", outputformat="123") is None
    assert (
        find_date(
            "<html><body>XYZ</body></html>", url="http://www.website.com/9999/01/43/"
        )
        is None
    )
    assert find_date("<html><body><time></time></body></html>") is None
    assert (
        find_date(
            '<html><body><abbr class="published"></abbr></body></html>',
        )
        is None
    )


def test_exact_date():
    """These pages should return an exact date"""
    ## HTML tree
    assert (
        find_date(
            '<html><head><meta property="dc:created" content="2017-09-01"/></head><body></body></html>'
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><meta property="og:published_time" content="2017-09-01"/></head><body></body></html>',
            original_date=True,
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><meta name="last-modified" content="2017-09-01"/></head><body></body></html>',
            original_date=False,
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><meta property="OG:Updated_Time" content="2017-09-01"/></head><body></body></html>',
            extensive_search=False,
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><Meta Property="og:updated_time" content="2017-09-01"/></head><body></body></html>',
            extensive_search=False,
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><meta property="og:regDate" content="20210820030646"></head><body></body></html>',
            extensive_search=False,
        )
        == "2021-08-20"
    )
    assert (
        find_date(
            '<html><head><meta name="created" content="2017-01-09"/></head><body></body></html>'
        )
        == "2017-01-09"
    )
    assert (
        find_date(
            '<html><head><meta itemprop="copyrightyear" content="2017"/></head><body></body></html>'
        )
        == "2017-01-01"
    )

    # original date
    htmldoc = '<html><head><meta property="OG:Updated_Time" content="2017-09-01"/><meta property="OG:DatePublished" content="2017-07-02"/></head><body/></html>'
    assert (
        find_date(
            htmldoc,
            original_date=True,
        )
        == "2017-07-02"
    )
    assert (
        find_date(
            htmldoc,
            original_date=False,
        )
        == "2017-09-01"
    )
    htmldoc = """<html><head>
                 <meta property="article:modified_time" content="2021-04-06T06:32:14+00:00" />
                 <meta property="article:published_time" content="2020-07-21T00:17:28+00:00" />
                 </head><body/></html>"""
    assert find_date(htmldoc, original_date=True) == "2020-07-21"
    assert find_date(htmldoc, original_date=False) == "2021-04-06"

    htmldoc = """<html><head>
                 <meta property="article:published_time" content="2020-07-21T00:17:28+00:00" />
                 <meta property="article:modified_time" content="2021-04-06T06:32:14+00:00" />
                 </head><body/></html>"""
    assert find_date(htmldoc, original_date=True) == "2020-07-21"
    assert find_date(htmldoc, original_date=False) == "2021-04-06"

    ## meta in header
    assert find_date("<html><head><meta/></head><body></body></html>") is None

    assert (
        find_date(
            '<html><head><meta name="og:url" content="http://www.example.com/2018/02/01/entrytitle"/></head><body></body></html>'
        )
        == "2018-02-01"
    )
    assert (
        find_date(
            '<html><head><meta itemprop="datecreated" datetime="2018-02-02"/></head><body></body></html>'
        )
        == "2018-02-02"
    )
    assert (
        find_date(
            '<html><head><meta itemprop="datemodified" content="2018-02-04"/></head><body></body></html>'
        )
        == "2018-02-04"
    )
    assert (
        find_date(
            '<html><head><meta http-equiv="last-modified" content="2018-02-05"/></head><body></body></html>'
        )
        == "2018-02-05"
    )
    assert (
        find_date(
            '<html><head><meta name="lastmodified" content="2018-02-05"/></head><body></body></html>',
            original_date=True,
        )
        == "2018-02-05"
    )
    assert (
        find_date(
            '<html><head><meta name="lastmodified" content="2018-02-05"/></head><body></body></html>',
            original_date=False,
        )
        == "2018-02-05"
    )
    assert (
        find_date(
            '<html><head><meta http-equiv="date" content="2017-09-01"/></head><body></body></html>',
            original_date=True,
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><meta http-equiv="last-modified" content="2018-10-01"/><meta http-equiv="date" content="2017-09-01"/></head><body></body></html>',
            original_date=True,
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><meta http-equiv="last-modified" content="2018-10-01"/><meta http-equiv="date" content="2017-09-01"/></head><body></body></html>',
            original_date=False,
        )
        == "2018-10-01"
    )
    assert (
        find_date(
            '<html><head><meta http-equiv="date" content="2017-09-01"/><meta http-equiv="last-modified" content="2018-10-01"/></head><body></body></html>',
            original_date=True,
        )
        == "2017-09-01"
    )
    assert (
        find_date(
            '<html><head><meta http-equiv="date" content="2017-09-01"/><meta http-equiv="last-modified" content="2018-10-01"/></head><body></body></html>',
            original_date=False,
        )
        == "2018-10-01"
    )
    assert (
        find_date(
            '<html><head><meta name="Publish_Date" content="02.02.2004"/></head><body></body></html>'
        )
        == "2004-02-02"
    )
    assert (
        find_date(
            '<html><head><meta name="pubDate" content="2018-02-06"/></head><body></body></html>'
        )
        == "2018-02-06"
    )
    assert (
        find_date(
            '<html><head><meta pubdate="pubDate" content="2018-02-06"/></head><body></body></html>'
        )
        == "2018-02-06"
    )
    assert (
        find_date(
            '<html><head><meta itemprop="DateModified" datetime="2018-02-06"/></head><body></body></html>'
        )
        == "2018-02-06"
    )
    assert (
        find_date(
            '<html><head><meta name="DC.Issued" content="2021-07-13"/></head><body></body></html>'
        )
        == "2021-07-13"
    )
    assert (
        find_date(
            '<html><head><meta itemprop="dateUpdate" datetime="2018-02-06"/></head><body></body></html>',
            original_date=True,
        )
        == "2018-02-06"
    )
    assert (
        find_date(
            '<html><head><meta itemprop="dateUpdate" datetime="2018-02-06"/></head><body></body></html>',
            original_date=False,
        )
        == "2018-02-06"
    )

    ## time in document body
    assert find_date('<html><body><time datetime="08:00"></body></html>') is None
    assert (
        find_date('<html><body><time datetime="2014-07-10 08:30:45.687"></body></html>')
        == "2014-07-10"
    )
    assert (
        find_date(
            '<html><head></head><body><time class="entry-time" itemprop="datePublished" datetime="2018-04-18T09:57:38+00:00"></body></html>'
        )
        == "2018-04-18"
    )
    assert (
        find_date(
            '<html><body><footer class="article-footer"><p class="byline">Veröffentlicht am <time class="updated" datetime="2019-01-03T14:56:51+00:00">3. Januar 2019 um 14:56 Uhr.</time></p></footer></body></html>'
        )
        == "2019-01-03"
    )
    assert (
        find_date(
            '<html><body><footer class="article-footer"><p class="byline">Veröffentlicht am <time class="updated" datetime="2019-01-03T14:56:51+00:00"></time></p></footer></body></html>'
        )
        == "2019-01-03"
    )
    assert (
        find_date(
            '<html><body><time datetime="2011-09-27" class="entry-date"></time><time datetime="2011-09-28" class="updated"></time></body></html>',
            original_date=True,
        )
        == "2011-09-27"
    )
    # updated vs original in time elements
    assert (
        find_date(
            '<html><body><time datetime="2011-09-27" class="entry-date"></time><time datetime="2011-09-28" class="updated"></time></body></html>',
            original_date=False,
        )
        == "2011-09-28"
    )
    assert (
        find_date(
            '<html><body><time datetime="2011-09-28" class="updated"></time><time datetime="2011-09-27" class="entry-date"></time></body></html>',
            original_date=True,
        )
        == "2011-09-27"
    )
    assert (
        find_date(
            '<html><body><time datetime="2011-09-28" class="updated"></time><time datetime="2011-09-27" class="entry-date"></time></body></html>',
            original_date=False,
        )
        == "2011-09-28"
    )
    # removed from HTML5 https://www.w3schools.com/TAgs/att_time_datetime_pubdate.asp
    assert (
        find_date(
            '<html><body><time datetime="2011-09-28" pubdate="pubdate"></time></body></html>',
            original_date=False,
        )
        == "2011-09-28"
    )
    assert (
        find_date(
            '<html><body><time datetime="2011-09-28" pubdate="pubdate"></time></body></html>',
            original_date=True,
        )
        == "2011-09-28"
    )
    assert (
        find_date(
            '<html><body><time datetime="2011-09-28" class="entry-date"></time></body></html>',
            original_date=True,
        )
        == "2011-09-28"
    )
    # bug #54
    assert (
        find_date(
            '<html><body><time class="Feed-module--feed__item-meta-time--3t1fg" dateTime="November 29, 2020">November 2020</time></body></html>',
            outputformat="%Y-%m-%d %H:%m:%S",
        )
        == "2020-11-29 00:11:00"  # 00:11 unclear
    )
    assert (
        find_date(
            '<html><body><time class="Feed-module--feed__item-meta-time--3t1fg" dateTime="November 29, 2020">November 2020</time></body></html>',
            outputformat="%Y-%m-%d",
        )
        == "2020-11-29"
    )

    ## precise pattern in document body
    assert (
        find_date(
            '<html><body><font size="2" face="Arial,Geneva,Helvetica">Bei <a href="../../sonstiges/anfrage.php"><b>Bestellungen</b></a> bitte Angabe der Titelnummer nicht vergessen!<br><br>Stand: 03.04.2019</font></body></html>'
        )
        == "2019-04-03"
    )
    assert (
        find_date(
            "<html><body><div>Erstausstrahlung: 01.01.2020</div><div>Preisstand: 03.02.2022 03:00 GMT+1</div></body></html>"
        )
        == "2022-02-03"
    )
    assert find_date("<html><body>Datum: 10.11.2017</body></html>") == "2017-11-10"

    # abbr in document body
    assert (
        find_date(
            '<html><body><abbr class="published">am 12.11.16</abbr></body></html>',
            original_date=False,
        )
        == "2016-11-12"
    )
    assert (
        find_date(
            '<html><body><abbr class="published">am 12.11.16</abbr></body></html>',
            original_date=True,
        )
        == "2016-11-12"
    )
    assert (
        find_date(
            '<html><body><abbr class="published" title="2016-11-12">XYZ</abbr></body></html>',
            original_date=True,
        )
        == "2016-11-12"
    )
    assert (
        find_date(
            '<html><body><abbr class="date-published">8.11.2016</abbr></body></html>'
        )
        == "2016-11-08"
    )
    # valid vs. invalid data-utime
    assert (
        find_date(
            '<html><body><abbr data-utime="1438091078" class="something">A date</abbr></body></html>'
        )
        == "2015-07-28"
    )
    assert (
        find_date(
            '<html><body><abbr data-utime="143809-1078" class="something">A date</abbr></body></html>'
        )
        is None
    )

    # time in document body
    assert (
        find_date("<html><body><time>2018-01-04</time></body></html>") == "2018-01-04"
    )

    # additional XPath expressions
    assert (
        find_date('<html><body><div class="fecha">2018-01-04</div></body></html>')
        == "2018-01-04"
    )

    ## other expressions in document body
    assert (
        find_date('<html><body>"datePublished":"2018-01-04"</body></html>')
        == "2018-01-04"
    )
    assert find_date("<html><body>Stand: 1.4.18</body></html>") == "2018-04-01"

    # free text
    assert find_date("<html><body>&copy; 2017</body></html>") == "2017-01-01"
    assert find_date("<html><body>© 2017</body></html>") == "2017-01-01"
    assert (
        find_date(
            "<html><body><p>Dieses Datum ist leider ungültig: 30. Februar 2018.</p></body></html>",
            extensive_search=False,
        )
        is None
    )
    assert (
        find_date(
            "<html><body><p>Dieses Datum ist leider ungültig: 30. Februar 2018.</p></body></html>"
        )
        == "2018-01-01"
    )

    # URL in IMG
    # header
    assert (
        find_date(
            '<html><meta property="og:image" content="https://example.org/img/2019-05-05/test.jpg"><body></body></html>'
        )
        == "2019-05-05"
    )
    assert (
        find_date(
            '<html><meta property="og:image" content="https://example.org/img/test.jpg"><body></body></html>'
        )
        is None
    )
    # body
    assert (
        find_date(
            '<html><body><img src="https://example.org/img/2019-05-05/test.jpg"/></body></html>'
        )
        == "2019-05-05"
    )
    assert (
        find_date(
            '<html><body><img src="https://example.org/img/test.jpg"/></body></html>'
        )
        is None
    )
    assert (
        find_date(
            '<html><body><img src="https://example.org/img/2019-05-03/test.jpg"/><img src="https://example.org/img/2019-05-04/test.jpg"/><img src="https://example.org/img/2019-05-05/test.jpg"/></body></html>'
        )
        == "2019-05-05"
    )
    assert (
        find_date(
            '<html><body><img src="https://example.org/img/2019-05-05/test.jpg"/><img src="https://example.org/img/2019-05-04/test.jpg"/><img src="https://example.org/img/2019-05-03/test.jpg"/></body></html>'
        )
        == "2019-05-05"
    )
    # in title
    assert (
        find_date(
            "<html><head><title>Bericht zur Coronalage vom 22.04.2020 – worauf wartet die Politik? – DIE ACHSE DES GUTEN. ACHGUT.COM</title></head></html>"
        )
        == "2020-04-22"
    )
    # in unknown div
    assert (
        find_date(
            '<html><body><div class="spip spip-block-right" style="text-align:right;">Le 26 juin 2019</div></body></html>',
            extensive_search=False,
        )
        is None
    )
    assert (
        find_date(
            '<html><body><div class="spip spip-block-right" style="text-align:right;">Le 26 juin 2019</div></body></html>',
            extensive_search=True,
        )
        == "2019-06-26"
    )
    # in link title
    assert (
        find_date(
            '<html><body><a class="ribbon date " title="12th December 2018" href="https://example.org/" itemprop="url">Text</a></body></html>'
        )
        == "2018-12-12"
    )

    # min_date parameter
    assert (
        find_date(
            '<html><meta><meta property="article:published_time" content="1991-01-02T01:01:00+01:00"></meta><body></body></html>',
            min_date="2000-01-01",
        )
        is None
    )
    assert (
        find_date(
            '<html><meta><meta property="article:published_time" content="1991-01-02T01:01:00+01:00"></meta><body></body></html>',
            min_date="1990-01-01",
        )
        == "1991-01-02"
    )
    assert (
        find_date(
            '<html><meta><meta property="article:published_time" content="1991-01-02T01:01:00+00:00"></meta><body></body></html>',
            min_date="1991-01-02T01:02:00+00:00",
        )
        is None
    )
    assert (
        find_date(
            '<html><meta><meta property="article:published_time" content="1991-01-02T01:01:00+00:00"></meta><body></body></html>',
            min_date="1991-01-02T01:00:00+00:00",
        )
        == "1991-01-02"
    )

    # wild text in body
    assert (
        find_date("<html><body>Wed, 19 Oct 2022 14:24:05 +0000</body></html>")
        == "2022-10-19"
    )


def test_is_valid_date():
    """test internal date validation"""
    assert (
        is_valid_date(
            "2016-01-01", OUTPUTFORMAT, earliest=MIN_DATE, latest=LATEST_POSSIBLE
        )
        is True
    )
    assert (
        is_valid_date(
            "1998-08-08", OUTPUTFORMAT, earliest=MIN_DATE, latest=LATEST_POSSIBLE
        )
        is True
    )
    assert (
        is_valid_date(
            "2001-12-31", OUTPUTFORMAT, earliest=MIN_DATE, latest=LATEST_POSSIBLE
        )
        is True
    )
    assert (
        is_valid_date(
            "1992-07-30", OUTPUTFORMAT, earliest=MIN_DATE, latest=LATEST_POSSIBLE
        )
        is False
    )
    assert (
        is_valid_date(
            "1901-13-98", OUTPUTFORMAT, earliest=MIN_DATE, latest=LATEST_POSSIBLE
        )
        is False
    )
    assert (
        is_valid_date("202-01", OUTPUTFORMAT, earliest=MIN_DATE, latest=LATEST_POSSIBLE)
        is False
    )
    assert (
        is_valid_date("1922", "%Y", earliest=MIN_DATE, latest=LATEST_POSSIBLE) is False
    )
    assert (
        is_valid_date("2004", "%Y", earliest=MIN_DATE, latest=LATEST_POSSIBLE) is True
    )
    assert (
        is_valid_date(
            "1991-01-02",
            OUTPUTFORMAT,
            earliest=datetime.datetime(1990, 1, 1),
            latest=LATEST_POSSIBLE,
        )
        is True
    )
    assert (
        is_valid_date(
            "1991-01-02",
            OUTPUTFORMAT,
            earliest=datetime.datetime(1992, 1, 1),
            latest=LATEST_POSSIBLE,
        )
        is False
    )
    assert (
        is_valid_date(
            "1991-01-02",
            OUTPUTFORMAT,
            earliest=MIN_DATE,
            latest=datetime.datetime(1990, 1, 1),
        )
        is False
    )
    assert (
        is_valid_date(
            "1991-01-02",
            OUTPUTFORMAT,
            earliest=datetime.datetime(1990, 1, 1),
            latest=datetime.datetime(1995, 1, 1),
        )
        is True
    )
    assert (
        is_valid_date(
            "1991-01-02",
            OUTPUTFORMAT,
            earliest=datetime.datetime(1990, 1, 1),
            latest=datetime.datetime(1990, 12, 31),
        )
        is False
    )


def test_convert_date():
    """test date conversion"""
    assert convert_date("2016-11-18", "%Y-%m-%d", "%d %B %Y") == "18 November 2016"
    assert convert_date("18 November 2016", "%d %B %Y", "%Y-%m-%d") == "2016-11-18"
    dateobject = datetime.datetime.strptime("2016-11-18", "%Y-%m-%d")
    assert convert_date(dateobject, "%d %B %Y", "%Y-%m-%d") == "2016-11-18"


def test_try_date_expr():
    """test date extraction via external package"""
    assert try_date_expr(None, OUTPUTFORMAT, False, MIN_DATE, LATEST_POSSIBLE) is None

    find_date.extensive_search = False
    assert (
        try_date_expr(
            "Fri, Sept 1, 2017", OUTPUTFORMAT, False, MIN_DATE, LATEST_POSSIBLE
        )
        is None
    )

    find_date.extensive_search = True
    assert (
        try_date_expr(
            "Friday, September 01, 2017", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE
        )
        == "2017-09-01"
    )
    assert (
        try_date_expr(
            "Fr, 1 Sep 2017 16:27:51 MESZ",
            OUTPUTFORMAT,
            True,
            MIN_DATE,
            LATEST_POSSIBLE,
        )
        == "2017-09-01"
    )
    assert (
        try_date_expr(
            "Freitag, 01. September 2017", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE
        )
        == "2017-09-01"
    )
    assert (
        try_date_expr(
            "Am 1. September 2017 um 15:36 Uhr schrieb",
            OUTPUTFORMAT,
            True,
            MIN_DATE,
            LATEST_POSSIBLE,
        )
        == "2017-09-01"
    )
    assert (
        try_date_expr(
            "Fri - September 1 - 2017", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE
        )
        == "2017-09-01"
    )
    assert (
        try_date_expr("1.9.2017", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE)
        == "2017-09-01"
    )
    assert (
        try_date_expr("1/9/17", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE)
        == "2017-09-01"
    )  # assuming MDY format
    assert (
        try_date_expr("201709011234", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE)
        == "2017-09-01"
    )
    # other output format
    assert (
        try_date_expr("1.9.2017", "%d %B %Y", True, MIN_DATE, LATEST_POSSIBLE)
        == "01 September 2017"
    )
    # wrong
    assert try_date_expr("201", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE) is None
    assert (
        try_date_expr("14:35:10", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE) is None
    )
    assert (
        try_date_expr("12:00 h", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE) is None
    )
    # date range
    assert (
        try_date_expr("2005-2006", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE)
        is None
    )
    # Mandarin
    assert (
        try_date_expr(
            "发布时间: 2022-02-25 14:34", OUTPUTFORMAT, True, MIN_DATE, LATEST_POSSIBLE
        )
        == "2022-02-25"
    )


# def test_header():
#    assert examine_header(tree, options)


def test_compare_reference():
    """test comparison function"""
    options = Extractor(False, LATEST_POSSIBLE, MIN_DATE, False, OUTPUTFORMAT)
    assert compare_reference(0, "AAAA", options) == 0
    assert compare_reference(1517500000, "2018-33-01", options) == 1517500000
    assert 1517400000 < compare_reference(0, "2018-02-01", options) < 1517500000
    assert compare_reference(1517500000, "2018-02-01", options) == 1517500000


def test_candidate_selection():
    """test the algorithm for several candidates"""
    options = Extractor(False, LATEST_POSSIBLE, MIN_DATE, False, OUTPUTFORMAT)
    # patterns
    catch = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})")
    yearpat = re.compile(r"^([0-9]{4})")
    # nonsense
    occurrences = Counter(
        [
            "20208956",
            "20208956",
            "20208956",
            "19018956",
            "209561",
            "22020895607-12",
            "2-28",
        ]
    )
    result = select_candidate(occurrences, catch, yearpat, options)
    assert result is None
    # plausible
    occurrences = Counter(
        [
            "2016-12-23",
            "2016-12-23",
            "2016-12-23",
            "2016-12-23",
            "2017-08-11",
            "2016-07-12",
            "2017-11-28",
        ]
    )
    result = select_candidate(occurrences, catch, yearpat, options)
    assert result.group(0) == "2017-11-28"

    options = Extractor(False, LATEST_POSSIBLE, MIN_DATE, True, OUTPUTFORMAT)
    result = select_candidate(occurrences, catch, yearpat, options)
    assert result.group(0) == "2016-07-12"
    # mix plausible/implausible
    occurrences = Counter(
        ["2116-12-23", "2116-12-23", "2116-12-23", "2017-08-11", "2017-08-11"]
    )
    result = select_candidate(occurrences, catch, yearpat, options)
    assert result.group(0) == "2017-08-11"

    options = Extractor(False, LATEST_POSSIBLE, MIN_DATE, False, OUTPUTFORMAT)
    occurrences = Counter(
        ["2116-12-23", "2116-12-23", "2116-12-23", "2017-08-11", "2017-08-11"]
    )
    result = select_candidate(occurrences, catch, yearpat, options)
    assert result.group(0) == "2017-08-11"
    # taking date present twice, corner case
    occurrences = Counter(
        ["2016-12-23", "2016-12-23", "2017-08-11", "2017-08-11", "2017-08-11"]
    )
    result = select_candidate(occurrences, catch, yearpat, options)
    assert result.group(0) == "2016-12-23"


def test_regex_parse():
    """test date extraction using rules and regular expressions"""
    assert regex_parse("3. Dezember 2008") is not None
    assert regex_parse("33. Dezember 2008") is None
    assert regex_parse("3. Dez 2008") is not None
    assert regex_parse("3 dez 2008") is not None
    assert regex_parse("3 Aralık 2008 Çarşamba") is not None
    assert regex_parse("3 Aralık 2008") is not None
    assert regex_parse("Tuesday, March 26th, 2019") is not None
    assert regex_parse("March 26, 2019") is not None
    assert regex_parse("3rd Tuesday in March") is None
    assert regex_parse("Mart 26, 2019") is not None
    assert regex_parse("Salı, Mart 26, 2019") is not None
    assert regex_parse("36/14/2016") is None
    assert regex_parse("January 36 1998") is None
    assert (
        custom_parse("January 12 1098", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    )
    assert custom_parse("1998-01", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    assert custom_parse("01-1998", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    assert custom_parse("13-1998", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    assert custom_parse("10.10.98", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    assert custom_parse("12122004", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    assert (
        custom_parse("3/14/2016", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    )
    assert custom_parse("20041212", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    assert custom_parse("20041212", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    assert custom_parse("1212-20-04", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    assert custom_parse("1212-20-04", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    assert (
        custom_parse("2004-12-12", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    )
    assert (
        custom_parse("2004-12-12", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    )
    assert custom_parse("33.20.2004", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    assert (
        custom_parse("12.12.2004", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is not None
    )
    assert custom_parse("2019 28 meh", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    assert custom_parse("2019 28 meh", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE) is None
    # regex-based matches
    assert (
        custom_parse("abcd 20041212 efgh", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE)
        is not None
    )
    assert (
        custom_parse("abcd 2004-2-12 efgh", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE)
        is not None
    )
    assert (
        custom_parse("abcd 2004-2 efgh", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE)
        is not None
    )
    assert (
        custom_parse("abcd 2004-2 efgh", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE)
        is not None
    )
    assert (
        custom_parse(
            "abcd 32. Januar 2020 efgh", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE
        )
        is None
    )
    # plausible but impossible dates
    assert (
        custom_parse("February 29 2008", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE)
        == "2008-02-29"
    )
    assert (
        custom_parse("February 30 2008", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE)
        is None
    )
    assert (
        custom_parse(
            "XXTag, den 29. Februar 2008", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE
        )
        == "2008-02-29"
    )
    assert (
        custom_parse(
            "XXTag, den 30. Februar 2008", OUTPUTFORMAT, MIN_DATE, LATEST_POSSIBLE
        )
        is None
    )
    # for Nones caused by newlines and duplicates
    assert regex_parse("January 1st, 1998") is not None
    assert regex_parse("February 1st, 1998") is not None
    assert regex_parse("March 1st, 1998") is not None
    assert regex_parse("April 1st, 1998") is not None
    assert regex_parse("May 1st, 1998") is not None
    assert regex_parse("June 1st, 1998") is not None
    assert regex_parse("July 1st, 1998") is not None
    assert regex_parse("August 1st, 1998") is not None
    assert regex_parse("September 1st, 1998") is not None
    assert regex_parse("October 1st, 1998") is not None
    assert regex_parse("November 1st, 1998") is not None
    assert regex_parse("December 1st, 1998") is not None
    assert regex_parse("Jan 1st, 1998") is not None
    assert regex_parse("Feb 1st, 1998") is not None
    assert regex_parse("Mar 1st, 1998") is not None
    assert regex_parse("Apr 1st, 1998") is not None
    assert regex_parse("Jun 1st, 1998") is not None
    assert regex_parse("Jul 1st, 1998") is not None
    assert regex_parse("Aug 1st, 1998") is not None
    assert regex_parse("Sep 1st, 1998") is not None
    assert regex_parse("Oct 1st, 1998") is not None
    assert regex_parse("Nov 1st, 1998") is not None
    assert regex_parse("Dec 1st, 1998") is not None
    assert regex_parse("Januar 1, 1998") is not None
    assert regex_parse("Jänner 1, 1998") is not None
    assert regex_parse("Februar 1, 1998") is not None
    assert regex_parse("Feber 1, 1998") is not None
    assert regex_parse("März 1, 1998") is not None
    assert regex_parse("April 1, 1998") is not None
    assert regex_parse("Mai 1, 1998") is not None
    assert regex_parse("Juni 1, 1998") is not None
    assert regex_parse("Juli 1, 1998") is not None
    assert regex_parse("August 1, 1998") is not None
    assert regex_parse("September 1, 1998") is not None
    assert regex_parse("Oktober 1, 1998") is not None
    assert regex_parse("1. Okt. 1998") is not None
    assert regex_parse("November 1, 1998") is not None
    assert regex_parse("Dezember 1, 1998") is not None
    assert regex_parse("Ocak 1, 1998") is not None
    assert regex_parse("Şubat 1, 1998") is not None
    assert regex_parse("Mart 1, 1998") is not None
    assert regex_parse("Nisan 1, 1998") is not None
    assert regex_parse("Mayıs 1, 1998") is not None
    assert regex_parse("Haziran 1, 1998") is not None
    assert regex_parse("Temmuz 1, 1998") is not None
    assert regex_parse("Ağustos 1, 1998") is not None
    assert regex_parse("Eylül 1, 1998") is not None
    assert regex_parse("Ekim 1, 1998") is not None
    assert regex_parse("Kasım 1, 1998") is not None
    assert regex_parse("Aralık 1, 1998") is not None
    assert regex_parse("Oca 1, 1998") is not None
    assert regex_parse("Şub 1, 1998") is not None
    assert regex_parse("Mar 1, 1998") is not None
    assert regex_parse("Nis 1, 1998") is not None
    assert regex_parse("May 1, 1998") is not None
    assert regex_parse("Haz 1, 1998") is not None
    assert regex_parse("Tem 1, 1998") is not None
    assert regex_parse("Ağu 1, 1998") is not None
    assert regex_parse("Eyl 1, 1998") is not None
    assert regex_parse("Eki 1, 1998") is not None
    assert regex_parse("Kas 1, 1998") is not None
    assert regex_parse("Ara 1, 1998") is not None
    assert regex_parse("1 January 1998") is not None
    assert regex_parse("1 February 1998") is not None
    assert regex_parse("1 March 1998") is not None
    assert regex_parse("1 April 1998") is not None
    assert regex_parse("1 May 1998") is not None
    assert regex_parse("1 June 1998") is not None
    assert regex_parse("1 July 1998") is not None
    assert regex_parse("1 August 1998") is not None
    assert regex_parse("1 September 1998") is not None
    assert regex_parse("1 October 1998") is not None
    assert regex_parse("1 November 1998") is not None
    assert regex_parse("1 December 1998") is not None
    assert regex_parse("1 Jan 1998") is not None
    assert regex_parse("1 Feb 1998") is not None
    assert regex_parse("1 Mar 1998") is not None
    assert regex_parse("1 Apr 1998") is not None
    assert regex_parse("1 Jun 1998") is not None
    assert regex_parse("1 Jul 1998") is not None
    assert regex_parse("1 Aug 1998") is not None
    assert regex_parse("1 Sep 1998") is not None
    assert regex_parse("1 Oct 1998") is not None
    assert regex_parse("1 Nov 1998") is not None
    assert regex_parse("1 Dec 1998") is not None
    assert regex_parse("1 Januar 1998") is not None
    assert regex_parse("1 Jänner 1998") is not None
    assert regex_parse("1 Februar 1998") is not None
    assert regex_parse("1 Feber 1998") is not None
    assert regex_parse("1 März 1998") is not None
    assert regex_parse("1 April 1998") is not None
    assert regex_parse("1 Mai 1998") is not None
    assert regex_parse("1 Juni 1998") is not None
    assert regex_parse("1 Juli 1998") is not None
    assert regex_parse("1 August 1998") is not None
    assert regex_parse("1 September 1998") is not None
    assert regex_parse("1 Oktober 1998") is not None
    assert regex_parse("1 November 1998") is not None
    assert regex_parse("1 Dezember 1998") is not None
    assert regex_parse("1 Ocak 1998") is not None
    assert regex_parse("1 Şubat 1998") is not None
    assert regex_parse("1 Mart 1998") is not None
    assert regex_parse("1 Nisan 1998") is not None
    assert regex_parse("1 Mayıs 1998") is not None
    assert regex_parse("1 Haziran 1998") is not None
    assert regex_parse("1 Temmuz 1998") is not None
    assert regex_parse("1 Ağustos 1998") is not None
    assert regex_parse("1 Eylül 1998") is not None
    assert regex_parse("1 Ekim 1998") is not None
    assert regex_parse("1 Kasım 1998") is not None
    assert regex_parse("1 Aralık 1998") is not None
    assert regex_parse("1 Oca 1998") is not None
    assert regex_parse("1 Şub 1998") is not None
    assert regex_parse("1 Mar 1998") is not None
    assert regex_parse("1 Nis 1998") is not None
    assert regex_parse("1 May 1998") is not None
    assert regex_parse("1 Haz 1998") is not None
    assert regex_parse("1 Tem 1998") is not None
    assert regex_parse("1 Ağu 1998") is not None
    assert regex_parse("1 Eyl 1998") is not None
    assert regex_parse("1 Eki 1998") is not None
    assert regex_parse("1 Kas 1998") is not None
    assert regex_parse("1 Ara 1998") is not None


def test_external_date_parser():
    """test external date parser"""
    assert (
        external_date_parser("Wednesday, January 1st 2020", OUTPUTFORMAT)
        == "2020-01-01"
    )
    assert external_date_parser("Random text with 2020", OUTPUTFORMAT) is None
    # https://github.com/scrapinghub/dateparser/issues/333
    assert external_date_parser("1 January 0001", "%d %B %Y") in (
        "01 January 1",
        "01 January 0001",
    )
    assert external_date_parser("1 January 1900", "%d %B %Y") == "01 January 1900"
    # https://github.com/scrapinghub/dateparser/issues/406
    assert (
        external_date_parser("2018-04-12 17:20:03.12345678999a", OUTPUTFORMAT)
        == "2018-12-04"
    )
    # https://github.com/scrapinghub/dateparser/issues/685
    assert external_date_parser("12345678912 days", OUTPUTFORMAT) is None
    # https://github.com/scrapinghub/dateparser/issues/680
    assert external_date_parser("2.2250738585072011e-308", OUTPUTFORMAT) is None
    assert external_date_parser("⁰⁴⁵₀₁₂", OUTPUTFORMAT) is None


def test_url():
    """test url parameter"""
    assert (
        find_date(
            "<html><body><p>Aaa, bbb.</p></body></html>",
            url="http://example.com/category/2016/07/12/key-words",
        )
        == "2016-07-12"
    )
    assert (
        find_date(
            "<html><body><p>Aaa, bbb.</p></body></html>",
            url="http://example.com/2016/key-words",
        )
        is None
    )
    assert (
        find_date(
            "<html><body><p>Aaa, bbb.</p></body></html>",
            url="http://www.kreditwesen.org/widerstand-berlin/2012-11-29/keine-kurzung-bei-der-jugend-klubs-konnen-vorerst-aufatmen-bvv-beschliest-haushaltsplan/",
        )
        == "2012-11-29"
    )
    assert (
        find_date(
            "<html><body><p>Aaa, bbb.</p></body></html>",
            url="http://www.kreditwesen.org/widerstand-berlin/6666-42-87/",
        )
        is None
    )
    assert (
        find_date(
            "<html><body><p>Z.</p></body></html>",
            url="https://www.pamelaandersonfoundation.org/news/2019/6/26/dm4wjh7skxerzzw8qa8cklj8xdri5j",
        )
        == "2019-06-26"
    )


def test_approximate_url():
    """test url parameter"""
    assert (
        find_date(
            "<html><body><p>Aaa, bbb.</p></body></html>",
            url="http://example.com/category/2016/",
        )
        is None
    )


def test_search_pattern():
    """test pattern search in strings"""
    options = Extractor(True, LATEST_POSSIBLE, MIN_DATE, False, OUTPUTFORMAT)
    pattern = re.compile(r"\D([0-9]{4}[/.-][0-9]{2})\D")
    catch = re.compile(r"([0-9]{4})[/.-]([0-9]{2})")
    yearpat = re.compile(r"^([12][0-9]{3})")
    assert (
        search_pattern(
            "It happened on the 202.E.19, the day when it all began.",
            pattern,
            catch,
            yearpat,
            options,
        )
        is None
    )
    assert (
        search_pattern(
            "The date is 2002.02.15.",
            pattern,
            catch,
            yearpat,
            options,
        )
        is not None
    )
    assert (
        search_pattern(
            "http://www.url.net/index.html",
            pattern,
            catch,
            yearpat,
            options,
        )
        is None
    )
    assert (
        search_pattern(
            "http://www.url.net/2016/01/index.html",
            pattern,
            catch,
            yearpat,
            options,
        )
        is not None
    )
    #
    pattern = re.compile(r"\D([0-9]{2}[/.-][0-9]{4})\D")
    catch = re.compile(r"([0-9]{2})[/.-]([0-9]{4})")
    yearpat = re.compile(r"([12][0-9]{3})$")
    assert (
        search_pattern(
            "It happened on the 202.E.19, the day when it all began.",
            pattern,
            catch,
            yearpat,
            options,
        )
        is None
    )
    assert (
        search_pattern(
            "It happened on the 15.02.2002, the day when it all began.",
            pattern,
            catch,
            yearpat,
            options,
        )
        is not None
    )
    #
    pattern = re.compile(r"\D(2[01][0-9]{2})\D")
    catch = re.compile(r"(2[01][0-9]{2})")
    yearpat = re.compile(r"^(2[01][0-9]{2})")
    assert (
        search_pattern(
            "It happened in the film 300.",
            pattern,
            catch,
            yearpat,
            options,
        )
        is None
    )
    assert (
        search_pattern(
            "It happened in 2002.",
            pattern,
            catch,
            yearpat,
            options,
        )
        is not None
    )


def test_search_html():
    "Test the pattern search in raw HTML"
    options = Extractor(True, LATEST_POSSIBLE, MIN_DATE, False, OUTPUTFORMAT)
    # tree input
    assert (
        search_page("<html><body><p>The date is 5/2010</p></body></html>", options)
        == "2010-05-01"
    )
    assert (
        search_page("<html><body><p>The date is 5.5.2010</p></body></html>", options)
        == "2010-05-05"
    )
    assert (
        search_page("<html><body><p>The date is 11/10/99</p></body></html>", options)
        == "1999-10-11"
    )
    assert (
        search_page("<html><body><p>The date is 3/3/11</p></body></html>", options)
        == "2011-03-03"
    )
    assert (
        search_page("<html><body><p>The date is 06.12.06</p></body></html>", options)
        == "2006-12-06"
    )
    assert (
        search_page(
            "<html><body><p>The timestamp is 20140915D15:23H</p></body></html>", options
        )
        == "2014-09-15"
    )
    options = Extractor(True, LATEST_POSSIBLE, MIN_DATE, True, OUTPUTFORMAT)
    assert (
        search_page(
            "<html><body><p>It could be 2015-04-30 or 2003-11-24.</p></body></html>",
            options,
        )
        == "2003-11-24"
    )
    options = Extractor(True, LATEST_POSSIBLE, MIN_DATE, False, OUTPUTFORMAT)
    assert (
        search_page(
            "<html><body><p>It could be 2015-04-30 or 2003-11-24.</p></body></html>",
            options,
        )
        == "2015-04-30"
    )
    assert (
        search_page(
            "<html><body><p>It could be 03/03/2077 or 03/03/2013.</p></body></html>",
            options,
        )
        == "2013-03-03"
    )
    assert (
        search_page(
            "<html><body><p>It could not be 03/03/2077 or 03/03/1988.</p></body></html>",
            options,
        )
        is None
    )
    assert (
        search_page(
            "<html><body><p>© The Web Association 2013.</p></body></html>", options
        )
        == "2013-01-01"
    )
    assert (
        search_page("<html><body><p>Next © Copyright 2018</p></body></html>", options)
        == "2018-01-01"
    )
    assert (
        search_page("<html><body><p> © Company 2014-2019 </p></body></html>", options)
        == "2019-01-01"
    )
    assert (
        search_page(
            "<html><body><p> &copy; Copyright 1999-2020 Asia Pacific Star. All rights reserved.</p></body></html>",
            options,
        )
        == "2020-01-01"
    )
    assert (
        search_page(
            '<html><head><link xmlns="http://www.w3.org/1999/xhtml"/></head></html>',
            options,
        )
        is None
    )
    assert (
        search_page(
            '<html><body><link href="//homepagedesigner.telekom.de/.cm4all/res/static/beng-editor/5.1.98/css/deploy.css"/></body></html>',
            options,
        )
        is None
    )


def test_idiosyncrasies():
    assert (
        find_date("<html><body><p><em>Last updated: 5/5/20</em></p></body></html>")
        == "2020-05-05"
    )
    assert (
        find_date("<html><body><p><em>Last updated: 01/23/2021</em></p></body></html>")
        == "2021-01-23"
    )
    assert (
        find_date("<html><body><p><em>Last updated: 01/23/21</em></p></body></html>")
        == "2021-01-23"
    )
    assert (
        find_date("<html><body><p><em>Last updated: 1/23/21</em></p></body></html>")
        == "2021-01-23"
    )
    assert (
        find_date("<html><body><p><em>Last updated: 23/1/21</em></p></body></html>")
        == "2021-01-23"
    )
    assert (
        find_date("<html><body><p><em>Last updated: 23/01/21</em></p></body></html>")
        == "2021-01-23"
    )
    assert (
        find_date("<html><body><p><em>Last updated: 23/01/2021</em></p></body></html>")
        == "2021-01-23"
    )
    assert (
        find_date("<html><body><p><em>Last updated: 33/23/3033</em></p></body></html>")
        is None
    )
    assert (
        find_date("<html><body><p><em>Published: 5/5/2020</em></p></body></html>")
        == "2020-05-05"
    )
    assert (
        find_date("<html><body><p><em>Published in: 05.05.2020</em></p></body></html>")
        == "2020-05-05"
    )
    assert (
        find_date("<html><body><p><em>Son güncelleme: 5/5/20</em></p></body></html>")
        == "2020-05-05"
    )
    assert (
        find_date("<html><body><p><em>Son güncellenme: 5/5/2020</em></p></body></html>")
        == "2020-05-05"
    )
    assert (
        find_date(
            "<html><body><p><em>Yayımlama tarihi: 05.05.2020</em></p></body></html>"
        )
        == "2020-05-05"
    )
    assert (
        find_date(
            "<html><body><p><em>Son güncelleme tarihi: 5/5/20</em></p></body></html>"
        )
        == "2020-05-05"
    )
    assert (
        find_date(
            "<html><body><p><em>5/5/20 tarihinde güncellendi.</em></p></body></html>"
        )
        == "2020-05-05"
    )
    assert (
        find_date(
            """<html><body><p><em>5/5/20'de güncellendi.</em></p></body></html>"""
        )
        == "2020-05-05"
    )
    assert (
        find_date(
            "<html><body><p><em>5/5/2020 tarihinde yayımlandı.</em></p></body></html>"
        )
        == "2020-05-05"
    )
    assert (
        find_date(
            "<html><body><p><em>05.05.2020 tarihinde yayınlandı.</em></p></body></html>"
        )
        == "2020-05-05"
    )
    assert (
        find_date(
            "<html><body><p>veröffentlicht am 6.12.06</p></body></html>",
        )
        == "2006-12-06"
    )


def test_parser():
    """test argument parsing for the command-line interface"""
    testargs = [
        "-f",
        "-v",
        "--original",
        "-max",
        "2015-12-31",
        "-u",
        "https://www.example.org",
    ]
    with patch.object(sys, "argv", testargs):
        args = parse_args(testargs)
    assert args.fast is True
    assert args.original is True
    assert args.verbose is True
    assert args.maxdate == "2015-12-31"
    assert args.URL == "https://www.example.org"
    testargs = ["-f", "-min", "2015-12-31"]
    with patch.object(sys, "argv", testargs):
        args = parse_args(testargs)
    assert args.fast is True
    assert args.original is False
    assert args.verbose is False
    assert args.mindate == "2015-12-31"
    # version
    f = io.StringIO()
    testargs = ["", "--version"]
    with pytest.raises(SystemExit) as e, redirect_stdout(f):
        with patch.object(sys, "argv", testargs):
            args = parse_args(testargs)
    assert e.type == SystemExit and e.value.code == 0
    assert re.match(
        r"Htmldate [0-9]\.[0-9]+\.[0-9] - Python [0-9]\.[0-9]+\.[0-9]", f.getvalue()
    )


@pytest.mark.skip
def test_cli():
    "Test the command-line interface"
    testargs = ["--original"]
    with patch.object(sys, "argv", testargs):
        args = parse_args(testargs)

    assert cli_examine(None, args) is None
    assert cli_examine(" ", args) is None
    assert cli_examine("0" * int(10e7), args) is None

    args.fast = True
    assert cli_examine(" ", args) is None
    assert cli_examine("0" * int(10e7), args) is None

    args.fast = False
    assert (
        cli_examine(
            '<?xml version="1.0" encoding="utf-8"?><!DOCTYPE html><html lang="en"><head><meta charset="utf-8"/><meta http-equiv="Content-Type" content="text/html; charset=utf-8"/></head><body><p>A</p><p>B</p></body></html>',
            args,
        )
        is None
    )
    assert (
        cli_examine(
            '<html><body><span class="entry-date">12. Juli 2016</span></body></html>',
            args,
        )
        == "2016-07-12"
    )
    assert cli_examine("<html><body>2016-07-12</body></html>", args) == "2016-07-12"

    args.maxdate = "2015-01-01"
    assert (
        cli_examine(
            "<html><body>2016-07-12</body></html>",
            args,
        )
        is None
    )

    args.maxdate = "2017-12-31"
    assert (
        cli_examine(
            "<html><body>2016-07-12</body></html>",
            args,
        )
        == "2016-07-12"
    )

    args.maxdate = "2017-41-41"
    assert (
        cli_examine(
            "<html><body>2016-07-12</body></html>",
            args,
        )
        == "2016-07-12"
    )

    # first test
    testargs = ["", "-u", "123", "-v"]
    with patch.object(sys, "argv", testargs):
        args = parse_args(testargs)
    with pytest.raises(SystemExit) as err:
        process_args(args)
    assert err.type == SystemExit
    # meaningful test
    testargs = ["", "-u", "https://httpbun.com/html"]
    with patch.object(sys, "argv", testargs):
        args = parse_args(testargs)
    f = io.StringIO()
    with redirect_stdout(f):
        process_args(args)
    assert len(f.getvalue()) == 0
    # second test
    args.URL = None
    args.inputfile = os.path.join(TEST_DIR, "testlist.txt")
    f = io.StringIO()
    with redirect_stdout(f):
        process_args(args)
    assert f.getvalue() == "https://httpbun.com/html\tNone\n"


@pytest.mark.skip
def test_download():
    """test page download"""
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["", "-u", "https://httpbin.org/status/404"]):
            main()

    testargs = ["--original"]
    with patch.object(sys, "argv", testargs):
        args = parse_args(testargs)

    url = "https://httpbin.org/status/200"
    teststring = fetch_url(url)
    assert teststring is None
    assert cli_examine(teststring, args) is None
    url = "https://httpbun.com/links/2/2"
    teststring = fetch_url(url)
    assert teststring is not None
    assert cli_examine(teststring, args) is None
    url = "https://httpbun.com/html"
    teststring = fetch_url(url)
    assert teststring is not None
    assert cli_examine(teststring, args) is None


def test_dependencies():
    "Test README examples for consistency"
    assert (
        try_date_expr(
            "Fri | September 1 | 2017",
            OUTPUTFORMAT,
            True,
            MIN_DATE,
            LATEST_POSSIBLE,
        )
        == "2017-09-01"
    )


def test_deferred():
    "Test deferred extraction"
    htmlstring = """<html><head>
    <link rel="canonical" href="https://example.org/2017/08/30/this.html"/>
    <meta property="og:published_time" content="2017-09-01"/>
    </head><body></body></html>"""
    assert find_date(htmlstring, deferred_url_extractor=True) == "2017-09-01"
    assert find_date(htmlstring, deferred_url_extractor=False) == "2017-08-30"


if __name__ == "__main__":
    # function-level
    test_input()
    test_sanity()
    test_is_valid_date()
    test_search_pattern()
    test_try_date_expr()
    test_convert_date()
    test_compare_reference()
    test_candidate_selection()
    test_regex_parse()
    test_external_date_parser()
    # test_header()

    # module-level
    test_deferred()
    test_no_date()
    test_exact_date()
    test_search_html()
    test_url()
    test_approximate_url()
    test_idiosyncrasies()
    # new_pages()

    # dependencies
    test_dependencies()

    # cli
    test_parser()
    test_cli()

    # loading functions
    test_download()
