# pylint:disable-msg=E0611,I1101
"""
Custom parsers and XPath expressions for date extraction
"""

import logging
import re

from datetime import datetime
from functools import lru_cache
from typing import List, Optional, Pattern, Tuple

# coverage for date parsing
from dateparser import DateDataParser  # type: ignore  # third-party, slow

from dateutil.parser import parse as dateutil_parse

from lxml.etree import XPath
from lxml.html import HtmlElement

# own
from .settings import CACHE_SIZE
from .utils import Extractor, trim_text
from .validators import convert_date, is_valid_date, validate_and_convert


LOGGER = logging.getLogger(__name__)

EXTERNAL_PARSER = DateDataParser(
    languages=None,
    locales=None,
    region=None,
    settings={
        "NORMALIZE": True,  # False may be faster
        "PARSERS": [
            "custom-formats",
            "absolute-time",
        ],
        "PREFER_DATES_FROM": "past",
        "PREFER_LOCALE_DATE_ORDER": True,
        "RETURN_AS_TIMEZONE_AWARE": False,
        "STRICT_PARSING": True,
    },
)


FAST_PREPEND = ".//*[self::div or self::h2 or self::h3 or self::h4 or self::li or self::p or self::span or self::time or self::ul]"
# self::b or self::em or self::font or self::i or self::strong
SLOW_PREPEND = ".//*"

DATE_EXPRESSIONS = """
[
    contains(translate(@id|@class|@itemprop, "D", "d"), 'date') or
    contains(translate(@id|@class|@itemprop, "D", "d"), 'datum') or
    contains(translate(@id|@class, "M", "m"), 'meta') or
    contains(@id|@class, 'time') or
    contains(@id|@class, 'publish') or
    contains(@id|@class, 'footer') or
    contains(@class, 'info') or
    contains(@class, 'post_detail') or
    contains(@class, 'block-content') or
    contains(@class, 'byline') or
    contains(@class, 'subline') or
    contains(@class, 'posted') or
    contains(@class, 'submitted') or
    contains(@class, 'created-post') or
    contains(@class, 'publication') or
    contains(@class, 'author') or
    contains(@class, 'autor') or
    contains(@class, 'field-content') or
    contains(@class, 'fa-clock-o') or
    contains(@class, 'fa-calendar') or
    contains(@class, 'fecha') or
    contains(@class, 'parution') or
    contains(@id, 'footer-info-lastmod')
] |
.//footer | .//small
"""

# further tests needed:
# or contains(@class, 'article')
# or contains(@id, 'lastmod') or contains(@class, 'updated')

FREE_TEXT_EXPRESSIONS = XPath(FAST_PREPEND + "/text()")
MIN_SEGMENT_LEN = 6
MAX_SEGMENT_LEN = 52

# discard parts of the webpage
# archive.org banner inserts
DISCARD_EXPRESSIONS = XPath('.//div[@id="wm-ipp-base" or @id="wm-ipp"]')
# not discarded for consistency (see above):
# .//footer
# .//*[(self::div or self::section)][@id="footer" or @class="footer"]

DAY_RE = "[0-3]?[0-9]"
MONTH_RE = "[0-1]?[0-9]"
YEAR_RE = "199[0-9]|20[0-3][0-9]"

# regex cache
YMD_NO_SEP_PATTERN = re.compile(r"\b(\d{8})\b")
YMD_PATTERN = re.compile(
    rf"(?:\D|^)(?:(?P<year>{YEAR_RE})[\-/.](?P<month>{MONTH_RE})[\-/.](?P<day>{DAY_RE})|"
    rf"(?P<day2>{DAY_RE})[\-/.](?P<month2>{MONTH_RE})[\-/.](?P<year2>\d{{2,4}}))(?:\D|$)"
)
YM_PATTERN = re.compile(
    rf"(?:\D|^)(?:(?P<year>{YEAR_RE})[\-/.](?P<month>{MONTH_RE})|"
    rf"(?P<month2>{MONTH_RE})[\-/.](?P<year2>{YEAR_RE}))(?:\D|$)"
)

REGEX_MONTHS = """
January?|February?|March|A[pv]ril|Ma[iy]|Jun[ei]|Jul[iy]|August|September|O[ck]tober|November|De[csz]ember|
Jan|Feb|M[aä]r|Apr|Jun|Jul|Aug|Sep|O[ck]t|Nov|De[cz]|
Januari|Februari|Maret|Mei|Agustus|
Jänner|Feber|März|
janvier|février|mars|juin|juillet|aout|septembre|octobre|novembre|décembre|
Ocak|Şubat|Mart|Nisan|Mayıs|Haziran|Temmuz|Ağustos|Eylül|Ekim|Kasım|Aralık|
Oca|Şub|Mar|Nis|Haz|Tem|Ağu|Eyl|Eki|Kas|Ara
"""  # todo: check "août"
LONG_TEXT_PATTERN = re.compile(
    rf"""(?P<month>{REGEX_MONTHS})\s
(?P<day>{DAY_RE})(?:st|nd|rd|th)?,? (?P<year>{YEAR_RE})|
(?P<day2>{DAY_RE})(?:st|nd|rd|th|\.)? (?:of )?
(?P<month2>{REGEX_MONTHS})[,.]? (?P<year2>{YEAR_RE})""".replace(
        "\n", ""
    ),
    re.I,
)

COMPLETE_URL = re.compile(rf"\D({YEAR_RE})[/_-]({MONTH_RE})[/_-]({DAY_RE})(?:\D|$)")

JSON_MODIFIED = re.compile(rf'"dateModified": ?"({YEAR_RE}-{MONTH_RE}-{DAY_RE})', re.I)
JSON_PUBLISHED = re.compile(
    rf'"datePublished": ?"({YEAR_RE}-{MONTH_RE}-{DAY_RE})', re.I
)
TIMESTAMP_PATTERN = re.compile(
    rf"({YEAR_RE}-{MONTH_RE}-{DAY_RE}).[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}"
)

# English, French, German, Indonesian and Turkish dates cache
MONTHS = [
    ("jan", "januar", "jänner", "january", "januari", "janvier", "ocak", "oca"),
    ("feb", "februar", "feber", "february", "februari", "février", "şubat", "şub"),
    ("mar", "mär", "märz", "march", "maret", "mart", "mars"),
    ("apr", "april", "avril", "nisan", "nis"),
    ("may", "mai", "mei", "mayıs"),
    ("jun", "juni", "june", "juin", "haziran", "haz"),
    ("jul", "juli", "july", "juillet", "temmuz", "tem"),
    ("aug", "august", "agustus", "ağustos", "ağu", "aout"),
    ("sep", "september", "septembre", "eylül", "eyl"),
    ("oct", "oktober", "october", "octobre", "okt", "ekim", "eki"),
    ("nov", "november", "kasım", "kas", "novembre"),
    ("dec", "dez", "dezember", "december", "desember", "décembre", "aralık", "ara"),
]

TEXT_MONTHS = {
    month: mnum for mnum, mlist in enumerate(MONTHS, start=1) for month in mlist
}

TEXT_DATE_PATTERN = re.compile(r"[.:,_/ -]|^\d+$")

DISCARD_PATTERNS = re.compile(
    r"^\d{2}:\d{2}(?: |:|$)|"
    r"^\D*\d{4}\D*$|"
    r"[$€¥Ұ£¢₽₱฿#₹]|"  # currency symbols and special characters
    r"[A-Z]{3}[^A-Z]|"  # currency codes
    r"(?:^|\D)(?:\+\d{2}|\d{3}|\d{5})\D|"  # tel./IPs/postal codes
    r"ftps?|https?|sftp|"  # protocols
    r"\.(?:com|net|org|info|gov|edu|de|fr|io)\b|"  # TLDs
    r"IBAN|[A-Z]{2}[0-9]{2}|"  # bank accounts
    r"®"  # ©
)

# use of regex module for speed?
TEXT_PATTERNS = re.compile(
    r'(?:date[^0-9"]{,20}|updated|last-modified|published|posted|on)(?:[ :])*?([0-9]{1,4})[./]([0-9]{1,2})[./]([0-9]{2,4})|'  # EN
    r"(?:Datum|Stand|Veröffentlicht am):? ?([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{2,4})|"  # DE
    r"(?:güncellen?me|yayı(?:m|n)lan?ma) *?(?:tarihi)? *?:? *?([0-9]{1,2})[./]([0-9]{1,2})[./]([0-9]{2,4})|"
    r"([0-9]{1,2})[./]([0-9]{1,2})[./]([0-9]{2,4}) *?(?:'de|'da|'te|'ta|’de|’da|’te|’ta|tarihinde) *(?:güncellendi|yayı(?:m|n)landı)",  # TR
    re.I,
)

# core patterns
THREE_COMP_REGEX_A = re.compile(rf"({DAY_RE})[/.-]({MONTH_RE})[/.-]({YEAR_RE})")
THREE_COMP_REGEX_B = re.compile(
    rf"({DAY_RE})/({MONTH_RE})/([0-9]{{2}})|({DAY_RE})[.-]({MONTH_RE})[.-]([0-9]{{2}})"
)
TWO_COMP_REGEX = re.compile(rf"({MONTH_RE})[/.-]({YEAR_RE})")

# extensive search patterns
YEAR_PATTERN = re.compile(rf"^\D?({YEAR_RE})")
COPYRIGHT_PATTERN = re.compile(
    rf"(?:©|\&copy;|Copyright|\(c\))\D*(?:{YEAR_RE})?-?({YEAR_RE})\D"
)
THREE_PATTERN = re.compile(r"/([0-9]{4}/[0-9]{2}/[0-9]{2})[01/]")
THREE_CATCH = re.compile(r"([0-9]{4})/([0-9]{2})/([0-9]{2})")
THREE_LOOSE_PATTERN = re.compile(r"\D([0-9]{4}[/.-][0-9]{2}[/.-][0-9]{2})\D")
THREE_LOOSE_CATCH = re.compile(r"([0-9]{4})[/.-]([0-9]{2})[/.-]([0-9]{2})")
SELECT_YMD_PATTERN = re.compile(r"\D([0-3]?[0-9][/.-][01]?[0-9][/.-][0-9]{4})\D")
SELECT_YMD_YEAR = re.compile(rf"({YEAR_RE})\D?$")
YMD_YEAR = re.compile(rf"^({YEAR_RE})")
DATESTRINGS_PATTERN = re.compile(
    r"(\D19[0-9]{2}[01][0-9][0-3][0-9]\D|\D20[0-9]{2}[01][0-9][0-3][0-9]\D)"
)
DATESTRINGS_CATCH = re.compile(rf"({YEAR_RE})([01][0-9])([0-3][0-9])")
SLASHES_PATTERN = re.compile(
    r"\D([0-3]?[0-9]/[01]?[0-9]/[0129][0-9]|[0-3][0-9]\.[01][0-9]\.[0129][0-9])\D"
)
SLASHES_YEAR = re.compile(r"([0-9]{2})$")
YYYYMM_PATTERN = re.compile(r"\D([12][0-9]{3}[/.-](?:1[0-2]|0[1-9]))\D")
YYYYMM_CATCH = re.compile(rf"({YEAR_RE})[/.-](1[0-2]|0[1-9]|)")
MMYYYY_PATTERN = re.compile(r"\D([01]?[0-9][/.-][12][0-9]{3})\D")
MMYYYY_YEAR = re.compile(rf"({YEAR_RE})\D?$")
SIMPLE_PATTERN = re.compile(rf"(?<!w3.org)\D({YEAR_RE})\D")


def discard_unwanted(tree: HtmlElement) -> Tuple[HtmlElement, List[HtmlElement]]:
    """Delete unwanted sections of an HTML document and return them as a list"""
    my_discarded = []
    for subtree in DISCARD_EXPRESSIONS(tree):
        my_discarded.append(subtree)
        subtree.getparent().remove(subtree)
    return tree, my_discarded


def extract_url_date(
    testurl: Optional[str],
    options: Extractor,
) -> Optional[str]:
    """Extract the date out of an URL string complying with the Y-M-D format"""
    if testurl is not None:
        match = COMPLETE_URL.search(testurl)
        if match:
            LOGGER.debug("found date in URL: %s", match[0])
            try:
                dateobject = datetime(int(match[1]), int(match[2]), int(match[3]))
                if is_valid_date(
                    dateobject, options.format, earliest=options.min, latest=options.max
                ):
                    return dateobject.strftime(options.format)
            except ValueError as err:  # pragma: no cover
                LOGGER.debug("conversion error: %s %s", match[0], err)
    return None


def correct_year(year: int) -> int:
    """Adapt year from YY to YYYY format"""
    if year < 100:
        year += 1900 if year >= 90 else 2000
    return year


def try_swap_values(day: int, month: int) -> Tuple[int, int]:
    """Swap day and month values if it seems feasible."""
    return (month, day) if month > 12 and day <= 12 else (day, month)


def regex_parse(string: str) -> Optional[datetime]:
    """Try full-text parse for date elements using a series of regular expressions
    with particular emphasis on English, French, German and Turkish"""
    # https://github.com/vi3k6i5/flashtext ?
    # multilingual day-month-year + American English patterns
    match = LONG_TEXT_PATTERN.search(string)
    if not match:
        return None
    groups = (
        ("day", "month", "year")
        if match.lastgroup == "year"
        else ("day2", "month2", "year2")
    )
    # process and return
    try:
        day, month, year = (
            int(match.group(groups[0])),
            int(TEXT_MONTHS[match.group(groups[1]).lower().strip(".")]),
            int(match.group(groups[2])),
        )
        year = correct_year(year)
        day, month = try_swap_values(day, month)
        dateobject = datetime(year, month, day)
    except ValueError:
        return None
    LOGGER.debug("multilingual text found: %s", dateobject)
    return dateobject


def custom_parse(
    string: str, outputformat: str, min_date: datetime, max_date: datetime
) -> Optional[str]:
    """Try to bypass the slow dateparser"""
    LOGGER.debug("custom parse test: %s", string)

    # 1. shortcut
    if string[:4].isdigit():
        candidate = None
        # a. '201709011234' not covered by dateparser, and regex too slow
        if string[4:8].isdigit():
            try:
                candidate = datetime(
                    int(string[:4]), int(string[4:6]), int(string[6:8])
                )
            except ValueError:
                LOGGER.debug("8-digit error: %s", string[:8])  # return None
        # b. much faster than extensive parsing
        else:
            try:
                candidate = datetime.fromisoformat(string)  # type: ignore[attr-defined]
            except ValueError:
                LOGGER.debug("not an ISO date string: %s", string)
                try:
                    candidate = dateutil_parse(string, fuzzy=False)  # ignoretz=True
                except (OverflowError, TypeError, ValueError):
                    LOGGER.debug("dateutil parsing error: %s", string)
        # c. plausibility test
        if candidate is not None and (
            is_valid_date(candidate, outputformat, earliest=min_date, latest=max_date)
        ):
            LOGGER.debug("parsing result: %s", candidate)
            return candidate.strftime(outputformat)

    # 2. Try YYYYMMDD, use regex
    match = YMD_NO_SEP_PATTERN.search(string)
    if match:
        try:
            year, month, day = int(match[1][:4]), int(match[1][4:6]), int(match[1][6:8])
            candidate = datetime(year, month, day)
        except ValueError:
            LOGGER.debug("YYYYMMDD value error: %s", match[0])
        else:
            if is_valid_date(candidate, "%Y-%m-%d", earliest=min_date, latest=max_date):
                LOGGER.debug("YYYYMMDD match: %s", candidate)
                return candidate.strftime(outputformat)

    # 3. Try the very common YMD, Y-M-D, and D-M-Y patterns
    match = YMD_PATTERN.search(string)
    if match:
        try:
            if match.lastgroup == "day":
                year, month, day = (
                    int(match.group("year")),
                    int(match.group("month")),
                    int(match.group("day")),
                )
            else:
                day, month, year = (
                    int(match.group("day2")),
                    int(match.group("month2")),
                    int(match.group("year2")),
                )
                year = correct_year(year)
                day, month = try_swap_values(day, month)

            candidate = datetime(year, month, day)
        except ValueError:  # pragma: no cover
            LOGGER.debug("regex value error: %s", match[0])
        else:
            if is_valid_date(candidate, "%Y-%m-%d", earliest=min_date, latest=max_date):
                LOGGER.debug("regex match: %s", candidate)
                return candidate.strftime(outputformat)

    # 4. Try the Y-M and M-Y patterns
    match = YM_PATTERN.search(string)
    if match:
        try:
            if match.lastgroup == "month":
                candidate = datetime(
                    int(match.group("year")), int(match.group("month")), 1
                )
            else:
                candidate = datetime(
                    int(match.group("year2")), int(match.group("month2")), 1
                )
        except ValueError:
            LOGGER.debug("Y-M value error: %s", match[0])
        else:
            if is_valid_date(candidate, "%Y-%m-%d", earliest=min_date, latest=max_date):
                LOGGER.debug("Y-M match: %s", candidate)
                return candidate.strftime(outputformat)

    # 5. Try the other regex pattern
    dateobject = regex_parse(string)
    return validate_and_convert(
        dateobject, outputformat, earliest=min_date, latest=max_date
    )


def external_date_parser(string: str, outputformat: str) -> Optional[str]:
    """Use dateutil parser or dateparser module according to system settings"""
    LOGGER.debug("send to external parser: %s", string)
    try:
        target = EXTERNAL_PARSER.get_date_data(string)["date_obj"]
    # 2 types of errors possible
    except (OverflowError, ValueError) as err:  # pragma: no cover
        target = None
        LOGGER.error("external parser error: %s %s", string, err)
    # issue with data type
    return datetime.strftime(target, outputformat) if target else None


@lru_cache(maxsize=CACHE_SIZE)
def try_date_expr(
    string: Optional[str],
    outputformat: str,
    extensive_search: bool,
    min_date: datetime,
    max_date: datetime,
) -> Optional[str]:
    """Use a series of heuristics and rules to parse a potential date expression"""
    if not string:
        return None

    # trim
    string = trim_text(string)[:MAX_SEGMENT_LEN]

    # formal constraint: 4 to 18 digits
    if not string or not 4 <= sum(map(str.isdigit, string)) <= 18:
        return None

    # check if string only contains time/single year or digits and not a date
    if DISCARD_PATTERNS.search(string):
        return None

    # try to parse using the faster method
    customresult = custom_parse(string, outputformat, min_date, max_date)
    if customresult is not None:
        return customresult

    # use slow but extensive search
    # additional filters to prevent computational cost
    if extensive_search and TEXT_DATE_PATTERN.search(string):
        # send to date parser
        dateparser_result = external_date_parser(string, outputformat)
        if is_valid_date(
            dateparser_result, outputformat, earliest=min_date, latest=max_date
        ):
            return dateparser_result

    return None


def img_search(
    tree: HtmlElement,
    options: Extractor,
) -> Optional[str]:
    """Skim through image elements"""
    element = tree.find('.//meta[@property="og:image"][@content]')
    if element is not None:
        return extract_url_date(
            element.get("content"),
            options,
        )
    return None


def pattern_search(
    text: str,
    date_pattern: Pattern[str],
    options: Extractor,
) -> Optional[str]:
    "Look for date expressions using a regular expression on a string of text."
    match = date_pattern.search(text)
    if match and is_valid_date(
        match[1], "%Y-%m-%d", earliest=options.min, latest=options.max
    ):
        LOGGER.debug("regex found: %s %s", date_pattern, match[0])
        return convert_date(match[1], "%Y-%m-%d", options.format)
    return None


def json_search(
    tree: HtmlElement,
    options: Extractor,
) -> Optional[str]:
    """Look for JSON time patterns in JSON sections of the tree"""
    # determine pattern
    json_pattern = JSON_PUBLISHED if options.original else JSON_MODIFIED
    # look throughout the HTML tree
    for elem in tree.xpath(
        './/script[@type="application/ld+json" or @type="application/settings+json"]'
    ):
        if not elem.text or '"date' not in elem.text:
            continue
        return pattern_search(elem.text, json_pattern, options)
    return None


def idiosyncrasies_search(
    htmlstring: str,
    options: Extractor,
) -> Optional[str]:
    """Look for author-written dates throughout the web page"""
    match = TEXT_PATTERNS.search(htmlstring)  # EN+DE+TR
    if match:
        parts = list(filter(None, match.groups()))

        try:
            if len(parts[0]) == 4:  # year in first position
                candidate = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
            else:  # len(parts[2]) in (2, 4):  # DD/MM/YY
                day, month = try_swap_values(int(parts[0]), int(parts[1]))
                year = correct_year(int(parts[2]))
                candidate = datetime(year, month, day)
            if is_valid_date(
                candidate, "%Y-%m-%d", earliest=options.min, latest=options.max
            ):
                return candidate.strftime(options.format)  # type: ignore[union-attr]
        except (IndexError, ValueError):
            LOGGER.debug("cannot process idiosyncrasies: %s", match[0])

    return None
