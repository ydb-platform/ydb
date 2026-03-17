# pylint:disable-msg=E0611,I1101
"""Module bundling all functions needed to determine the date of HTML strings
or LXML trees.
"""

import logging
import re

from collections import Counter
from copy import deepcopy
from datetime import datetime
from functools import lru_cache, partial
from typing import Match, Optional, Pattern, Union, Counter as Counter_Type

from lxml.html import HtmlElement, tostring

# own
from .extractors import (
    discard_unwanted,
    extract_url_date,
    idiosyncrasies_search,
    img_search,
    json_search,
    regex_parse,
    pattern_search,
    try_date_expr,
    DATE_EXPRESSIONS,
    FAST_PREPEND,
    SLOW_PREPEND,
    FREE_TEXT_EXPRESSIONS,
    MAX_SEGMENT_LEN,
    MIN_SEGMENT_LEN,
    YEAR_PATTERN,
    YMD_PATTERN,
    COPYRIGHT_PATTERN,
    TIMESTAMP_PATTERN,
    THREE_PATTERN,
    THREE_CATCH,
    THREE_LOOSE_PATTERN,
    THREE_LOOSE_CATCH,
    SELECT_YMD_PATTERN,
    SELECT_YMD_YEAR,
    YMD_YEAR,
    DATESTRINGS_PATTERN,
    DATESTRINGS_CATCH,
    SLASHES_PATTERN,
    SLASHES_YEAR,
    YYYYMM_PATTERN,
    YYYYMM_CATCH,
    MMYYYY_PATTERN,
    MMYYYY_YEAR,
    SIMPLE_PATTERN,
    THREE_COMP_REGEX_A,
    THREE_COMP_REGEX_B,
    TWO_COMP_REGEX,
)
from .settings import CACHE_SIZE, CLEANING_LIST, MAX_POSSIBLE_CANDIDATES
from .utils import Extractor, clean_html, load_html, trim_text
from .validators import (
    check_extracted_reference,
    compare_values,
    filter_ymd_candidate,
    get_min_date,
    get_max_date,
    is_valid_date,
    is_valid_format,
    plausible_year_filter,
    validate_and_convert,
)


LOGGER = logging.getLogger(__name__)


def logstring(element: HtmlElement) -> str:
    """Format the element to be logged to a string."""
    return tostring(element, pretty_print=False, encoding="unicode").strip()


DATE_ATTRIBUTES = {
    "analyticsattributes.articledate",
    "article.created",
    "article_date_original",
    "article:post_date",
    "article.published",
    "article:published",
    "article:published_date",
    "article:published_time",
    "article:publicationdate",
    "bt:pubdate",
    "citation_date",
    "citation_publication_date",
    "content_create_date",
    "created",
    "cxenseparse:recs:publishtime",
    "date",
    "date_created",
    "date_published",
    "datecreated",
    "dateposted",
    "datepublished",
    # Dublin Core: https://wiki.whatwg.org/wiki/MetaExtensions
    "dc.date",
    "dc.created",
    "dc.date.created",
    "dc.date.issued",
    "dc.date.publication",
    "dcsext.articlefirstpublished",
    "dcterms.created",
    "dcterms.date",
    "dcterms.issued",
    "dc:created",
    "dc:date",
    "displaydate",
    "doc_date",
    "field-name-post-date",
    "gentime",
    "mediator_published_time",
    "meta",  # too loose?
    # Open Graph: https://opengraphprotocol.org/
    "og:article:published",
    "og:article:published_time",
    "og:datepublished",
    "og:pubdate",
    "og:publish_date",
    "og:published_time",
    "og:question:published_time",
    "og:regdate",
    "originalpublicationdate",
    "parsely-pub-date",
    "pdate",
    "ptime",
    "pubdate",
    "publishdate",
    "publish_date",
    "publish_time",
    "publish-date",
    "published-date",
    "published_date",
    "published_time",
    "publisheddate",
    "publication_date",
    "rbpubdate",
    "release_date",
    "rnews:datepublished",
    "sailthru.date",
    "shareaholic:article_published_time",
    "timestamp",
    "twt-published-at",
    "video:release_date",
    "vr:published_time",
}


NAME_MODIFIED = {
    "lastdate",
    "lastmod",
    "lastmodified",
    "last-modified",
    "modified",
    "utime",
}


PROPERTY_MODIFIED = {
    "article:modified",
    "article:modified_date",
    "article:modified_time",
    "article:post_modified",
    "bt:moddate",
    "datemodified",
    "dc.modified",
    "dcterms.modified",
    "lastmodified",
    "modified_time",
    "modificationdate",
    "og:article:modified_time",
    "og:modified_time",
    "og:updated_time",
    "release_date",
    "revision_date",
    "updated_time",
}


ITEMPROP_ATTRS_ORIGINAL = {"datecreated", "datepublished", "pubyear"}
ITEMPROP_ATTRS_MODIFIED = {"datemodified", "dateupdate"}
ITEMPROP_ATTRS = ITEMPROP_ATTRS_ORIGINAL.union(ITEMPROP_ATTRS_MODIFIED)
CLASS_ATTRS = {"date-published", "published", "time published"}

NON_DIGITS_REGEX = re.compile(r"\D+$")

THREE_COMP_PATTERNS = (
    (THREE_PATTERN, THREE_CATCH),
    (THREE_LOOSE_PATTERN, THREE_LOOSE_CATCH),
)


def examine_text(
    text: str,
    options: Extractor,
) -> Optional[str]:
    "Prepare text and try to extract a date."
    text = trim_text(text)

    if len(text) <= MIN_SEGMENT_LEN:
        return None

    text = NON_DIGITS_REGEX.sub("", text[:MAX_SEGMENT_LEN])
    return try_date_expr(
        text, options.format, options.extensive, options.min, options.max
    )


def examine_date_elements(
    tree: HtmlElement,
    expression: str,
    options: Extractor,
) -> Optional[str]:
    """Check HTML elements one by one for date expressions"""
    elements = tree.xpath(expression)
    if not elements or len(elements) > MAX_POSSIBLE_CANDIDATES:
        return None

    for elem in elements:
        # try element text and link title (Blogspot)
        for text in [elem.text_content(), elem.get("title", "")]:
            attempt = examine_text(text, options)
            if attempt:
                return attempt

    return None


def examine_header(
    tree: HtmlElement,
    options: Extractor,
) -> Optional[str]:
    """
    Parse header elements to find date cues

    :param tree:
        LXML parsed tree object
    :type tree: LXML tree
    :param options:
        Options for extraction
    :type options: Extractor
    :return: Returns a valid date expression as a string, or None

    """
    headerdate, reserve = None, None
    tryfunc = partial(
        try_date_expr,
        outputformat=options.format,
        extensive_search=options.extensive,
        min_date=options.min,
        max_date=options.max,
    )
    # loop through all meta elements
    for elem in tree.iterfind(".//meta"):
        # safeguard
        if (
            not elem.attrib
            or "content" not in elem.attrib
            and "datetime" not in elem.attrib
        ):
            continue
        # name attribute, most frequent
        if "name" in elem.attrib:
            attribute = elem.get("name", "").lower()
            # url
            if attribute == "og:url":
                reserve = extract_url_date(elem.get("content"), options)
            # date
            elif attribute in DATE_ATTRIBUTES:
                LOGGER.debug("examining meta name: %s", logstring(elem))
                headerdate = tryfunc(elem.get("content"))
            # modified
            elif attribute in NAME_MODIFIED:
                LOGGER.debug("examining meta name: %s", logstring(elem))
                if not options.original:
                    headerdate = tryfunc(elem.get("content"))
                else:
                    reserve = tryfunc(elem.get("content"))
        # property attribute
        elif "property" in elem.attrib:
            attribute = elem.get("property", "").lower()
            if attribute in DATE_ATTRIBUTES or attribute in PROPERTY_MODIFIED:
                LOGGER.debug("examining meta property: %s", logstring(elem))
                attempt = tryfunc(elem.get("content"))
                if attempt is not None:
                    if (attribute in DATE_ATTRIBUTES and options.original) or (
                        attribute in PROPERTY_MODIFIED and not options.original
                    ):
                        headerdate = attempt
                    # hurts precision
                    else:
                        reserve = attempt
        # itemprop
        elif "itemprop" in elem.attrib:
            attribute = elem.get("itemprop", "").lower()
            # original: store / updated: override date
            if attribute in ITEMPROP_ATTRS:
                LOGGER.debug("examining meta itemprop: %s", logstring(elem))
                attempt = tryfunc(elem.get("datetime") or elem.get("content"))
                # store value
                if attempt is not None:
                    if (attribute in ITEMPROP_ATTRS_ORIGINAL and options.original) or (
                        attribute in ITEMPROP_ATTRS_MODIFIED and not options.original
                    ):
                        headerdate = attempt
                    # put on hold: hurts precision
                    # else:
                    #    reserve = attempt
            # reserve with copyrightyear
            elif attribute == "copyrightyear":
                LOGGER.debug("examining meta itemprop: %s", logstring(elem))
                if "content" in elem.attrib:
                    attempt = "-".join([elem.get("content", ""), "01", "01"])
                    if is_valid_date(
                        attempt, "%Y-%m-%d", earliest=options.min, latest=options.max
                    ):
                        reserve = attempt
        # pubdate, relatively rare
        elif "pubdate" in elem.attrib:
            if elem.get("pubdate", "").lower() == "pubdate":
                LOGGER.debug("examining meta pubdate: %s", logstring(elem))
                headerdate = tryfunc(elem.get("content"))
        # http-equiv, rare
        elif "http-equiv" in elem.attrib:
            attribute = elem.get("http-equiv", "").lower()
            if attribute == "date":
                LOGGER.debug("examining meta http-equiv: %s", logstring(elem))
                if options.original:
                    headerdate = tryfunc(elem.get("content"))
                else:
                    reserve = tryfunc(elem.get("content"))
            elif attribute == "last-modified":
                LOGGER.debug("examining meta http-equiv: %s", logstring(elem))
                if not options.original:
                    headerdate = tryfunc(elem.get("content"))
                else:
                    reserve = tryfunc(elem.get("content"))
        # exit loop
        if headerdate is not None:
            break
    # if nothing was found, look for lower granularity (so far: "copyright year")
    if headerdate is None and reserve is not None:
        LOGGER.debug("opting for reserve date with less granularity")
        headerdate = reserve
    # return value
    return headerdate


def select_candidate(
    occurrences: Counter_Type[str],
    catch: Pattern[str],
    yearpat: Pattern[str],
    options: Extractor,
) -> Optional[Match[str]]:
    """Select a candidate among the most frequent matches"""
    if not occurrences or len(occurrences) > MAX_POSSIBLE_CANDIDATES:
        return None

    if len(occurrences) == 1:
        return catch.search(next(iter(occurrences)))

    # select among most frequent: more than 10? more than 2 candidates?
    firstselect = occurrences.most_common(10)
    LOGGER.debug("firstselect: %s", firstselect)
    # sort and find probable candidates
    bestones = sorted(firstselect, reverse=not options.original)[:2]
    LOGGER.debug("bestones: %s", bestones)

    # plausibility heuristics
    patterns, counts = zip(*bestones)

    years = []
    for pattern in patterns:
        year_match = yearpat.search(pattern)
        if year_match:
            years.append(year_match[1])

    validation = [
        is_valid_date(
            datetime(int(year), 1, 1), "%Y", earliest=options.min, latest=options.max
        )
        for year in years
    ]

    # safety net: plausibility
    if all(validation):
        # same number of occurrences: always take top of the pile?
        if counts[0] == counts[1]:
            match = catch.search(patterns[0])
        # safety net: newer date but up to 50% less frequent
        elif years[1] != years[0] and counts[1] / counts[0] > 0.5:
            match = catch.search(patterns[1])
        # not newer or hopefully not significant
        else:
            match = catch.search(patterns[0])
    elif any(validation):
        match = catch.search(patterns[validation.index(True)])
    else:
        LOGGER.debug("no suitable candidate: %s %s", years[0], years[1])
        match = None
    return match


def search_pattern(
    htmlstring: str,
    pattern: Pattern[str],
    catch: Pattern[str],
    yearpat: Pattern[str],
    options: Extractor,
) -> Optional[Match[str]]:
    """Chained candidate filtering and selection"""
    candidates = plausible_year_filter(
        htmlstring,
        pattern=pattern,
        yearpat=yearpat,
        earliest=options.min,
        latest=options.max,
    )
    return select_candidate(candidates, catch, yearpat, options)


@lru_cache(maxsize=CACHE_SIZE)
def compare_reference(
    reference: int,
    expression: str,
    options: Extractor,
) -> int:
    """Compare candidate to current date reference (includes date validation and older/newer test)"""
    attempt = try_date_expr(
        expression, options.format, options.extensive, options.min, options.max
    )
    if attempt is not None:
        return compare_values(reference, attempt, options)
    return reference


def examine_abbr_elements(
    tree: HtmlElement,
    options: Extractor,
) -> Optional[str]:
    """Scan the page for abbr elements and check if their content contains an eligible date"""
    elements = tree.findall(".//abbr")
    if 0 < len(elements) < MAX_POSSIBLE_CANDIDATES:
        reference = 0
        for elem in elements:
            # data-utime (mostly Facebook)
            if "data-utime" in elem.attrib:
                try:
                    candidate = int(elem.get("data-utime", ""))
                except ValueError:
                    continue
                LOGGER.debug("data-utime found: %s", candidate)
                # look for original date
                if options.original and (reference == 0 or candidate < reference):
                    reference = candidate
                # look for newest (i.e. largest time delta)
                elif not options.original and candidate > reference:
                    reference = candidate
            # class
            elif elem.get("class") in CLASS_ATTRS:
                # other attributes
                if "title" in elem.attrib:
                    trytext = elem.get("title")
                    LOGGER.debug("abbr published-title found: %s", trytext)
                    # shortcut
                    if options.original:
                        attempt = try_date_expr(
                            trytext,
                            options.format,
                            options.extensive,
                            options.min,
                            options.max,
                        )
                        if attempt is not None:
                            return attempt
                    else:
                        reference = compare_reference(reference, trytext, options)
                        # faster execution
                        if reference > 0:
                            break
                # dates, not times of the day
                elif elem.text and len(elem.text) > 10:
                    LOGGER.debug("abbr published found: %s", elem.text)
                    reference = compare_reference(reference, elem.text, options)
        # return or try rescue in abbr content
        return check_extracted_reference(reference, options) or examine_date_elements(
            tree,
            ".//abbr",
            options,
        )
    return None


def examine_time_elements(
    tree: HtmlElement,
    options: Extractor,
) -> Optional[str]:
    """Scan the page for time elements and check if their content contains an eligible date"""
    elements = tree.findall(".//time")
    if 0 < len(elements) < MAX_POSSIBLE_CANDIDATES:
        # scan all the tags and look for the newest one
        reference = 0
        for elem in elements:
            shortcut_flag = False
            datetime_attr = elem.get("datetime", "")
            # go for datetime
            if len(datetime_attr) > 6:
                # shortcut: time pubdate
                if (
                    "pubdate" in elem.attrib
                    and elem.get("pubdate") == "pubdate"
                    and options.original
                ):
                    shortcut_flag = True
                    LOGGER.debug("shortcut for time pubdate found: %s", datetime_attr)
                # shortcuts: class attribute
                elif "class" in elem.attrib:
                    if options.original and (
                        elem.get("class", "").startswith("entry-date")
                        or elem.get("class", "").startswith("entry-time")
                    ):
                        shortcut_flag = True
                        LOGGER.debug(
                            "shortcut for time/datetime found: %s", datetime_attr
                        )
                    # updated time
                    elif not options.original and elem.get("class") == "updated":
                        shortcut_flag = True
                        LOGGER.debug(
                            "shortcut for updated time/datetime found: %s",
                            datetime_attr,
                        )
                # datetime attribute
                else:
                    LOGGER.debug("time/datetime found: %s", datetime_attr)
                # analyze attribute
                if shortcut_flag:
                    attempt = try_date_expr(
                        datetime_attr,
                        options.format,
                        options.extensive,
                        options.min,
                        options.max,
                    )
                    if attempt is not None:
                        return attempt
                else:
                    reference = compare_reference(reference, datetime_attr, options)
            # bare text in element
            elif elem.text is not None and len(elem.text) > 6:
                LOGGER.debug("time/datetime found in text: %s", elem.text)
                reference = compare_reference(reference, elem.text, options)
            # else...?
        # return
        return check_extracted_reference(reference, options)
    return None


def normalize_match(match: Optional[Match[str]]) -> str:
    """Normalize string output by adding "0" if necessary,
    and optionally expand the year from two to four digits."""
    day, month, year = (g.zfill(2) for g in match.groups() if g)  # type: ignore[union-attr]
    if len(year) == 2:
        year = f"19{year}" if year[0] == "9" else f"20{year}"
    return f"{year}-{month}-{day}"


def search_page(htmlstring: str, options: Extractor) -> Optional[str]:
    """
    Opportunistically search the HTML text for common text patterns

    :param htmlstring:
        The HTML document in string format, potentially cleaned and stripped to
        the core (much faster)
    :type htmlstring: string
    :param options:
        Define extraction options
    :type options: Extractor
    :return: Returns a valid date expression as a string, or None

    """

    # copyright symbol
    LOGGER.debug("looking for copyright/footer information")
    copyear = 0
    bestmatch = search_pattern(
        htmlstring,
        COPYRIGHT_PATTERN,
        YEAR_PATTERN,
        YEAR_PATTERN,
        options,
    )
    if bestmatch is not None:
        year = int(bestmatch[0])
        if is_valid_date(
            datetime(year, 1, 1), "%Y", earliest=options.min, latest=options.max
        ):
            LOGGER.debug("copyright year/footer pattern found: %s", year)
            copyear = year

    # 3 components
    LOGGER.debug("3 components")
    # target URL characteristics
    # then more loosely structured data
    for patterns in THREE_COMP_PATTERNS:
        bestmatch = search_pattern(
            htmlstring,
            patterns[0],
            patterns[1],
            YEAR_PATTERN,
            options,
        )
        result = filter_ymd_candidate(
            bestmatch,
            patterns[0],
            options.original,
            copyear,
            options.format,
            options.min,
            options.max,
        )
        if result is not None:
            return result

    # YYYY-MM-DD/DD-MM-YYYY
    candidates = plausible_year_filter(
        htmlstring,
        pattern=SELECT_YMD_PATTERN,
        yearpat=SELECT_YMD_YEAR,
        earliest=options.min,
        latest=options.max,
    )
    # revert DD-MM-YYYY patterns before sorting
    replacement = {}
    for item in candidates:
        match = THREE_COMP_REGEX_A.match(item)
        candidate = normalize_match(match)
        replacement[candidate] = candidates[item]
    candidates = Counter(replacement)
    # select
    bestmatch = select_candidate(candidates, YMD_PATTERN, YMD_YEAR, options)
    result = filter_ymd_candidate(
        bestmatch,
        SELECT_YMD_PATTERN,
        options.original,
        copyear,
        options.format,
        options.min,
        options.max,
    )
    if result is not None:
        return result

    # valid dates strings
    bestmatch = search_pattern(
        htmlstring,
        DATESTRINGS_PATTERN,
        DATESTRINGS_CATCH,
        YEAR_PATTERN,
        options,
    )
    result = filter_ymd_candidate(
        bestmatch,
        DATESTRINGS_PATTERN,
        options.original,
        copyear,
        options.format,
        options.min,
        options.max,
    )
    if result is not None:
        return result

    # DD?/MM?/YY
    candidates = plausible_year_filter(
        htmlstring,
        pattern=SLASHES_PATTERN,
        yearpat=SLASHES_YEAR,
        earliest=options.min,
        latest=options.max,
        incomplete=True,
    )
    # revert DD-MM-YYYY patterns before sorting
    replacement = {}
    for item in candidates:
        match = THREE_COMP_REGEX_B.match(item)
        candidate = normalize_match(match)
        replacement[candidate] = candidates[item]
    candidates = Counter(replacement)
    bestmatch = select_candidate(candidates, YMD_PATTERN, YMD_YEAR, options)
    result = filter_ymd_candidate(
        bestmatch,
        SLASHES_PATTERN,
        options.original,
        copyear,
        options.format,
        options.min,
        options.max,
    )
    if result is not None:
        return result

    # 2 components
    LOGGER.debug("switching to two components")
    # first option
    bestmatch = search_pattern(
        htmlstring,
        YYYYMM_PATTERN,
        YYYYMM_CATCH,
        YEAR_PATTERN,
        options,
    )
    if bestmatch is not None:
        dateobject = datetime(int(bestmatch[1]), int(bestmatch[2]), 1)
        if copyear == 0 or dateobject.year >= copyear:
            result = validate_and_convert(
                dateobject, options.format, earliest=options.min, latest=options.max
            )
            if result is not None:
                LOGGER.debug(
                    'date found for pattern "%s": %s, %s',
                    YYYYMM_PATTERN,
                    bestmatch[1],
                    bestmatch[2],
                )
                return result

    # 2 components, second option
    candidates = plausible_year_filter(
        htmlstring,
        pattern=MMYYYY_PATTERN,
        yearpat=MMYYYY_YEAR,
        earliest=options.min,
        latest=options.max,
        incomplete=options.original,
    )
    # revert DD-MM-YYYY patterns before sorting
    replacement = {}
    for item in candidates:
        match = TWO_COMP_REGEX.match(item)
        month = match[1]  # type: ignore[index]
        if len(month) == 1:
            month = f"0{month}"
        candidate = "-".join([match[2], month, "01"])  # type: ignore[index]
        replacement[candidate] = candidates[item]
    candidates = Counter(replacement)
    # select
    bestmatch = select_candidate(candidates, YMD_PATTERN, YMD_YEAR, options)
    result = filter_ymd_candidate(
        bestmatch,
        MMYYYY_PATTERN,
        options.original,
        copyear,
        options.format,
        options.min,
        options.max,
    )
    if result is not None:
        return result

    # try full-blown text regex on all HTML?
    dateobject = regex_parse(htmlstring)  # type: ignore[assignment]
    # todo: find all candidates and disambiguate?
    if copyear == 0 or (dateobject and dateobject.year >= copyear):
        result = validate_and_convert(
            dateobject, options.format, earliest=options.min, latest=options.max
        )
        if result is not None:
            return result

    # catchall: copyright mention
    if copyear != 0:
        LOGGER.debug("using copyright year as default")
        dateobject = datetime(int(copyear), 1, 1)
        return dateobject.strftime(options.format)

    # last resort: 1 component
    LOGGER.debug("switching to one component")
    bestmatch = search_pattern(
        htmlstring,
        SIMPLE_PATTERN,
        YEAR_PATTERN,
        YEAR_PATTERN,
        options,
    )
    if bestmatch is not None:
        dateobject = datetime(int(bestmatch[0]), 1, 1)
        if (
            is_valid_date(
                dateobject, "%Y-%m-%d", earliest=options.min, latest=options.max
            )
            and dateobject.year >= copyear
        ):
            LOGGER.debug(
                'date found for pattern "%s": %s', SIMPLE_PATTERN, bestmatch[0]
            )
            return dateobject.strftime(options.format)

    return None


def find_date(
    htmlobject: Union[bytes, str, HtmlElement],
    extensive_search: bool = True,
    original_date: bool = False,
    outputformat: str = "%Y-%m-%d",
    url: Optional[str] = None,
    verbose: bool = False,
    min_date: Optional[Union[datetime, str]] = None,
    max_date: Optional[Union[datetime, str]] = None,
    deferred_url_extractor: bool = False,
) -> Optional[str]:
    """
    Extract dates from HTML documents using markup analysis and text patterns

    :param htmlobject:
        Two possibilities: 1. HTML document (e.g. body of HTTP request or .html-file) in text string
        form or LXML parsed tree or 2. URL string (gets detected automatically)
    :type htmlobject: string or lxml tree
    :param extensive_search:
        Activate pattern-based opportunistic text search
    :type extensive_search: boolean
    :param original_date:
        Look for original date (e.g. publication date) instead of most recent
        one (e.g. last modified, updated time)
    :type original_date: boolean
    :param outputformat:
        Provide a valid datetime format for the returned string
        (see datetime.strftime())
    :type outputformat: string
    :param url:
        Provide an URL manually for pattern-searching in URL
        (in some cases much faster)
    :type url: string
    :param verbose:
        Set verbosity level for debugging
    :type verbose: boolean
    :param min_date:
        Set the earliest acceptable date manually (ISO 8601 YMD format)
    :type min_date: datetime, string
    :param max_date:
        Set the latest acceptable date manually (ISO 8601 YMD format)
    :type max_date: datetime, string
    :param deferred_url_extractor:
        Use url extractor as backup only to prioritize full expressions,
        e.g. of the type `%Y-%m-%d %H:%M:%S`
    :type deferred_url_extractor: boolean
    :return: Returns a valid date expression as a string, or None
    """

    # init
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    tree = load_html(htmlobject)

    # safeguards
    if tree is None:
        return None
    if outputformat != "%Y-%m-%d" and not is_valid_format(outputformat):
        return None

    # define options and time boundaries
    options = Extractor(
        extensive_search,
        get_max_date(max_date),
        get_min_date(min_date),
        original_date,
        outputformat,
    )
    # unclear what this line is for and it impedes type checking:
    # find_date.extensive_search = extensive_search

    # URL
    url_result = None
    if url is None:
        # probe for canonical links
        urlelem = tree.find('.//link[@rel="canonical"]')
        if urlelem is not None:
            url = urlelem.get("href")

    # direct processing of URL info
    url_result = extract_url_date(url, options)
    if url_result is not None and not deferred_url_extractor:
        return url_result

    # first try header
    # then try to use JSON data
    result = examine_header(tree, options) or json_search(tree, options)
    if result is not None:
        return result

    # deferred processing of URL info (may be moved even further down if necessary)
    if deferred_url_extractor and url_result is not None:
        return url_result

    # try abbr elements
    abbr_result = examine_abbr_elements(
        tree,
        options,
    )
    if abbr_result is not None:
        return abbr_result

    # first, prune tree
    try:
        search_tree, discarded = discard_unwanted(
            clean_html(deepcopy(tree), CLEANING_LIST)
        )
    # rare LXML error: no NULL bytes or control characters
    except ValueError:  # pragma: no cover
        search_tree = tree
        LOGGER.error("lxml cleaner error")

    # define expressions + text_content
    if extensive_search:
        date_expr = SLOW_PREPEND + DATE_EXPRESSIONS
    else:
        date_expr = FAST_PREPEND + DATE_EXPRESSIONS

    # then look for expressions
    # and try time elements
    result = (
        examine_date_elements(
            search_tree,
            date_expr,
            options,
        )
        or examine_date_elements(
            search_tree,
            ".//title|.//h1",
            options,
        )
        or examine_time_elements(search_tree, options)
    )
    if result is not None:
        return result

    # TODO: decide on this
    # search in discarded parts (e.g. archive.org-banner)
    # for subtree in discarded:
    #    dateresult = examine_date_elements(subtree, DATE_EXPRESSIONS, options)
    #    if dateresult is not None:
    #        return dateresult

    # robust conversion to string
    try:
        htmlstring = tostring(search_tree, pretty_print=False, encoding="unicode")
    except UnicodeDecodeError:
        htmlstring = tostring(search_tree, pretty_print=False).decode("utf-8", "ignore")

    # date regex timestamp rescue
    # try image elements
    # precise patterns and idiosyncrasies
    result = (
        pattern_search(htmlstring, TIMESTAMP_PATTERN, options)
        or img_search(search_tree, options)
        or idiosyncrasies_search(htmlstring, options)
    )
    if result is not None:
        return result

    # last resort
    if extensive_search:
        LOGGER.debug("extensive search started")
        # TODO: further tests & decide according to original_date
        reference = 0
        for segment in FREE_TEXT_EXPRESSIONS(search_tree):
            segment = segment.strip()
            if not MIN_SEGMENT_LEN < len(segment) < MAX_SEGMENT_LEN:
                continue
            reference = compare_reference(reference, segment, options)
        converted = check_extracted_reference(reference, options)
        # return or search page HTML
        return converted or search_page(htmlstring, options)

    return None
