"""
Core functions needed to make the module work.
"""

# import locale
import logging
import re

from typing import List, Optional, Set, Tuple
from urllib.robotparser import RobotFileParser

from .clean import normalize_url, scrub_url
from .filters import (
    basic_filter,
    domain_filter,
    extension_filter,
    is_navigation_page,
    is_not_crawlable,
    lang_filter,
    path_filter,
    type_filter,
    validate_url,
)
from .network import redirection_test
from .settings import BLACKLIST
from .urlutils import (
    extract_domain,
    get_base_url,
    fix_relative_urls,
    is_external,
    is_known_link,
)


LOGGER = logging.getLogger(__name__)

FIND_LINKS_REGEX = re.compile(r"<a [^<>]+?>", re.I)
HREFLANG_REGEX = re.compile(r'hreflang=["\']?([a-z-]+)', re.I)
LINK_REGEX = re.compile(r'href=["\']?([^ ]+?)(["\' >])', re.I)


def check_url(
    url: str,
    strict: bool = False,
    with_redirects: bool = False,
    language: Optional[str] = None,
    with_nav: bool = False,
    trailing_slash: bool = True,
) -> Optional[Tuple[str, str]]:
    """Check links for appropriateness and sanity
    Args:
        url: url to check
        strict: set to True for stricter filtering
        with_redirects: set to True for redirection test (per HTTP HEAD request)
        language: set target language (ISO 639-1 codes)
        with_nav: set to True to include navigation pages instead of discarding them
        trailing_slash: set to False to trim trailing slashes

    Returns:
        A tuple consisting of canonical URL and extracted domain

    Raises:
        ValueError, handled in exception.
    """

    # first sanity check
    # use standard parsing library, validate and strip fragments, then normalize
    try:
        # length test
        if basic_filter(url) is False:
            LOGGER.debug("rejected, basic filter: %s", url)
            raise ValueError

        # clean
        url = scrub_url(url)

        # get potential redirect, can raise ValueError
        if with_redirects:
            url = redirection_test(url)

        # spam & structural elements
        if type_filter(url, strict=strict, with_nav=with_nav) is False:
            LOGGER.debug("rejected, type filter: %s", url)
            raise ValueError

        # internationalization and language heuristics in URL
        if (
            language is not None
            and lang_filter(url, language, strict, trailing_slash) is False
        ):
            LOGGER.debug("rejected, lang filter: %s", url)
            raise ValueError

        # split and validate
        validation_test, parsed_url = validate_url(url)
        if validation_test is False:
            LOGGER.debug("rejected, validation test: %s", url)
            raise ValueError

        # content filter based on extensions
        if extension_filter(parsed_url.path) is False:
            LOGGER.debug("rejected, extension filter: %s", url)
            raise ValueError

        # unsuitable domain/host name
        if domain_filter(parsed_url.netloc) is False:
            LOGGER.debug("rejected, domain name: %s", url)
            raise ValueError

        # strict content filtering
        if strict and path_filter(parsed_url.path, parsed_url.query) is False:
            LOGGER.debug("rejected, path filter: %s", url)
            raise ValueError

        # normalize
        url = normalize_url(parsed_url, strict, language, trailing_slash)

        # domain info: use blacklist in strict mode only
        if strict:
            domain = extract_domain(url, blacklist=BLACKLIST, fast=True)
        else:
            domain = extract_domain(url, fast=True)
        if domain is None:
            LOGGER.debug("rejected, domain name: %s", url)
            return None

    # handle exceptions
    except (AttributeError, ValueError):
        LOGGER.debug("discarded URL: %s", url)
        return None

    return url, domain


def extract_links(
    pagecontent: str,
    url: Optional[str] = None,
    external_bool: bool = False,
    *,
    no_filter: bool = False,
    language: Optional[str] = None,
    strict: bool = True,
    trailing_slash: bool = True,
    with_nav: bool = False,
    redirects: bool = False,
    reference: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Set[str]:
    """Filter links in a HTML document using a series of heuristics
    Args:
        pagecontent: whole page in binary format
        url: full URL of the original page
        external_bool: set to True for external links only, False for
                  internal links only
        no_filter: override settings and bypass checks to return all possible URLs
        language: set target language (ISO 639-1 codes)
        strict: set to True for stricter filtering
        trailing_slash: set to False to trim trailing slashes
        with_nav: set to True to include navigation pages instead of discarding them
        redirects: set to True for redirection test (per HTTP HEAD request)
        reference: provide a host reference for external/internal evaluation

    Returns:
        A set containing filtered HTTP links checked for sanity and consistency.

    Raises:
        Nothing.
    """
    if base_url:
        raise ValueError("'base_url' is deprecated, use 'url' instead.")

    base_url = get_base_url(url)
    url = url or base_url
    candidates, validlinks = set(), set()  # type: Set[str], Set[str]
    if not pagecontent:
        return validlinks

    # define host reference
    reference = reference or base_url

    # extract links
    for link in (m[0] for m in FIND_LINKS_REGEX.finditer(pagecontent)):
        if "rel" in link and "nofollow" in link:
            continue
        # https://en.wikipedia.org/wiki/Hreflang
        if no_filter is False and language is not None and "hreflang" in link:
            langmatch = HREFLANG_REGEX.search(link)
            if langmatch and (
                langmatch[1].startswith(language) or langmatch[1] == "x-default"
            ):
                linkmatch = LINK_REGEX.search(link)
                if linkmatch:
                    candidates.add(linkmatch[1])
        # default
        else:
            linkmatch = LINK_REGEX.search(link)
            if linkmatch:
                candidates.add(linkmatch[1])

    # filter candidates
    for link in candidates:
        # repair using base
        if not link.startswith("http"):
            link = fix_relative_urls(url, link)
        # check
        if no_filter is False:
            checked = check_url(
                link,
                strict=strict,
                trailing_slash=trailing_slash,
                with_nav=with_nav,
                with_redirects=redirects,
                language=language,
            )
            if checked is None:
                continue
            link = checked[0]
            # external/internal links
            if external_bool != is_external(
                url=link, reference=reference, ignore_suffix=True
            ):
                continue
        if is_known_link(link, validlinks):
            continue
        validlinks.add(link)

    LOGGER.info("%s links found – %s valid links", len(candidates), len(validlinks))
    return validlinks


def filter_links(
    htmlstring: str,
    url: Optional[str],
    *,
    lang: Optional[str] = None,
    rules: Optional[RobotFileParser] = None,
    external: bool = False,
    strict: bool = False,
    with_nav: bool = True,
    base_url: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    "Find links in a HTML document, filter and prioritize them for crawling purposes."

    if base_url:
        raise ValueError("'base_url' is deprecated, use 'url' instead.")

    links, links_priority = [], []

    for link in extract_links(
        pagecontent=htmlstring,
        url=url,
        external_bool=external,
        language=lang,
        strict=strict,
        with_nav=with_nav,
    ):
        # sanity check
        if is_not_crawlable(link) or (
            rules is not None and not rules.can_fetch("*", link)
        ):
            continue
        # store
        if is_navigation_page(link):
            links_priority.append(link)
        else:
            links.append(link)

    return links, links_priority
