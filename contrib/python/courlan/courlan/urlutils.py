"""
Functions related to URL manipulation and extraction of URL parts.
"""

import re

from html import unescape
from typing import Any, List, Optional, Set, Tuple, Union
from urllib.parse import urljoin, urlsplit, urlunsplit, SplitResult

from tld import get_tld


DOMAIN_REGEX = re.compile(
    r"(?:(?:f|ht)tp)s?://"  # protocols
    r"(?:[^/?#]{,63}\.)?"  # subdomain, www, etc.
    r"([^/?#.]{4,63}\.[^/?#]{2,63}|"  # domain and extension
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|"  # IPv4
    r"[0-9a-f:]{16,})"  # IPv6
    r"(?:/|$)"  # slash or end of string
)
STRIP_PORT_REGEX = re.compile(r"(?<=\D):\d+")
CLEAN_FLD_REGEX = re.compile(r"^www[0-9]*\.")
FEED_WHITELIST_REGEX = re.compile(r"(?:feed(?:burner|proxy))", re.I)


def get_tldinfo(
    url: str, fast: bool = False
) -> Union[Tuple[None, None], Tuple[str, str]]:
    """Cached function to extract top-level domain info"""
    if not url or not isinstance(url, str):
        return None, None
    if fast:
        # try with regexes
        domain_match = DOMAIN_REGEX.match(url)
        if domain_match:
            full_domain = STRIP_PORT_REGEX.sub("", domain_match[1].split("@")[-1])
            clean_match = full_domain.split(".")[0]
            if clean_match:
                return clean_match, full_domain
    # fallback
    tldinfo = get_tld(url, as_object=True, fail_silently=True)
    if tldinfo is None:
        return None, None
    # this step is necessary to standardize output
    return tldinfo.domain, CLEAN_FLD_REGEX.sub("", tldinfo.fld)  # type: ignore[union-attr]


def extract_domain(
    url: str, blacklist: Optional[Set[str]] = None, fast: bool = False
) -> Optional[str]:
    """Extract domain name information using top-level domain info"""
    if blacklist is None:
        blacklist = set()
    # new code: Python >= 3.6 with tld module
    domain, full_domain = get_tldinfo(url, fast=fast)

    return (
        full_domain
        if full_domain and domain not in blacklist and full_domain not in blacklist
        else None
    )


def _parse(url: Any) -> SplitResult:
    "Parse a string or use urllib.parse object directly."
    if isinstance(url, str):
        parsed_url = urlsplit(unescape(url))
    elif isinstance(url, SplitResult):
        parsed_url = url
    else:
        raise TypeError("wrong input type:", type(url))
    return parsed_url


def get_base_url(url: Any) -> str:
    """Strip URL of some of its parts to get base URL.
    Accepts strings and urllib.parse ParseResult objects."""
    parsed_url = _parse(url)
    if parsed_url.scheme:
        scheme = parsed_url.scheme + "://"
    else:
        scheme = ""
    return scheme + parsed_url.netloc


def get_host_and_path(url: Any) -> Tuple[str, str]:
    """Decompose URL in two parts: protocol + host/domain and path.
    Accepts strings and urllib.parse ParseResult objects."""
    parsed_url = _parse(url)
    hostname = get_base_url(parsed_url)
    pathval = urlunsplit(
        ["", "", parsed_url.path, parsed_url.query, parsed_url.fragment]
    )
    # correction for root/homepage
    if pathval == "":
        pathval = "/"
    if not hostname or not pathval:
        raise ValueError(f"incomplete URL: {url}")
    return hostname, pathval


def get_hostinfo(url: str) -> Tuple[Optional[str], str]:
    "Convenience function returning domain and host info (protocol + host/domain) from a URL."
    domainname = extract_domain(url, fast=True)
    base_url = get_base_url(url)
    return domainname, base_url


def fix_relative_urls(baseurl: str, url: str) -> str:
    "Prepend protocol and host information to relative links."
    if url.startswith("{"):
        return url

    base_netloc = urlsplit(baseurl).netloc
    split_url = urlsplit(url)

    if split_url.netloc not in (base_netloc, ""):
        if split_url.scheme:
            return url
        return urlunsplit(split_url._replace(scheme="http"))

    return urljoin(baseurl, url)


def filter_urls(link_list: List[str], urlfilter: Optional[str]) -> List[str]:
    "Return a list of links corresponding to the given substring pattern."
    if urlfilter is None:
        return sorted(set(link_list))
    # filter links
    filtered_list = [l for l in link_list if urlfilter in l]
    # feedburner option: filter and wildcards for feeds
    if not filtered_list:
        filtered_list = [l for l in link_list if FEED_WHITELIST_REGEX.search(l)]
    return sorted(set(filtered_list))


def is_external(url: str, reference: str, ignore_suffix: bool = True) -> bool:
    """Determine if a link leads to another host, takes a reference URL and
    a URL as input, returns a boolean"""
    stripped_ref, ref = get_tldinfo(reference, fast=True)
    stripped_domain, domain = get_tldinfo(url, fast=True)
    # comparison
    if ignore_suffix:
        return stripped_domain != stripped_ref
    return domain != ref


def is_known_link(link: str, known_links: Set[str]) -> bool:
    "Compare the link and its possible variants to the existing URL base."
    # check exact link
    if link in known_links:
        return True

    # check link and variants with trailing slashes
    slash_test = link.rstrip("/") if link[-1] == "/" else link + "/"
    if slash_test in known_links:
        return True

    # check link and variants with modified protocol
    if link.startswith("http"):
        protocol_test = (
            "http" + link[:5] if link.startswith("https") else "https" + link[4:]
        )
        slash_test = (
            protocol_test.rstrip("/")
            if protocol_test[-1] == "/"
            else protocol_test + "/"
        )
        if protocol_test in known_links or slash_test in known_links:
            return True

    return False
