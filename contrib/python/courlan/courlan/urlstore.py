"""
Defines a URL store which holds URLs along with relevant information and entails crawling helpers.
"""

import gc
import logging
import pickle
import signal
import sys

try:
    import bz2

    HAS_BZ2 = True
except ImportError:
    HAS_BZ2 = False

try:
    import zlib

    HAS_ZLIB = True
except ImportError:
    HAS_ZLIB = False


from collections import defaultdict, deque
from datetime import datetime, timedelta
from enum import Enum
from operator import itemgetter
from threading import Lock
from typing import (
    Any,
    DefaultDict,
    Deque,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from urllib.robotparser import RobotFileParser

from .clean import normalize_url
from .core import filter_links
from .filters import lang_filter, validate_url
from .meta import clear_caches
from .urlutils import get_base_url, get_host_and_path, is_known_link


LOGGER = logging.getLogger(__name__)


class Compressor:
    "Use system information on available compression modules and define corresponding methods."
    __slots__ = ("compressor", "decompressor")

    def __init__(self, compression: bool = True) -> None:
        self.compressor: Any = (
            bz2.compress
            if compression and HAS_BZ2
            else zlib.compress if compression and HAS_ZLIB else self._identical
        )
        self.decompressor: Any = (
            bz2.decompress
            if compression and HAS_BZ2
            else zlib.decompress if compression and HAS_ZLIB else self._identical
        )

    @staticmethod
    def _identical(data: Any) -> Any:
        "Return unchanged data."
        return data

    def compress(self, data: Any) -> Any:
        "Pickle the data and compress it if a method is available."
        return self.compressor(pickle.dumps(data, protocol=5))

    def decompress(self, data: bytes) -> Any:
        "Decompress the data if a method is available and load the object."
        return pickle.loads(self.decompressor(data))


COMPRESSOR = Compressor()


class State(Enum):
    "Record state information about a domain or host."
    OPEN = 1
    ALL_VISITED = 2
    BUSTED = 3


class DomainEntry:
    "Class to record host-related information and URL paths."
    __slots__ = ("count", "rules", "state", "timestamp", "total", "tuples")

    def __init__(self, state: State = State.OPEN) -> None:
        self.count: int = 0
        self.rules: Optional[RobotFileParser] = None
        self.state: State = state
        self.timestamp: Optional[Any] = None
        self.total: int = 0
        self.tuples: Deque[UrlPathTuple] = deque()


class UrlPathTuple:
    "Class storing information for URL paths relative to a domain/host."
    __slots__ = ("urlpath", "visited")

    def __init__(self, urlpath: str, visited: bool) -> None:
        self.urlpath: bytes = urlpath.encode("utf-8")
        self.visited: bool = visited

    def path(self) -> str:
        "Get the URL path as string."
        return self.urlpath.decode("utf-8")


class UrlStore:
    "Defines a class to store domain-classified URLs and perform checks against it."
    __slots__ = (
        "compressed",
        "done",
        "language",
        "strict",
        "trailing_slash",
        "urldict",
        "_lock",
    )

    def __init__(
        self,
        compressed: bool = False,
        language: Optional[str] = None,
        strict: bool = False,
        trailing: bool = True,
        verbose: bool = False,
    ) -> None:
        self.compressed: bool = compressed
        self.done: bool = False
        self.language: Optional[str] = language
        self.strict: bool = strict
        self.trailing_slash: bool = trailing
        self.urldict: DefaultDict[str, DomainEntry] = defaultdict(DomainEntry)
        self._lock: Lock = Lock()

        def dump_unvisited_urls(num: Any, frame: Any) -> None:
            LOGGER.debug(
                "Processing interrupted, dumping unvisited URLs from %s hosts",
                len(self.urldict),
            )
            self.print_unvisited_urls()
            sys.exit(1)

        # don't use the following on Windows
        if verbose and not sys.platform.startswith("win"):
            signal.signal(signal.SIGINT, dump_unvisited_urls)
            signal.signal(signal.SIGTERM, dump_unvisited_urls)

    def _buffer_urls(
        self, data: List[str], visited: bool = False
    ) -> DefaultDict[str, Deque[UrlPathTuple]]:
        inputdict: DefaultDict[str, Deque[UrlPathTuple]] = defaultdict(deque)
        for url in dict.fromkeys(data):
            # segment URL and add to domain dictionary
            try:
                # validate
                validation_result, parsed_url = validate_url(url)
                if validation_result is False:
                    LOGGER.debug("Invalid URL: %s", url)
                    raise ValueError
                # filter
                if (
                    self.language is not None
                    and lang_filter(
                        url, self.language, self.strict, self.trailing_slash
                    )
                    is False
                ):
                    LOGGER.debug("Wrong language: %s", url)
                    raise ValueError
                parsed_url = normalize_url(
                    parsed_url,
                    strict=self.strict,
                    language=self.language,
                    trailing_slash=self.trailing_slash,
                )
                hostinfo, urlpath = get_host_and_path(parsed_url)
                inputdict[hostinfo].append(UrlPathTuple(urlpath, visited))
            except (TypeError, ValueError):
                LOGGER.warning("Discarding URL: %s", url)
        return inputdict

    def _load_urls(self, domain: str) -> Deque[UrlPathTuple]:
        if domain in self.urldict:
            if self.compressed:
                return COMPRESSOR.decompress(self.urldict[domain].tuples)  # type: ignore
            return self.urldict[domain].tuples
        return deque()

    def _set_done(self) -> None:
        if not self.done and all(v.state != State.OPEN for v in self.urldict.values()):
            with self._lock:
                self.done = True

    def _store_urls(
        self,
        domain: str,
        to_right: Optional[Deque[UrlPathTuple]] = None,
        timestamp: Optional[datetime] = None,
        to_left: Optional[Deque[UrlPathTuple]] = None,
    ) -> None:
        # http/https switch
        if domain.startswith("http://"):
            candidate = "https" + domain[4:]
            # switch
            if candidate in self.urldict:
                domain = candidate
        elif domain.startswith("https://"):
            candidate = "http" + domain[5:]
            # replace entry
            if candidate in self.urldict:
                self.urldict[domain] = self.urldict[candidate]
                del self.urldict[candidate]

        # load URLs or create entry
        if domain in self.urldict:
            # discard if busted
            if self.urldict[domain].state is State.BUSTED:
                return
            urls = self._load_urls(domain)
            known = {u.path() for u in urls}
        else:
            urls = deque()
            known = set()

        # check if the link or its variants are known
        if to_right is not None:
            urls.extend(t for t in to_right if not is_known_link(t.path(), known))
        if to_left is not None:
            urls.extendleft(t for t in to_left if not is_known_link(t.path(), known))

        with self._lock:
            if self.compressed:
                self.urldict[domain].tuples = COMPRESSOR.compress(urls)
            else:
                self.urldict[domain].tuples = urls
            self.urldict[domain].total = len(urls)

            if timestamp is not None:
                self.urldict[domain].timestamp = timestamp

            if all(u.visited for u in urls):
                self.urldict[domain].state = State.ALL_VISITED
            else:
                self.urldict[domain].state = State.OPEN
                if self.done:
                    self.done = False

    def _search_urls(
        self, urls: List[str], switch: Optional[int] = None
    ) -> List[Union[Any, str]]:
        # init
        last_domain: Optional[str] = None
        known_paths: Dict[str, Optional[bool]] = {}
        remaining_urls = dict.fromkeys(urls)
        # iterate
        for url in sorted(remaining_urls):
            hostinfo, urlpath = get_host_and_path(url)
            # examine domain
            if hostinfo != last_domain:
                last_domain = hostinfo
                known_paths = {u.path(): u.visited for u in self._load_urls(hostinfo)}
            # run checks: case 1: the path matches, case 2: visited URL
            if urlpath in known_paths and (
                switch == 1 or (switch == 2 and known_paths[urlpath])
            ):
                del remaining_urls[url]
        # preserve input order
        return list(remaining_urls)

    # ADDITIONS AND DELETIONS

    def add_urls(
        self,
        urls: Optional[List[str]] = None,
        appendleft: Optional[List[str]] = None,
        visited: bool = False,
    ) -> None:
        """Add a list of URLs to the (possibly) existing one.
        Optional: append certain URLs to the left,
        specify if the URLs have already been visited."""
        if urls:
            for host, urltuples in self._buffer_urls(urls, visited).items():
                self._store_urls(host, to_right=urltuples)
        if appendleft:
            for host, urltuples in self._buffer_urls(appendleft, visited).items():
                self._store_urls(host, to_left=urltuples)

    def add_from_html(
        self,
        htmlstring: str,
        url: str,
        external: bool = False,
        lang: Optional[str] = None,
        with_nav: bool = True,
    ) -> None:
        "Find links in a HTML document, filter them and add them to the data store."
        # lang = lang or self.language
        base_url = get_base_url(url)
        rules = self.get_rules(base_url)
        links, links_priority = filter_links(
            htmlstring=htmlstring,
            url=url,
            external=external,
            lang=lang or self.language,
            rules=rules,
            strict=self.strict,
            with_nav=with_nav,
        )
        self.add_urls(urls=links, appendleft=links_priority)

    def discard(self, domains: List[str]) -> None:
        "Declare domains void and prune the store."
        with self._lock:
            for d in domains:
                self.urldict[d] = DomainEntry(state=State.BUSTED)
        self._set_done()
        num = gc.collect()
        LOGGER.debug("%s objects in GC after UrlStore.discard", num)

    def reset(self) -> None:
        "Re-initialize the URL store."
        with self._lock:
            self.urldict = defaultdict(DomainEntry)
        clear_caches()
        num = gc.collect()
        LOGGER.debug("UrlStore reset, %s objects in GC", num)

    # DOMAINS / HOSTNAMES

    def get_known_domains(self) -> List[str]:
        "Return all known domains as a list."
        return list(self.urldict.keys())

    def get_unvisited_domains(self) -> List[str]:
        """Find all domains for which there are unvisited URLs
        and potentially adjust done meta-information."""
        return [d for d, v in self.urldict.items() if v.state == State.OPEN]

    def is_exhausted_domain(self, domain: str) -> bool:
        "Tell if all known URLs for the website have been visited."
        if domain in self.urldict:
            return self.urldict[domain].state != State.OPEN
        return False
        # raise KeyError("website not in store")

    def unvisited_websites_number(self) -> int:
        "Return the number of websites for which there are still URLs to visit."
        return len(self.get_unvisited_domains())

    # URL-BASED QUERIES

    def find_known_urls(self, domain: str) -> List[str]:
        """Get all already known URLs for the given domain (ex. "https://example.org")."""
        return [domain + u.path() for u in self._load_urls(domain)]

    def find_unvisited_urls(self, domain: str) -> List[str]:
        "Get all unvisited URLs for the given domain."
        if not self.is_exhausted_domain(domain):
            return [domain + u.path() for u in self._load_urls(domain) if not u.visited]
        return []

    def filter_unknown_urls(self, urls: List[str]) -> List[str]:
        "Take a list of URLs and return the currently unknown ones."
        return self._search_urls(urls, switch=1)

    def filter_unvisited_urls(self, urls: List[str]) -> List[Union[Any, str]]:
        "Take a list of URLs and return the currently unvisited ones."
        return self._search_urls(urls, switch=2)

    def has_been_visited(self, url: str) -> bool:
        "Check if the given URL has already been visited."
        return not bool(self.filter_unvisited_urls([url]))

    def is_known(self, url: str) -> bool:
        "Check if the given URL has already been stored."
        hostinfo, urlpath = get_host_and_path(url)
        # returns False if domain or URL is new
        return urlpath in {u.path() for u in self._load_urls(hostinfo)}

    # DOWNLOADS

    def get_url(self, domain: str, as_visited: bool = True) -> Optional[str]:
        "Retrieve a single URL and consider it to be visited (with corresponding timestamp)."
        # not fully used
        if not self.is_exhausted_domain(domain):
            url_tuples = self._load_urls(domain)
            # get first non-seen url
            for url in url_tuples:
                if not url.visited:
                    # store information
                    if as_visited:
                        url.visited = True
                        with self._lock:
                            self.urldict[domain].count += 1
                        self._store_urls(domain, url_tuples, timestamp=datetime.now())
                    return domain + url.path()
        # nothing to draw from
        with self._lock:
            self.urldict[domain].state = State.ALL_VISITED
        self._set_done()
        return None

    def get_download_urls(
        self,
        time_limit: float = 10.0,
        max_urls: int = 10000,
    ) -> List[str]:
        """Get a list of immediately downloadable URLs according to the given
        time limit per domain."""
        urls = []
        for website, entry in self.urldict.items():
            if entry.state != State.OPEN:
                continue
            if (
                not entry.timestamp
                or (datetime.now() - entry.timestamp).total_seconds() > time_limit
            ):
                url = self.get_url(website)
                if url is not None:
                    urls.append(url)
                    if len(urls) >= max_urls:
                        break
        self._set_done()
        return urls

    def establish_download_schedule(
        self, max_urls: int = 100, time_limit: int = 10
    ) -> List[str]:
        """Get up to the specified number of URLs along with a suitable
        backoff schedule (in seconds)."""
        # see which domains are free
        potential = self.get_unvisited_domains()
        if not potential:
            return []
        # variables init
        per_domain = max_urls // len(potential) or 1
        targets: List[Tuple[float, str]] = []
        # iterate potential domains
        for domain in potential:
            # load urls
            url_tuples = self._load_urls(domain)
            urlpaths: List[str] = []
            # get first non-seen urls
            for url in url_tuples:
                if (
                    len(urlpaths) >= per_domain
                    or (len(targets) + len(urlpaths)) >= max_urls
                ):
                    break
                if not url.visited:
                    urlpaths.append(url.path())
                    url.visited = True
                    with self._lock:
                        self.urldict[domain].count += 1
            # determine timestamps
            now = datetime.now()
            original_timestamp = self.urldict[domain].timestamp
            if (
                not original_timestamp
                or (now - original_timestamp).total_seconds() > time_limit
            ):
                schedule_secs = 0.0
            else:
                schedule_secs = time_limit - float(
                    f"{(now - original_timestamp).total_seconds():.2f}"
                )
            for urlpath in urlpaths:
                targets.append((schedule_secs, domain + urlpath))
                schedule_secs += time_limit
            # calculate difference and offset last addition
            total_diff = now + timedelta(0, schedule_secs - time_limit)
            # store new info
            self._store_urls(domain, url_tuples, timestamp=total_diff)
        # sort by first tuple element (time in secs)
        self._set_done()
        return sorted(targets, key=itemgetter(1))  # type: ignore[arg-type]

    # CRAWLING

    def store_rules(self, website: str, rules: Optional[RobotFileParser]) -> None:
        "Store crawling rules for a given website."
        if self.compressed:
            rules = COMPRESSOR.compress(rules)
        self.urldict[website].rules = rules

    def get_rules(self, website: str) -> Optional[RobotFileParser]:
        "Return the stored crawling rules for the given website."
        if website in self.urldict:
            if self.compressed:
                return COMPRESSOR.decompress(self.urldict[website].rules)  # type: ignore
            return self.urldict[website].rules
        return None

    def get_crawl_delay(self, website: str, default: float = 5) -> float:
        "Return the delay as extracted from robots.txt, or a given default."
        delay = None
        rules = self.get_rules(website)
        try:
            delay = rules.crawl_delay("*")  # type: ignore[union-attr]
        except AttributeError:  # no rules or no crawl delay
            pass
        # backup
        return delay or default  # type: ignore[return-value]

    # GENERAL INFO

    def get_all_counts(self) -> List[int]:
        "Return all download counts for the hosts in store."
        return [v.count for v in self.urldict.values()]

    def total_url_number(self) -> int:
        "Find number of all URLs in store."
        return sum(v.total for v in self.urldict.values())

    def download_threshold_reached(self, threshold: float) -> bool:
        "Find out if the download limit (in seconds) has been reached for one of the websites in store."
        return any(v.count >= threshold for v in self.urldict.values())

    def dump_urls(self) -> List[str]:
        "Return a list of all known URLs."
        urls = []
        for domain in self.urldict:
            urls.extend(self.find_known_urls(domain))
        return urls

    def print_unvisited_urls(self) -> None:
        "Print all unvisited URLs in store."
        for domain in self.urldict:
            print("\n".join(self.find_unvisited_urls(domain)), flush=True)

    def print_urls(self) -> None:
        "Print all URLs in store (URL + TAB + visited or not)."
        for domain in self.urldict:
            print(
                "\n".join(
                    [
                        f"{domain}{u.path()}\t{str(u.visited)}"
                        for u in self._load_urls(domain)
                    ]
                ),
                flush=True,
            )

    # PERSISTANCE

    def write(self, filename: str) -> None:
        "Write the URL store to disk."
        del self._lock
        with open(filename, "wb") as output:
            pickle.dump(self, output)


def load_store(filename: str) -> UrlStore:
    "Load a URL store from disk."
    with open(filename, "rb") as output:
        url_store = pickle.load(output)
    url_store._lock = Lock()
    return url_store  # type: ignore[no-any-return]
