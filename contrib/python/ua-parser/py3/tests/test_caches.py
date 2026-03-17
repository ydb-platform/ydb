from collections import OrderedDict

import pytest  # type: ignore

from ua_parser import (
    CachingResolver,
    Domain,
    Parser,
    PartialResult,
)
from ua_parser.caching import Lru, S3Fifo, Sieve


def test_lru():
    """Tests that the cache entries do get moved when accessed, and are
    popped LRU-first.
    """
    cache = Lru(2)
    p = Parser(
        CachingResolver(lambda s, d: PartialResult(d, None, None, None, s), cache)
    )

    p.parse("a")
    p.parse("b")

    assert cache.cache == OrderedDict(
        [
            ("a", PartialResult(Domain.ALL, None, None, None, "a")),
            ("b", PartialResult(Domain.ALL, None, None, None, "b")),
        ]
    )

    p.parse("a")
    p.parse("c")
    assert cache.cache == OrderedDict(
        [
            ("a", PartialResult(Domain.ALL, None, None, None, "a")),
            ("c", PartialResult(Domain.ALL, None, None, None, "c")),
        ]
    )


@pytest.mark.parametrize("cache", [Lru, S3Fifo, Sieve])
def test_backfill(cache):
    """Tests that caches handle partial parsing correctly, by updating
    the existing entry when new parts get parsed, without evicting
    still-fitting entries.
    """
    misses = 0

    def resolver(ua: str, domains: Domain, /) -> PartialResult:
        nonlocal misses
        misses += 1
        return PartialResult(domains, None, None, None, ua)

    p = Parser(CachingResolver(resolver, cache(10)))

    # fill the cache first, no need to hit the entries twice because
    # S3 waits until it needs space in the main cache before demotes
    # (or promotes) from the probationary cache.
    for s in map(str, range(9)):
        p.parse(s)
    assert misses == 9
    # add a partial entry
    p.parse_user_agent("a")
    # fill the partial entry, counts as a miss since it needs to
    # resolve the new bit
    p.parse_os("a")
    assert misses == 11

    misses = 0
    # check that the original entries are all hits
    for s in map(str, range(9)):
        p.parse(s)
    assert misses == 0
