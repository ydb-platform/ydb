import argparse
import bisect
import collections
import csv
import gc
import io
import itertools
import math
import os
import random
import sys
import threading
import time
import types
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from . import (
    BasicResolver,
    CachingResolver,
    Domain,
    Matchers,
    Parser,
    PartialResult,
    Resolver,
    caching,
)
from .caching import Cache, Local
from .loaders import load_builtins, load_yaml

try:
    from .re2 import Resolver as Re2Resolver
except ImportError:
    pass
try:
    from .regex import Resolver as RegexResolver
except ImportError:
    pass
from .user_agent_parser import Parse

CACHEABLE = {
    "basic": True,
    "re2": True,
    "regex": True,
    "legacy": False,
}


CACHES: Dict[str, Optional[Callable[[int], Cache]]] = {"none": None}
CACHES.update(
    (cache.__name__.lower(), cache)
    for cache in [
        cast(Callable[[int], Cache], caching.Lru),
        caching.S3Fifo,
        caching.Sieve,
    ]
)

try:
    import tracemalloc
except ImportError:
    snapshot = types.SimpleNamespace(
        compare_to=lambda _1, _2: [],
    )
    tracemalloc = types.SimpleNamespace(  # type: ignore
        start=lambda: None,
        take_snapshot=lambda: snapshot,
    )


def get_rules(parsers: List[str], regexes: Optional[io.IOBase]) -> Matchers:
    if regexes:
        if not load_yaml:
            sys.exit("yaml loading unavailable, please install pyyaml")

        rules = load_yaml(regexes)
        if "legacy" in parsers:
            print(
                "The legacy parser is incompatible with custom regexes, ignoring.",
                file=sys.stderr,
            )
            parsers.remove("legacy")
    else:
        rules = load_builtins()

    return rules


def run_stdout(args: argparse.Namespace) -> None:
    lines = list(args.file)
    count = len(lines)
    uniques = len(set(lines))
    print(f"{args.file.name}: {count} lines, {uniques} unique ({uniques / count:.0%})")

    rules = get_rules(args.bases, args.regexes)

    # width of the parser label
    w = math.ceil(
        3
        + max(map(len, args.bases))
        + max(map(len, args.caches))
        + max(map(math.log10, args.cachesizes))
    )
    for p, c, n in (
        (p, c, n)
        for p in args.bases
        for c in (args.caches if CACHEABLE[p] and args.cachesizes != [0] else ["none"])
        for n in (args.cachesizes if c != "none" else [0])
    ):
        name = "-".join(map(str, filter(None, (p, c != "none" and c, n))))
        print(f"{name:{w}}", end=": ", flush=True)

        p = get_parser(p, c, n, rules)
        t = run(p, lines)

        secs = t / 1e9
        tpl = t / 1000 / len(lines)

        print(f"{secs:>5.2f}s ({tpl:>4.0f}us/line)")


def run_csv(args: argparse.Namespace) -> None:
    lines = list(args.file)
    LEN = len(lines) * 1000
    rules = get_rules(args.bases, args.regexes)

    parsers = [
        (p, c, n)
        for p in args.bases
        for c in (args.caches if CACHEABLE[p] else ["none"])
        for n in (args.cachesizes if c != "none" else [0])
    ]
    if not parsers:
        sys.exit("No parser selected")

    columns = {"size": ""}
    columns.update(
        (f"{p}-{c}", p if c == "none" else f"{p}-{c}")
        for p in args.bases
        for c in (args.caches if CACHEABLE[p] else ["none"])
    )
    w = csv.DictWriter(
        sys.stdout,
        list(columns),
        dialect="unix",
        quoting=csv.QUOTE_MINIMAL,
    )
    w.writerow(columns)

    parsers.sort(key=lambda t: t[2])
    grouped = itertools.groupby(parsers, key=lambda t: t[2])

    # these are the "template rows", which contain the no-cache
    # runs which get replicated on every cachesize row
    zeroes = {}
    # if we have entries with no cache size, compute them first so
    # we can apply them to every cachesize
    if parsers[0][2] == 0:
        (_, ps) = next(grouped)
        # cache could be ignored as it should always be `"none"`
        for parser, cache, _ in ps:
            p = get_parser(parser, cache, 0, rules)
            zeroes[f"{parser}-{cache}"] = run(p, lines) // LEN

    # special cases for configurations where we can't have
    # cachesize lines, write the template row out directly
    if args.bases == ["legacy"] or args.caches == ["none"] or args.cachesizes == [0]:
        zeroes["size"] = 0
        w.writerow(zeroes)
        return

    for cachesize, ps in grouped:
        row = dict(zeroes, size=cachesize)
        for parser, cache, _ in ps:
            p = get_parser(parser, cache, cachesize, rules)
            row[f"{parser}-{cache}"] = run(p, lines) // LEN
        w.writerow(row)


def get_parser(
    parser: str, cache: str, cachesize: int, rules: Matchers
) -> Callable[[str], Any]:
    r: Resolver
    if parser == "legacy":
        return Parse
    elif parser == "basic":
        r = BasicResolver(rules)
    elif parser == "re2":
        r = Re2Resolver(rules)
    elif parser == "regex":
        r = RegexResolver(rules)
    else:
        sys.exit(f"unknown parser {parser!r}")

    if cache not in CACHES:
        sys.exit(f"unknown cache algorithm {cache!r}")

    c = CACHES.get(cache)
    if c is None:
        return Parser(r).parse

    return Parser(CachingResolver(r, c(cachesize))).parse


def run(
    parse: Callable[[str], None],
    lines: Iterable[str],
) -> int:
    t = time.perf_counter_ns()
    for line in lines:
        parse(line)
    return time.perf_counter_ns() - t


class Belady:
    def __init__(self, maxsize: int, data: List[str]):
        self.maxsize = maxsize
        self.cache: Dict[str, PartialResult] = {}
        self.queue: Deque[Tuple[int, str]] = collections.deque()
        self.distances: Dict[str, List[int]] = {}
        for i, e in enumerate(data):
            self.distances.setdefault(e, []).append(i)
        for freqs in self.distances.values():
            freqs.reverse()

    def __getitem__(self, key: str) -> Optional[PartialResult]:
        self.distances[key].pop()
        if c := self.cache.get(key):
            # on cache hit, the entry should be the lowest in the
            # queue
            assert self.queue.popleft()[1] == key
            # if the key has future occurrences
            if ds := self.distances[key]:
                # reinsert in queue
                bisect.insort(self.queue, (ds[-1], key))
            else:
                # otherwise remove from cache & occurrences map
                del self.cache[key]

        return c

    def __setitem__(self, key: str, entry: PartialResult) -> None:
        # if there are no future occurrences just bail
        ds = self.distances[key]
        if not ds:
            return

        next_distance = ds[-1]
        # if the cache has room, just add the entry
        if len(self.cache) >= self.maxsize:
            # if the next occurrence of the new entry is later than
            # every existing occurrence, ignore it
            if next_distance > self.queue[-1][0]:
                return
            # otherwise remove the latest entry
            _, k = self.queue.pop()
            del self.cache[k]

        self.cache[key] = entry
        bisect.insort(self.queue, (next_distance, key))


def run_hitrates(args: argparse.Namespace) -> None:
    r = PartialResult(
        domains=Domain.ALL,
        string="",
        user_agent=None,
        os=None,
        device=None,
    )

    class Counter:
        def __init__(self) -> None:
            self.count = 0

        def __call__(self, ua: str, domains: Domain, /) -> PartialResult:
            self.count += 1
            return r

    lines = list(args.file)
    total = len(lines)
    uniques = len(set(lines))
    print(total, "lines", uniques, "uniques")
    print()
    w = int(math.log10(max(args.cachesizes)) + 1)

    def belady(maxsize: int) -> Cache:
        return Belady(maxsize, lines)

    tracemalloc.start()
    for cache, cache_size in itertools.product(
        itertools.chain([belady], filter(None, CACHES.values())),
        args.cachesizes,
    ):
        misses = Counter()
        gc.collect()
        before = tracemalloc.take_snapshot()
        parser = Parser(CachingResolver(misses, cache(cache_size)))
        for line in lines:
            parser.parse(line)
        gc.collect()
        after = tracemalloc.take_snapshot()
        if cache == belady:
            diff = "{0:>14} {0:>12}".format("-")
        else:
            overhead = sum(s.size_diff for s in after.compare_to(before, "filename"))
            diff = "{:8} bytes ({:3.0f}b/entry)".format(
                overhead,
                overhead / cache_size,
            )
        print(
            f"{cache.__name__.lower():8}({cache_size:{w}}): {(total - misses.count) / total * 100:2.0f}% hit rate {diff}"
        )
        del misses, parser


CACHESIZE = 1000


def worker(
    start: threading.Event,
    parser: Parser,
    lines: Iterable[str],
    end: threading.Barrier,
) -> None:
    start.wait()

    for ua in lines:
        parser.parse(ua)

    end.wait()


def run_threaded(args: argparse.Namespace) -> None:
    lines = list(args.file)
    basic = BasicResolver(load_builtins())
    resolvers: List[Tuple[str, Resolver]] = [
        ("locking-lru", CachingResolver(basic, caching.Lru(CACHESIZE))),
        ("local-lru", CachingResolver(basic, Local(lambda: caching.Lru(CACHESIZE)))),
        ("re2", Re2Resolver(load_builtins())),
        ("regex", RegexResolver(load_builtins())),
    ]
    for name, resolver in resolvers:
        print(f"{name:11}: ", end="", flush=True)
        # randomize the dataset for each thread, predictably, to
        # simulate distributed load (not great but better than
        # nothing, and probably better than reusing the exact same
        # load)
        r = random.Random(42)
        start = threading.Event()
        end = threading.Barrier(args.threads + 1)

        parser = Parser(resolver)
        for _ in range(args.threads):
            threading.Thread(
                target=worker,
                args=(start, parser, r.sample(lines, len(lines)), end),
                daemon=True,
            ).start()

        st = time.perf_counter_ns()
        start.set()
        end.wait()

        # each thread gets len(lines), so total number of processed
        # lines is t*len(lines)
        totlines = len(lines) * args.threads
        # runtime in us
        t = (time.perf_counter_ns() - st) / 1000
        print(f"{t / totlines:>4.0f}us/line", flush=True)


EPILOG = """For good results the sample `file` should be an actual
non-sorted non-deduplicated sample of user agent strings from traffic
on a comparable (or the actual) site or application targeted for
classification."""

parser = argparse.ArgumentParser(prog="ua_parser", epilog="epi")
parser.set_defaults(func=None)

fp = argparse.ArgumentParser(add_help=False)
fp.add_argument(
    "file",
    type=argparse.FileType("r", encoding="utf-8"),
    help="Sample user agent file, the file must contain a single user agent "
    "string per line, use `-` for stdin.",
)

sub = parser.add_subparsers(title="commands")

bench = sub.add_parser(
    "bench",
    help="benchmark various parser configurations on sample files",
    parents=[fp],
    epilog=EPILOG,
    description="""Different sites and applications can have different
    traffic pattenrs, and thus want different setups and tradeoffs.
    This subcommand allows testing ua-parser's different base
    resolvers, caches, anc cache sizes in order to customise the
    parser to the application's requirements. It's also useful to
    bench the library itself though.""",
)
bench.add_argument(
    "-R",
    "--regexes",
    type=argparse.FileType("rb"),
    help="""Custom regexes.yaml file, if ommitted the benchmark will
    use the embedded regexes file rom uap-core. Custom regexes files
    can allow evaluating the performance impact of new rules or
    cut-down reference files (if legacy rules are nor relevant to your
    needs). Because YAML is (mostly) a superset of JSON, JSON regexes
    files will also work fine.""",
)


class ToFunc(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Union[str, Sequence[str], None],
        option_string: Optional[str] = None,
    ) -> None:
        if values == "stdout":
            setattr(namespace, self.dest, run_stdout)
        elif values == "csv":
            setattr(namespace, self.dest, run_csv)
        else:
            raise ValueError(f"invalid output {values!r}")


bench.add_argument(
    "-O",
    "--output",
    choices=["stdout", "csv"],
    default=run_stdout,
    dest="func",
    action=ToFunc,
    help="""By default (`stdout`) the result of each configuration /
    combination is printed to stdout with the combination name
    followed by the total parse time for the file and the per-entry
    average. `csv` will instead output a valid CSV table to stdout,
    with a parser combination per column and a cache size per row.
    Combinations without cache will have the same value on every row.
    If no combination uses a cache, the output will have a single row
    with a first cell of value 0.""",
)
bench.add_argument(
    "--bases",
    nargs="+",
    choices=["basic", "re2", "regex", "legacy"],
    default=["basic", "re2", "regex", "legacy"],
    help="""Base resolvers to benchmark. `basic` is a linear search
    through the regexes file, `re2` is a prefiltered regex set
    implemented in C++, `regex` is a prefiltered regex set implemented
    in Rust, `legacy` is the legacy API (essentially a basic resolver
    with a clearing cache of fixed 200 entries, but less layered so
    usually slightly faster than an equivalent basic-based resolver).""",
)
bench.add_argument(
    "--caches",
    nargs="+",
    choices=list(CACHES),
    default=list(CACHES),
    help="""Cache implementations to test. `clearing` completely
    clears the cache when full, `lru` uses a least-recently-eviction
    policy. `lru` is not thread-safe, so `lru-threadsafe` adds a mutex
    and measures *uncontended* locking overhead.""",
)
bench.add_argument(
    "--cachesizes",
    nargs="+",
    type=int,
    default=[10, 20, 50, 100, 200, 500, 1000, 2000, 5000],
    help="""Caches are a classic way to trade memory for performances.
    Different base resolvers and traffic patterns have different
    benefits from caches, this option allows testing the benefits of
    various cache sizes (and thus amounts of memory used) on the cache
    strategies. """,
)

hitrates = sub.add_parser(
    "hitrates",
    help="measure hitrates of cache configurations against sample files",
    parents=[fp],
    epilog=EPILOG,
)
hitrates.set_defaults(func=run_hitrates)
hitrates.add_argument(
    "--cachesizes",
    nargs="+",
    type=int,
    default=[10, 20, 50, 100, 200, 500, 1000, 2000, 5000],
    help="""List of cache sizes to test hitrates for, for each cache
    algorithm. """,
)

threaded = sub.add_parser(
    "threading",
    help="estimate impact of concurrency and contention on different parser configurations",
    parents=[fp],
    epilog=EPILOG,
)
threaded.set_defaults(func=run_threaded)
threaded.add_argument(
    "-n",
    "--threads",
    type=int,
    default=os.cpu_count() or 1,
)

args = parser.parse_args()
if args.func:
    args.func(args)
else:
    parser.print_help()
