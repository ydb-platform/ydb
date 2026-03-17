"""
Implements a basic command-line interface.
"""

import argparse
import logging
import sys

from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import islice
from typing import Any, Iterator, List, Optional, Tuple

from .core import check_url
from .sampling import _make_sample
from .urlstore import UrlStore


LOGGER = logging.getLogger(__name__)


def parse_args(args: Any) -> Any:
    """Define parser for command-line arguments"""
    argsparser = argparse.ArgumentParser(
        description="Command-line interface for Courlan"
    )
    group1 = argsparser.add_argument_group("I/O", "Manage input and output")
    group1.add_argument(
        "-i",
        "--inputfile",
        help="name of input file (required)",
        type=str,
        required=True,
    )
    group1.add_argument(
        "-o",
        "--outputfile",
        help="name of output file (required)",
        type=str,
        required=True,
    )
    group1.add_argument(
        "-d",
        "--discardedfile",
        help="name of file to store discarded URLs (optional)",
        type=str,
    )
    group1.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    group1.add_argument(
        "-p",
        "--parallel",
        help="number of parallel processes (not used for sampling)",
        type=int,
    )
    group2 = argsparser.add_argument_group("Filtering", "Configure URL filters")
    group2.add_argument(
        "--strict", help="perform more restrictive tests", action="store_true"
    )
    group2.add_argument(
        "-l", "--language", help="use language filter (ISO 639-1 code)", type=str
    )
    group2.add_argument(
        "-r", "--redirects", help="check redirects", action="store_true"
    )
    group3 = argsparser.add_argument_group(
        "Sampling", "Use sampling by host, configure sample size"
    )
    group3.add_argument("--sample", help="size of sample per domain", type=int)
    group3.add_argument(
        "--exclude-max", help="exclude domains with more than n URLs", type=int
    )
    group3.add_argument(
        "--exclude-min", help="exclude domains with less than n URLs", type=int
    )
    return argsparser.parse_args()


def _cli_check_urls(
    urls: List[str],
    strict: bool = False,
    with_redirects: bool = False,
    language: Optional[str] = None,
    with_nav: bool = False,
) -> List[Tuple[bool, str]]:
    "Internal function to be used with CLI multiprocessing."
    results = []
    for url in urls:
        result = check_url(
            url,
            strict=strict,
            with_redirects=with_redirects,
            language=language,
            with_nav=with_nav,
        )
        if result is not None:
            results.append((True, result[0]))
        else:
            results.append((False, url))
    return results


def _batch_lines(inputfile: str) -> Iterator[List[str]]:
    "Read input line in batches"
    with open(inputfile, "r", encoding="utf-8", errors="ignore") as inputfh:
        while True:
            batch = [line.strip() for line in islice(inputfh, 10**5)]
            if not batch:
                return
            yield batch


def _cli_sample(args: Any) -> None:
    "Sample URLs on the CLI."
    if args.verbose:
        LOGGER.setLevel(logging.DEBUG)
    else:
        LOGGER.setLevel(logging.ERROR)

    urlstore = UrlStore(
        compressed=True, language=None, strict=args.strict, verbose=args.verbose
    )
    for batch in _batch_lines(args.inputfile):
        urlstore.add_urls(batch)

    with open(args.outputfile, "w", encoding="utf-8") as outputfh:
        for url in _make_sample(
            urlstore,
            args.sample,
            exclude_min=args.exclude_min,
            exclude_max=args.exclude_max,
        ):
            outputfh.write(url + "\n")


def _cli_process(args: Any) -> None:
    "Read input file bit by bit and process URLs in batches."
    with ProcessPoolExecutor(max_workers=args.parallel) as executor, open(
        args.outputfile, "w", encoding="utf-8"
    ) as outputfh, open(
        args.inputfile, "r", encoding="utf-8", errors="ignore"
    ) as inputfh:
        while True:
            batches = []  # type: List[List[str]]
            while len(batches) < 1000:
                line_batch = list(islice(inputfh, 1000))
                if not line_batch:
                    break
                batches.append(line_batch)

            if not batches:
                break

            futures = (
                executor.submit(
                    _cli_check_urls,
                    batch,
                    strict=args.strict,
                    with_redirects=args.redirects,
                    language=args.language,
                )
                for batch in batches
            )

            for future in as_completed(futures):
                for valid, url in future.result():
                    if valid:
                        outputfh.write(url + "\n")
                    # proceed with discarded URLs. to be rewritten
                    elif args.discardedfile is not None:
                        with open(
                            args.discardedfile, "a", encoding="utf-8"
                        ) as discardfh:
                            discardfh.write(url)


def process_args(args: Any) -> None:
    """Start processing according to the arguments"""
    if args.sample:
        _cli_sample(args)
    else:
        _cli_process(args)


def main() -> None:
    """Run as a command-line utility."""
    args = parse_args(sys.argv[1:])
    process_args(args)


if __name__ == "__main__":
    main()
