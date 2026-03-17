#! /usr/bin/env python3

"""Simple Atop log processor."""

import argparse
import gzip
import json

import atoparser
from atoparser.parsers import atop_1_26

PARSEABLES = ["cpu", "CPL", "CPU", "DSK", "LVM", "MDD", "MEM", "NETL", "NETU", "PAG", "PRC", "PRG", "PRM", "PRN", "SWP"]
PARSEABLE_MAP = {
    "1.26": {parseable: getattr(atop_1_26, f"parse_{parseable}") for parseable in PARSEABLES},
}


def parse_args() -> argparse.Namespace:
    """Parse user arguments.

    Returns:
        Namespace with all the user arguments.
    """
    parser = argparse.ArgumentParser(
        "Convert binary files into JSON output.",
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="Files to process. May be uncompressed or gzip compressed.",
    )
    parser.add_argument(
        "-P",
        "--parseables",
        nargs="+",
        choices=PARSEABLES,
        metavar="PARSEABLE",
        help='Display output in Atop "parseable" format, instead of full structs. See "man atop" for full details. e.g. "CPU", "DSK", etc.',
    )
    parser.add_argument(
        "-p",
        "--pretty-print",
        action="store_true",
        help="Pretty print the JSON output with indentation.",
    )
    parser.add_argument(
        "--tstats",
        action="store_true",
        help="Include TStats/PStats in output. Very verbose.",
    )
    parser.add_argument(
        "--cstats",
        action="store_true",
        help="Include CGroup/CStats in output. Only available with Atop 2.11+ logs. Verbose.",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    """Primary function to load Atop data."""
    args = parse_args()

    for file in args.files:
        samples = []
        opener = open if ".gz" not in file else gzip.open
        with opener(file, "rb") as raw_file:
            header = atoparser.get_header(raw_file)
            if args.parseables and header.semantic_version not in PARSEABLE_MAP:
                samples.append(
                    {
                        "error": f"Atop version {header.semantic_version} does not support parseables, only full raw output.",
                        "file": file,
                    }
                )
                continue
            parsers = PARSEABLE_MAP.get(header.semantic_version, PARSEABLE_MAP["1.26"])
            for record, sstat, tstats, cgroups in atoparser.generate_statistics(
                raw_file,
                header,
                raise_on_truncation=False,
            ):
                if args.parseables:
                    for parseable in args.parseables:
                        for sample in parsers[parseable](header, record, sstat, tstats):
                            sample["parseable"] = parseable
                            samples.append(sample)
                else:
                    converted = {
                        "header": atoparser.struct_to_dict(header),
                        "record": atoparser.struct_to_dict(record),
                        "sstat": atoparser.struct_to_dict(sstat),
                    }
                    if args.tstats:
                        converted["tstat"] = [atoparser.struct_to_dict(stat) for stat in tstats]
                    if args.cstats:
                        converted["cgroup"] = [atoparser.struct_to_dict(stat) for stat in cgroups]
                    samples.append(converted)
        print(json.dumps(samples, indent=2 if args.pretty_print else None))


if __name__ == "__main__":
    main()
