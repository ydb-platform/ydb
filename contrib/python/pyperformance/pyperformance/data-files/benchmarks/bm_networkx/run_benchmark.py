"""
Some graph algorithm benchmarks using networkx

This uses the public domain Amazon data set from the SNAP benchmarks:

    https://snap.stanford.edu/data/amazon0302.html

Choice of benchmarks inspired by Timothy Lin's work here:

    https://www.timlrx.com/blog/benchmark-of-popular-graph-network-packages
"""

import collections
from pathlib import Path

import networkx

import pyperf


DATA_FILE = Path(__file__).parent / "data" / "amazon0302.txt.gz"


graph = networkx.read_adjlist(DATA_FILE)


def bench_shortest_path():
    collections.deque(networkx.shortest_path_length(graph, "0"))


def bench_connected_components():
    networkx.number_connected_components(graph)


def bench_k_core():
    networkx.k_core(graph)


BENCHMARKS = {
    "shortest_path": bench_shortest_path,
    "connected_components": bench_connected_components,
    "k_core": bench_k_core,
}


def add_cmdline_args(cmd, args):
    cmd.append(args.benchmark)


def add_parser_args(parser):
    parser.add_argument("benchmark", choices=BENCHMARKS, help="Which benchmark to run.")


if __name__ == "__main__":
    runner = pyperf.Runner(add_cmdline_args=add_cmdline_args)
    runner.metadata["description"] = "NetworkX benchmark"
    add_parser_args(runner.argparser)
    args = runner.parse_args()
    benchmark = args.benchmark

    runner.bench_func(args.benchmark, BENCHMARKS[args.benchmark])
