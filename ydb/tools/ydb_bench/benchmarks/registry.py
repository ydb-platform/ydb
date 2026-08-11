"""Generic benchmark model and ordered registry."""

from collections import OrderedDict
from dataclasses import dataclass


@dataclass(frozen=True)
class BenchmarkDefinition:
    name: str
    description: str
    test_filter: str
    parameter_name: str
    parameter_description: str
    parameter_environment: str
    parameter_column: str
    parse_metrics: object
    render_metrics: object
    validate_metrics: object
    summarize_metrics: object
    render_summary: object

    @property
    def csv_columns(self):
        return (
            "threads", "actorPairs", self.parameter_column, "msgs_per_sec", "elapsed_seconds",
            "min_pair_sent_msgs", "max_pair_sent_msgs",
        )

    @property
    def csv_header(self):
        return ",".join(self.csv_columns)


class BenchmarkRegistry:
    """An ordered collection of benchmark adapters, suitable for test injection."""

    def __init__(self):
        self._benchmarks = OrderedDict()

    def register(self, benchmark):
        if benchmark.name in self._benchmarks:
            raise ValueError("benchmark is already registered: {}".format(benchmark.name))
        self._benchmarks[benchmark.name] = benchmark
        return benchmark

    def get(self, name):
        return self._benchmarks[name]

    def values(self):
        return self._benchmarks.values()

    def __contains__(self, name):
        return name in self._benchmarks

    def __iter__(self):
        return iter(self._benchmarks)

    def __getitem__(self, name):
        return self._benchmarks[name]


BENCHMARKS = BenchmarkRegistry()
