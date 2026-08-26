"""Generic benchmark metadata and ordered registry."""

from collections import OrderedDict
from dataclasses import dataclass


def _single_process_measurement(_parameters):
    return 1


@dataclass(frozen=True)
class ParameterDefinition:
    name: str
    description: str
    value_type: str = "integer"
    default: tuple = ()
    required: bool = False
    matrix: bool = False
    minimum: object = 1
    maximum: object = None
    choices: tuple = ()
    environment: str = ""
    column: str = ""


@dataclass(frozen=True)
class DimensionDefinition:
    name: str
    value_type: str = "integer"
    description: str = ""
    series: bool = True


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    unit: str
    description: str = ""
    aggregate: bool = True


@dataclass(frozen=True)
class BenchmarkDefinition:
    name: str
    description: str
    resource_name: str
    parameters: tuple
    dimensions: tuple
    metrics: tuple
    parse_metrics: object
    render_metrics: object
    validate_metrics: object
    summarize_metrics: object
    render_summary: object
    command: object
    environment: object
    process_cases: object
    parse_worker_metrics: object = None
    render_worker_metrics: object = None
    test_filter: str = ""
    process_measurement_count: object = _single_process_measurement

    @property
    def csv_columns(self):
        return tuple(item.name for item in self.dimensions + self.metrics)

    @property
    def csv_header(self):
        return ",".join(self.csv_columns)

    @property
    def parameter_name(self):
        """Compatibility for older discovery/UI clients."""
        varying = [item for item in self.parameters if item.name != "actor-pairs"]
        return varying[0].name if varying else self.parameters[0].name

    @property
    def parameter_description(self):
        return next(item.description for item in self.parameters if item.name == self.parameter_name)

    @property
    def parameter_environment(self):
        return next(item.environment for item in self.parameters if item.name == self.parameter_name)

    @property
    def parameter_column(self):
        item = next(item for item in self.parameters if item.name == self.parameter_name)
        return item.column or item.name.replace("-", "_")

    def parameter(self, name):
        return next(item for item in self.parameters if item.name == name)


class BenchmarkRegistry:
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
