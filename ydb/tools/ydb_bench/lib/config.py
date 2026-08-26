import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

import yaml
from yaml.constructor import ConstructorError

from ydb.tools.ydb_bench.benchmarks import BENCHMARKS
from ydb.tools.ydb_bench.lib.actors_core import RunConfiguration
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.topology import AFFINITY_MODES

PROFILE_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"
_PROFILE_NAME_RE = re.compile(PROFILE_NAME_PATTERN)
_COMMON_REQUIRED_FIELDS = ("threads", "duration", "repetitions", "affinity")
BACKGROUND_LOAD_MODES = (
    "none",
    "memory-bandwidth",
    "coherence-chiplet",
    "coherence-numa",
    "coherence-all-numa",
)
_COMMON_OPTIONAL_FIELDS = ("timeout", "background-load")


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as error:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable key",
                key_node.start_mark,
            ) from error
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found duplicate key {!r}".format(key),
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _positive_integer_array(description):
    return {
        "type": "array",
        "description": description,
        "items": {"type": "integer", "minimum": 1},
        "minItems": 1,
        "uniqueItems": True,
    }


def _parameter_schema(parameter):
    item = {"type": parameter.value_type}
    if parameter.minimum is not None:
        item["minimum"] = parameter.minimum
    if parameter.maximum is not None:
        item["maximum"] = parameter.maximum
    if parameter.choices:
        item["enum"] = list(parameter.choices)
    return {"type": "array", "description": parameter.description, "items": item, "minItems": 1, "uniqueItems": True}


def _profile_schema(benchmark):
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(_COMMON_REQUIRED_FIELDS),
        "properties": {
            "threads": _positive_integer_array("Actor-system worker thread counts."),
            **{parameter.name: _parameter_schema(parameter) for parameter in benchmark.parameters},
            "duration": {
                "type": "integer",
                "minimum": 1,
                "description": "Measurement duration in seconds.",
            },
            "repetitions": {
                "type": "integer",
                "minimum": 1,
                "description": "Number of independent external repetitions.",
            },
            "affinity": {
                "type": "array",
                "items": {"enum": list(AFFINITY_MODES)},
                "minItems": 1,
                "uniqueItems": True,
            },
            "timeout": {
                "type": "number",
                "exclusiveMinimum": 0,
                "description": "Per-process timeout in seconds; computed automatically when omitted.",
            },
            "background-load": {
                "type": "array",
                "items": {"enum": list(BACKGROUND_LOAD_MODES)},
                "minItems": 1,
                "uniqueItems": True,
                "default": ["none"],
                "description": "Additional workload placed on unused physical cores.",
            },
        },
    }


def _profiles_schema(profile_schema):
    return {
        "type": "object",
        "description": "Profiles keyed by user-defined names.",
        "minProperties": 1,
        "propertyNames": {"pattern": PROFILE_NAME_PATTERN},
        "additionalProperties": profile_schema,
    }


def config_schema(registry=BENCHMARKS):
    """Build the public schema from registered benchmark adapters."""
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "ydb_bench configuration",
        "type": "object",
        "minProperties": 1,
        "additionalProperties": False,
        "properties": {benchmark.name: _profiles_schema(_profile_schema(benchmark)) for benchmark in registry.values()},
    }


# Compatibility value for consumers that import the schema directly.
CONFIG_SCHEMA = config_schema()


@dataclass(frozen=True)
class LoadedConfig:
    path: Path
    sha256: str
    runs: tuple


@dataclass(frozen=True)
class RunStep:
    id: str
    benchmark: str
    profile: str
    affinity: str
    background_load: str
    threads: int
    case: int
    parameters: object
    repeat: int
    configuration: RunConfiguration


@dataclass(frozen=True)
class RunPlan:
    config_path: Path
    config_sha256: str
    steps: tuple
    step_ids: object


def _step_key(benchmark, profile, affinity, background_load, threads, case, repeat):
    return benchmark, profile, affinity, background_load, threads, case, repeat


def build_run_plan(loaded_config):
    """Expand validated config in YAML/config order into an immutable queue."""
    steps = []
    step_ids = {}
    for configuration in loaded_config.runs:
        for affinity in configuration.affinity_modes:
            for background_load in configuration.background_load_modes:
                for case_index, case in enumerate(configuration.benchmark.process_cases(configuration), 1):
                    threads = case["threads"]
                    for repeat in range(1, configuration.repetitions + 1):
                        step_id = "{:04d}-{}-{}-{}-{}-t{:03d}-c{:03d}-r{:03d}".format(
                            len(steps) + 1,
                            configuration.benchmark.name,
                            configuration.profile,
                            affinity,
                            background_load,
                            threads,
                            case_index,
                            repeat,
                        )
                        step = RunStep(
                            step_id,
                            configuration.benchmark.name,
                            configuration.profile,
                            affinity,
                            background_load,
                            threads,
                            case_index,
                            case["parameters"],
                            repeat,
                            configuration,
                        )
                        steps.append(step)
                        step_ids[
                            _step_key(
                                step.benchmark,
                                step.profile,
                                step.affinity,
                                step.background_load,
                                step.threads,
                                step.case,
                                step.repeat,
                            )
                        ] = step.id
    return RunPlan(loaded_config.path, loaded_config.sha256, tuple(steps), MappingProxyType(step_ids))


def _config_error(location, message):
    raise BenchmarkError("invalid benchmark config at {}: {}".format(location, message))


def _positive_integer(value, location):
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _config_error(location, "must be a positive integer")
    return value


def _positive_integer_list(value, location):
    if not isinstance(value, list) or not value:
        _config_error(location, "must be a non-empty array of positive integers")
    parsed = tuple(_positive_integer(item, "{}[{}]".format(location, index)) for index, item in enumerate(value))
    if len(set(parsed)) != len(parsed):
        _config_error(location, "must not contain duplicate values")
    return parsed


def _nonnegative_integer_list(value, location):
    if not isinstance(value, list) or not value:
        _config_error(location, "must be a non-empty array of non-negative integers")
    if any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in value):
        _config_error(location, "must contain only non-negative integers")
    if len(set(value)) != len(value):
        _config_error(location, "must not contain duplicate values")
    return tuple(value)


def _affinity_modes(value, location):
    if not isinstance(value, list) or not value:
        _config_error(location, "must be a non-empty array")
    if any(not isinstance(mode, str) for mode in value):
        _config_error(location, "must contain only affinity mode names")
    invalid = sorted(set(value) - set(AFFINITY_MODES))
    if invalid:
        _config_error(location, "contains unknown modes: {}".format(", ".join(invalid)))
    if len(set(value)) != len(value):
        _config_error(location, "must not contain duplicate values")
    return tuple(value)


def _background_load_modes(value, location):
    if not isinstance(value, list) or not value or not all(isinstance(mode, str) for mode in value):
        _config_error(location, "must be a non-empty array of background load modes")
    invalid = sorted(set(value) - set(BACKGROUND_LOAD_MODES))
    if invalid:
        _config_error(location, "contains unknown modes: {}".format(", ".join(invalid)))
    if len(set(value)) != len(value):
        _config_error(location, "must not contain duplicate values")
    return tuple(value)


def _timeout(value, location):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _config_error(location, "must be a finite positive number")
    value = float(value)
    if value <= 0 or not math.isfinite(value):
        _config_error(location, "must be a finite positive number")
    return value


def _parse_profile(benchmark, profile_name, value, perf_enabled, perf_frequency):
    location = "{}.{}".format(benchmark.name, profile_name)
    if not isinstance(value, dict):
        _config_error(location, "must be a mapping")

    allowed_fields = set(
        _COMMON_REQUIRED_FIELDS + _COMMON_OPTIONAL_FIELDS + tuple(item.name for item in benchmark.parameters)
    )
    missing = sorted(set(_COMMON_REQUIRED_FIELDS) - set(value))
    unknown = sorted((field for field in value if field not in allowed_fields), key=str)
    if missing:
        _config_error(location, "missing required fields: {}".format(", ".join(missing)))
    if unknown:
        _config_error(location, "contains unknown fields: {}".format(", ".join(map(str, unknown))))

    threads = _positive_integer_list(value["threads"], location + ".threads")
    parameters = {}
    for parameter in benchmark.parameters:
        raw = value.get(parameter.name, list(parameter.default))
        if parameter.value_type == "integer":
            parsed = (
                _positive_integer_list(raw, location + "." + parameter.name)
                if parameter.minimum != 0
                else _nonnegative_integer_list(raw, location + "." + parameter.name)
            )
        elif parameter.value_type == "string":
            if not isinstance(raw, list) or not raw or not all(isinstance(item, str) for item in raw):
                _config_error(location + "." + parameter.name, "must be a non-empty array of strings")
            parsed = tuple(raw)
        else:
            _config_error(location + "." + parameter.name, "uses unsupported parameter type")
        if parameter.minimum is not None and any(item < parameter.minimum for item in parsed):
            _config_error(location + "." + parameter.name, "contains a value below {}".format(parameter.minimum))
        if parameter.choices and any(item not in parameter.choices for item in parsed):
            _config_error(
                location + "." + parameter.name, "contains a value outside {}".format(list(parameter.choices))
            )
        if parameter.maximum is not None and any(item > parameter.maximum for item in parsed):
            _config_error(location + "." + parameter.name, "contains a value above {}".format(parameter.maximum))
        parameters[parameter.name] = parsed
    duration = _positive_integer(value["duration"], location + ".duration")
    repetitions = _positive_integer(value["repetitions"], location + ".repetitions")
    affinity = _affinity_modes(value["affinity"], location + ".affinity")
    background_load = _background_load_modes(
        value.get("background-load", ["none"]), location + ".background-load"
    )
    timeout_explicit = "timeout" in value
    timeout = _timeout(
        value["timeout"] if timeout_explicit else benchmark.process_measurement_count(parameters) * duration * 3 + 30,
        location + ".timeout",
    )
    return RunConfiguration(
        benchmark=benchmark,
        profile=profile_name,
        threads=threads,
        parameters=parameters,
        duration_seconds=duration,
        repetitions=repetitions,
        timeout_seconds=timeout,
        timeout_explicit=timeout_explicit,
        affinity_modes=affinity,
        background_load_modes=background_load,
        perf_enabled=perf_enabled,
        perf_frequency=perf_frequency,
    )


def load_config(path, perf_enabled=False, perf_frequency=99):
    path = Path(path)
    try:
        data = path.read_bytes()
    except OSError as error:
        raise BenchmarkError("cannot read benchmark config {}: {}".format(path, error)) from error
    try:
        document = yaml.load(data, Loader=_UniqueKeyLoader)
    except yaml.YAMLError as error:
        raise BenchmarkError("cannot parse benchmark config {}: {}".format(path, error)) from error

    if not isinstance(document, dict) or not document:
        _config_error("$", "must be a non-empty mapping")

    unknown_benchmarks = sorted(
        (benchmark for benchmark in document if benchmark not in BENCHMARKS),
        key=str,
    )
    if unknown_benchmarks:
        _config_error("$", "contains unknown benchmarks: {}".format(", ".join(map(str, unknown_benchmarks))))

    runs = []
    for benchmark_name, profiles in document.items():
        benchmark = BENCHMARKS[benchmark_name]
        if not isinstance(profiles, dict) or not profiles:
            _config_error(benchmark_name, "must contain at least one profile")
        for profile_name, profile in profiles.items():
            if not isinstance(profile_name, str) or not _PROFILE_NAME_RE.fullmatch(profile_name):
                _config_error(
                    benchmark_name,
                    "profile names must match {}".format(PROFILE_NAME_PATTERN),
                )
            runs.append(_parse_profile(benchmark, profile_name, profile, perf_enabled, perf_frequency))

    return LoadedConfig(path=path.resolve(), sha256=hashlib.sha256(data).hexdigest(), runs=tuple(runs))
