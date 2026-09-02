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
from ydb.tools.ydb_bench.lib.local_ydb_workloads import (
    allowed_load_parameters,
    allowed_slo_metrics,
    all_load_parameters,
    all_slo_percentiles,
    normalize_workload,
    validate_workload_profile,
    workload_definition,
    workload_effective_warmup_seconds,
    workload_config_schema,
)
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
MAX_LOCAL_YDB_VERIFICATION_REPETITIONS = 20


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
    if benchmark.profile_kind == "local-ydb":
        role_affinity = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "mode": {"enum": list(AFFINITY_MODES)},
                "cpus": {
                    "oneOf": [
                        {"type": "integer", "minimum": 1},
                        {"enum": ["one-chiplet", "remaining"]},
                    ]
                },
            },
        }
        return {
            "type": "object",
            "additionalProperties": False,
            "required": ["workload", "load"],
            "properties": {
                "workload": workload_config_schema(),
                "geometry": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "preset": {"enum": ["single", "storage", "custom"]},
                        "static-nodes": {"type": "integer", "minimum": 1},
                        "dynamic-nodes": {"type": "integer", "minimum": 1},
                        "max-dynamic-nodes": {"type": "integer", "minimum": 1},
                        "disk-size-gb": {"type": "integer", "minimum": 1},
                        "storage-groups": {"type": "integer", "minimum": 1},
                    },
                },
                "client": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"threads": {"type": "integer", "minimum": 1}},
                },
                "load": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["parameter"],
                    "properties": {
                        # ``mode`` and the flat controller fields are retained for
                        # compatibility with configs written before search and
                        # objective became separate concepts.
                        "mode": {"enum": ["points", "maximize-throughput", "latency-slo"]},
                        "parameter": {"enum": list(all_load_parameters())},
                        "allow-errors": {"type": "boolean"},
                        "values": {
                            "type": "array",
                            "items": {"type": "integer", "minimum": 1},
                            "minItems": 1,
                            "uniqueItems": True,
                        },
                        "start": {"type": "integer", "minimum": 1},
                        "maximum": {"type": "integer", "minimum": 1},
                        "multiplier": {"type": "number", "exclusiveMinimum": 1},
                        "target-role": {"enum": ["static", "dynamic", "total"]},
                        "plateau-gain-percent": {"type": "number", "minimum": 0},
                        "plateau-points": {"type": "integer", "minimum": 1},
                        "cpu-saturation-percent": {"type": "number", "exclusiveMinimum": 0, "maximum": 100},
                        "search-resolution-percent": {
                            "type": "number",
                            "exclusiveMinimum": 0,
                            "maximum": 100,
                        },
                        "slo": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "percentile": {"enum": list(all_slo_percentiles())},
                                "max-ms": {"type": "number", "minimum": 0},
                                "max-errors": {"type": "integer", "minimum": 0},
                                "min-achieved-rate-ratio": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 1,
                                },
                            },
                        },
                        "search": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["start", "maximum"],
                            "properties": {
                                "start": {"type": "integer", "minimum": 1},
                                "maximum": {"type": "integer", "minimum": 1},
                                "multiplier": {"type": "number", "exclusiveMinimum": 1},
                                "resolution-percent": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 100,
                                },
                            },
                        },
                        "objective": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["type"],
                            "properties": {
                                "type": {"enum": ["maximize-throughput", "latency-slo"]},
                                "target-role": {"enum": ["static", "dynamic", "total"]},
                                "plateau-gain-percent": {"type": "number", "minimum": 0},
                                "plateau-points": {"type": "integer", "minimum": 1},
                                "cpu-saturation-percent": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 100,
                                },
                                "percentile": {"enum": list(all_slo_percentiles())},
                                "max-ms": {"type": "number", "minimum": 0},
                                "max-errors": {"type": "integer", "minimum": 0},
                                "min-achieved-rate-ratio": {
                                    "type": "number",
                                    "exclusiveMinimum": 0,
                                    "maximum": 1,
                                },
                            },
                        },
                    },
                },
                "measurement": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "warmup": {"type": "integer", "minimum": 0},
                        "duration": {"type": "integer", "minimum": 1},
                        "repetitions": {"type": "integer", "minimum": 1},
                        "verification-repetitions": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": MAX_LOCAL_YDB_VERIFICATION_REPETITIONS,
                        },
                    },
                },
                "affinity": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "ydb-cli": role_affinity,
                        "static-nodes": role_affinity,
                        "dynamic-nodes": role_affinity,
                    },
                },
                "timeout": {"type": "number", "exclusiveMinimum": 0},
            },
        }
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


def _mapping(value, location, allowed=()):
    if value is None:
        return {}
    if not isinstance(value, dict):
        _config_error(location, "must be a mapping")
    unknown = sorted((item for item in value if item not in allowed), key=str)
    if unknown:
        _config_error(location, "contains unknown fields: {}".format(", ".join(map(str, unknown))))
    return value


def _choice(value, choices, location):
    if value not in choices:
        _config_error(location, "must be one of {}".format(", ".join(choices)))
    return value


def _boolean(value, location):
    if not isinstance(value, bool):
        _config_error(location, "must be a boolean")
    return value


def _nonnegative_integer(value, location):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _config_error(location, "must be a non-negative integer")
    return value


def _finite_number(value, location, minimum=None, maximum=None, exclusive_minimum=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        _config_error(location, "must be a finite number")
    result = float(value)
    if minimum is not None and (result <= minimum if exclusive_minimum else result < minimum):
        _config_error(location, "must be {} {}".format("greater than" if exclusive_minimum else "at least", minimum))
    if maximum is not None and result > maximum:
        _config_error(location, "must be at most {}".format(maximum))
    return result


def _local_role_affinity(value, location, default_mode, default_cpus=None):
    value = _mapping(value, location, ("mode", "cpus"))
    mode = _choice(value.get("mode", default_mode), AFFINITY_MODES, location + ".mode")
    cpus = value.get("cpus", None if mode == "none" else default_cpus)
    if isinstance(cpus, str):
        _choice(cpus, ("one-chiplet", "remaining"), location + ".cpus")
    elif cpus is not None:
        cpus = _positive_integer(cpus, location + ".cpus")
    if mode == "none" and cpus is not None:
        _config_error(location + ".cpus", "must be omitted when affinity mode is none")
    if mode != "none" and cpus is None:
        _config_error(location + ".cpus", "is required when affinity mode is not none")
    return {"mode": mode, "cpus": cpus}


def _parse_local_ydb_profile(benchmark, profile_name, value, perf_enabled, perf_frequency):
    location = "{}.{}".format(benchmark.name, profile_name)
    value = _mapping(value, location, ("workload", "geometry", "client", "load", "measurement", "affinity", "timeout"))
    if perf_enabled:
        _config_error(location, "does not support --perf; CPU utilization is collected per process role")

    workload = normalize_workload(value.get("workload"), location + ".workload")
    workload_metadata = workload_definition(workload["type"])

    geometry = _mapping(
        value.get("geometry"),
        location + ".geometry",
        ("preset", "static-nodes", "dynamic-nodes", "max-dynamic-nodes", "disk-size-gb", "storage-groups"),
    )
    preset = _choice(geometry.get("preset", "single"), ("single", "storage", "custom"), location + ".geometry.preset")
    if preset == "single":
        if geometry.get("dynamic-nodes", 1) != 1 or geometry.get("max-dynamic-nodes", 1) != 1:
            _config_error(location + ".geometry", "single preset always uses one dynamic node")
        dynamic_nodes = 1
        max_dynamic_nodes = 1
    elif preset == "storage":
        dynamic_nodes = _positive_integer(geometry.get("dynamic-nodes", 1), location + ".geometry.dynamic-nodes")
        max_dynamic_nodes = _positive_integer(
            geometry.get("max-dynamic-nodes", 8), location + ".geometry.max-dynamic-nodes"
        )
    else:
        if "dynamic-nodes" not in geometry:
            _config_error(location + ".geometry", "custom preset requires dynamic-nodes")
        dynamic_nodes = _positive_integer(geometry["dynamic-nodes"], location + ".geometry.dynamic-nodes")
        max_dynamic_nodes = _positive_integer(
            geometry.get("max-dynamic-nodes", dynamic_nodes), location + ".geometry.max-dynamic-nodes"
        )
    if max_dynamic_nodes < dynamic_nodes:
        _config_error(location + ".geometry.max-dynamic-nodes", "must not be below dynamic-nodes")
    storage_groups = _positive_integer(geometry.get("storage-groups", 1), location + ".geometry.storage-groups")
    static_nodes = _positive_integer(
        geometry.get("static-nodes", 1),
        location + ".geometry.static-nodes",
    )
    geometry_config = {
        "preset": preset,
        "static_nodes": static_nodes,
        "dynamic_nodes": dynamic_nodes,
        "max_dynamic_nodes": max_dynamic_nodes,
        "disk_size_gb": _positive_integer(geometry.get("disk-size-gb", 64), location + ".geometry.disk-size-gb"),
        "storage_groups": storage_groups,
    }

    client = _mapping(value.get("client"), location + ".client", ("threads",))
    client_threads = _positive_integer(
        client.get("threads", workload_metadata.default_client_threads),
        location + ".client.threads",
    )

    load = _mapping(
        value.get("load"),
        location + ".load",
        (
            "mode",
            "parameter",
            "allow-errors",
            "values",
            "start",
            "maximum",
            "multiplier",
            "target-role",
            "plateau-gain-percent",
            "plateau-points",
            "cpu-saturation-percent",
            "search-resolution-percent",
            "slo",
            "search",
            "objective",
        ),
    )
    if "parameter" not in load:
        _config_error(location + ".load", "missing required field: parameter")
    parameter = _choice(
        load["parameter"],
        allowed_load_parameters(workload["type"]),
        location + ".load.parameter",
    )
    legacy_fields = {
        "mode",
        "start",
        "maximum",
        "multiplier",
        "target-role",
        "plateau-gain-percent",
        "plateau-points",
        "cpu-saturation-percent",
        "search-resolution-percent",
        "slo",
    }
    if ("search" in load or "objective" in load) and any(field in load for field in legacy_fields):
        _config_error(location + ".load", "must not mix search/objective with legacy controller fields")

    load_config = {
        "parameter": parameter,
        "allow_errors": _boolean(load.get("allow-errors", False), location + ".load.allow-errors"),
    }
    legacy_slo = _mapping(
        load.get("slo"),
        location + ".load.slo",
        ("percentile", "max-ms", "max-errors", "min-achieved-rate-ratio"),
    )
    if "search" not in load and "objective" not in load:
        load_mode = _choice(
            load.get("mode", "points"),
            ("points", "maximize-throughput", "latency-slo"),
            location + ".load.mode",
        )
        if load_mode == "points":
            if "values" not in load:
                _config_error(location + ".load", "manual load requires values")
            load_config["values"] = list(_positive_integer_list(load["values"], location + ".load.values"))
        else:
            for required in ("start", "maximum"):
                if required not in load:
                    _config_error(location + ".load", "{} mode requires {}".format(load_mode, required))
            load_config["search"] = {
                "start": load["start"],
                "maximum": load["maximum"],
                "multiplier": load.get("multiplier", 2),
                "resolution-percent": load.get("search-resolution-percent", 2),
            }
            load_config["objective"] = {
                "type": load_mode,
                "target-role": load.get("target-role", "static" if preset == "storage" else "dynamic"),
                "plateau-gain-percent": load.get("plateau-gain-percent", 2),
                "plateau-points": load.get("plateau-points", 2),
                "cpu-saturation-percent": load.get("cpu-saturation-percent", 95),
                **legacy_slo,
            }
    else:
        if "search" not in load or "objective" not in load:
            _config_error(location + ".load", "automatic load requires both search and objective")
        if "values" in load:
            _config_error(location + ".load", "must not combine values with search/objective")
        load_config["search"] = load["search"]
        load_config["objective"] = load["objective"]

    if "search" in load_config:
        search = _mapping(
            load_config["search"],
            location + ".load.search",
            ("start", "maximum", "multiplier", "resolution-percent"),
        )
        for required in ("start", "maximum"):
            if required not in search:
                _config_error(location + ".load.search", "missing required field: {}".format(required))
        start = _positive_integer(search["start"], location + ".load.search.start")
        maximum = _positive_integer(search["maximum"], location + ".load.search.maximum")
        if maximum < start:
            _config_error(location + ".load.search.maximum", "must not be below start")
        load_config["search"] = {
            "start": start,
            "maximum": maximum,
            "multiplier": _finite_number(
                search.get("multiplier", 2),
                location + ".load.search.multiplier",
                1,
                exclusive_minimum=True,
            ),
            "resolution_percent": _finite_number(
                search.get("resolution-percent", 2),
                location + ".load.search.resolution-percent",
                0,
                100,
                True,
            ),
        }
        objective = _mapping(
            load_config["objective"],
            location + ".load.objective",
            (
                "type",
                "target-role",
                "plateau-gain-percent",
                "plateau-points",
                "cpu-saturation-percent",
                "percentile",
                "max-ms",
                "max-errors",
                "min-achieved-rate-ratio",
            ),
        )
        if "type" not in objective:
            _config_error(location + ".load.objective", "missing required field: type")
        objective_type = _choice(
            objective["type"],
            ("maximize-throughput", "latency-slo"),
            location + ".load.objective.type",
        )
        parsed_objective = {
            "type": objective_type,
            "cpu_saturation_percent": _finite_number(
                objective.get("cpu-saturation-percent", 95),
                location + ".load.objective.cpu-saturation-percent",
                0,
                100,
                True,
            ),
        }
        if objective_type == "maximize-throughput":
            parsed_objective.update(
                {
                    "target_role": _choice(
                        objective.get("target-role", "static" if preset == "storage" else "dynamic"),
                        ("static", "dynamic", "total"),
                        location + ".load.objective.target-role",
                    ),
                    "plateau_gain_percent": _finite_number(
                        objective.get("plateau-gain-percent", 2),
                        location + ".load.objective.plateau-gain-percent",
                        0,
                    ),
                    "plateau_points": _positive_integer(
                        objective.get("plateau-points", 2), location + ".load.objective.plateau-points"
                    ),
                }
            )
        else:
            if "max-ms" not in objective:
                _config_error(location + ".load.objective", "latency-slo requires max-ms")
            slo_metrics = allowed_slo_metrics(workload["type"])
            percentile = _choice(
                objective.get("percentile", "p99"),
                tuple(slo_metrics),
                location + ".load.objective.percentile",
            )
            parsed_objective.update(
                {
                    "percentile": percentile,
                    "latency_metric": slo_metrics[percentile],
                    "max_ms": _finite_number(objective["max-ms"], location + ".load.objective.max-ms", 0),
                    "max_errors": _nonnegative_integer(
                        objective.get("max-errors", 0), location + ".load.objective.max-errors"
                    ),
                    "min_achieved_rate_ratio": _finite_number(
                        objective.get("min-achieved-rate-ratio", 0.98),
                        location + ".load.objective.min-achieved-rate-ratio",
                        0,
                        1,
                        True,
                    ),
                }
            )
        load_config["objective"] = parsed_objective

    measurement = _mapping(
        value.get("measurement"),
        location + ".measurement",
        ("warmup", "duration", "repetitions", "verification-repetitions"),
    )
    verification_location = location + ".measurement.verification-repetitions"
    verification_repetitions = _nonnegative_integer(
        measurement.get("verification-repetitions", 0),
        verification_location,
    )
    if verification_repetitions > MAX_LOCAL_YDB_VERIFICATION_REPETITIONS:
        _config_error(
            verification_location,
            "must be at most {}".format(MAX_LOCAL_YDB_VERIFICATION_REPETITIONS),
        )
    configured_warmup = (
        _nonnegative_integer(measurement["warmup"], location + ".measurement.warmup")
        if "warmup" in measurement
        else workload_metadata.default_warmup_seconds
    )
    measurement_config = {
        "warmup": configured_warmup,
        "duration": _positive_integer(measurement.get("duration", 30), location + ".measurement.duration"),
        "repetitions": _positive_integer(measurement.get("repetitions", 3), location + ".measurement.repetitions"),
        "verification_repetitions": verification_repetitions,
    }
    validate_workload_profile(workload, load_config, measurement_config, location)

    affinity = _mapping(value.get("affinity"), location + ".affinity", ("ydb-cli", "static-nodes", "dynamic-nodes"))
    affinity_config = {
        "ydb_cli": _local_role_affinity(
            affinity.get("ydb-cli"),
            location + ".affinity.ydb-cli",
            "pack-numa-pack-chiplet-spread-core",
            "one-chiplet",
        ),
        "static_nodes": _local_role_affinity(affinity.get("static-nodes"), location + ".affinity.static-nodes", "none"),
        "dynamic_nodes": _local_role_affinity(
            affinity.get("dynamic-nodes"), location + ".affinity.dynamic-nodes", "none"
        ),
    }

    attempts = len(load_config.get("values", ())) or 64
    measurement_runs = attempts * measurement_config["repetitions"] + measurement_config["verification_repetitions"]
    effective_warmup = workload_effective_warmup_seconds(workload, measurement_config["warmup"])
    computed_timeout = 300 + measurement_runs * (effective_warmup + measurement_config["duration"] + 10)
    timeout_explicit = "timeout" in value
    timeout = _timeout(value.get("timeout", computed_timeout), location + ".timeout")
    return RunConfiguration(
        benchmark=benchmark,
        profile=profile_name,
        threads=(client_threads,),
        parameters={
            "local_ydb": {
                "workload": workload,
                "geometry": geometry_config,
                "client": {"threads": client_threads},
                "load": load_config,
                "measurement": measurement_config,
                "affinity": affinity_config,
            }
        },
        duration_seconds=measurement_config["duration"],
        repetitions=1,
        timeout_seconds=timeout,
        timeout_explicit=timeout_explicit,
        affinity_modes=("roles",),
        background_load_modes=("none",),
        perf_enabled=False,
        perf_frequency=perf_frequency,
    )


def _parse_profile(benchmark, profile_name, value, perf_enabled, perf_frequency):
    if benchmark.profile_kind == "local-ydb":
        return _parse_local_ydb_profile(benchmark, profile_name, value, perf_enabled, perf_frequency)
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
    background_load = _background_load_modes(value.get("background-load", ["none"]), location + ".background-load")
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
