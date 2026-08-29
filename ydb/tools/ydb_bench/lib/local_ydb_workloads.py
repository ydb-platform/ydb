"""Declarative local-YDB workload catalog and YDB CLI command builders."""

import re
from dataclasses import dataclass
from types import MappingProxyType

from ydb.tools.ydb_bench.lib.common import BenchmarkError


@dataclass(frozen=True)
class WorkloadCli:
    executable: object
    endpoint: str
    database: str


@dataclass(frozen=True)
class WorkloadOption:
    name: str
    default: object
    kind: str = "integer"
    operation_defaults: tuple = ()
    allow_zero: bool = False
    maximum: object = None
    choices: tuple = ()
    schema_minimum: object = None
    pattern: str | None = None
    allow_empty: bool = False

    def default_for(self, operation):
        return dict(self.operation_defaults).get(operation, self.default)

    @property
    def minimum(self):
        if self.kind != "integer":
            return None
        if self.schema_minimum is not None:
            return self.schema_minimum
        return 0 if self.allow_zero else 1

    def config_schema(self):
        if self.kind == "integer":
            if self.choices:
                return {"enum": list(self.choices)}
            schema = {"type": "integer", "minimum": self.minimum}
            if self.maximum is not None:
                schema["maximum"] = self.maximum
            return schema
        if self.kind in ("string", "duration"):
            schema = {"type": "string"}
            if not self.allow_empty:
                schema["minLength"] = 1
            if self.choices:
                schema["enum"] = list(self.choices)
            if self.pattern is not None:
                schema["pattern"] = self.pattern
            return schema
        if self.kind == "boolean":
            schema = {"type": "boolean"}
            if self.choices:
                schema["enum"] = list(self.choices)
            return schema
        raise ValueError("unknown workload option kind: {}".format(self.kind))


@dataclass(frozen=True)
class WorkloadDefinition:
    name: str
    default_operation: str
    operations: tuple
    load_parameters: tuple
    options: tuple
    uses_path: bool
    table_name: str | None
    init_builder: object
    run_builder: object
    options_validator: object


def _config_error(location, message):
    raise BenchmarkError("invalid benchmark config at {}: {}".format(location, message))


def _mapping(value, location, allowed=()):
    if value is None:
        return {}
    if not isinstance(value, dict):
        _config_error(location, "must be a mapping")
    unknown = sorted((item for item in value if item not in allowed), key=str)
    if unknown:
        _config_error(location, "contains unknown fields: {}".format(", ".join(map(str, unknown))))
    return value


def _workload_base(cli, definition, path):
    command = [
        cli.executable,
        "--endpoint",
        cli.endpoint,
        "--database",
        cli.database,
        "workload",
        definition.name,
    ]
    if definition.uses_path:
        command += ["--path", path]
    return command


def _kv_init_builder(cli, definition, path, workload):
    options = workload["options"]
    return _workload_base(cli, definition, path) + [
        "init",
        "--init-upserts",
        options["init-upserts"],
        "--min-partitions",
        options["min-partitions"],
        "--max-partitions",
        options["max-partitions"],
        "--partition-size",
        options["partition-size-mb"],
        "--max-first-key",
        options["max-first-key"],
        "--len",
        options["value-size"],
        "--cols",
        options["columns"],
        "--int-cols",
        1,
        "--key-cols",
        1,
        "--rows",
        options["rows-per-query"],
    ]


def _kv_run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads):
    options = workload["options"]
    threads = load if load_parameter == "threads" else client_threads
    command = _workload_base(cli, definition, path) + [
        "run",
        workload["operation"],
        "--seconds",
        seconds,
        "--threads",
        threads,
        "--quiet",
        "--max-first-key",
        options["max-first-key"],
        "--int-cols",
        1,
        "--key-cols",
        1,
        "--cols",
        options["columns"],
    ]
    if workload["operation"] != "mixed":
        command += ["--rows", options["rows-per-query"]]
    if workload["operation"] in ("upsert", "mixed"):
        command += ["--len", options["value-size"]]
    if load_parameter == "rate":
        command += ["--rate", load]
    return command


def _stock_init_builder(cli, definition, path, workload):
    del path
    options = workload["options"]
    return _workload_base(cli, definition, None) + [
        "init",
        "--products",
        options["products"],
        "--quantity",
        options["quantity"],
        "--orders",
        options["orders"],
        "--min-partitions",
        options["min-partitions"],
        "--auto-partition",
        options["auto-partition"],
    ]


def _stock_run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads):
    del path
    options = workload["options"]
    threads = load if load_parameter == "threads" else client_threads
    command = _workload_base(cli, definition, None) + [
        "run",
        workload["operation"],
        "--seconds",
        seconds,
        "--threads",
        threads,
        "--quiet",
    ]
    if workload["operation"] in ("user-hist", "rand-user-hist"):
        command += ["--limit", options["limit"]]
    else:
        command += ["--products", options["products"]]
    if load_parameter == "rate":
        command += ["--rate", load]
    return command


def _validate_kv_options(options, location):
    if options["max-partitions"] < options["min-partitions"]:
        _config_error(location + ".max-partitions", "must not be below min-partitions")
    if options["columns"] < 2:
        _config_error(location + ".columns", "must be at least 2")


def _validate_stock_options(options, location):
    del options, location


def _validate_option_value(option, value, location):
    if option.kind == "integer":
        minimum = 0 if option.allow_zero else 1
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            requirement = "non-negative" if option.allow_zero else "positive"
            _config_error(location, "must be a {} integer".format(requirement))
        if option.maximum is not None and value > option.maximum:
            _config_error(location, "must not exceed {}".format(option.maximum))
    elif option.kind in ("string", "duration"):
        if not isinstance(value, str):
            _config_error(location, "must be a string")
        if not option.allow_empty and not value:
            _config_error(location, "must not be empty")
        if option.pattern is not None and re.search(option.pattern, value) is None:
            _config_error(location, "must match pattern {}".format(option.pattern))
    elif option.kind == "boolean":
        if not isinstance(value, bool):
            _config_error(location, "must be a boolean")
    else:
        raise ValueError("unknown workload option kind: {}".format(option.kind))
    if option.choices and value not in option.choices:
        if option.kind == "integer":
            _config_error(location, "must be {}".format(" or ".join(map(str, option.choices))))
        _config_error(location, "must be one of {}".format(", ".join(map(str, option.choices))))


def _validate_catalog(definitions):
    names = [definition.name for definition in definitions]
    if len(names) != len(set(names)):
        raise ValueError("local YDB workload names must be unique")
    option_schemas = {}
    for definition in definitions:
        if len(definition.operations) != len(set(definition.operations)):
            raise ValueError("operations must be unique for {}".format(definition.name))
        if len(definition.load_parameters) != len(set(definition.load_parameters)):
            raise ValueError("load parameters must be unique for {}".format(definition.name))
        if definition.default_operation not in definition.operations:
            raise ValueError("default operation must belong to {}".format(definition.name))
        option_names = [option.name for option in definition.options]
        if len(option_names) != len(set(option_names)):
            raise ValueError("option names must be unique for {}".format(definition.name))
        for option in definition.options:
            if option.kind not in ("integer", "string", "boolean", "duration"):
                raise ValueError("unknown workload option kind: {}".format(option.kind))
            if option.kind != "integer" and (
                option.allow_zero or option.maximum is not None or option.schema_minimum is not None
            ):
                raise ValueError("integer-only metadata is set for {}.{}".format(definition.name, option.name))
            if option.kind not in ("string", "duration") and (option.pattern is not None or option.allow_empty):
                raise ValueError("string-only metadata is set for {}.{}".format(definition.name, option.name))
            if option.kind == "duration" and option.pattern is None:
                raise ValueError("duration option {} requires a pattern".format(option.name))
            if option.pattern is not None:
                if "(?" in option.pattern or "\\" in option.pattern:
                    raise ValueError("pattern for {}.{} is not portable".format(definition.name, option.name))
                try:
                    re.compile(option.pattern)
                except re.error as error:
                    raise ValueError("invalid pattern for {}.{}".format(definition.name, option.name)) from error
            try:
                choices_are_unique = len(option.choices) == len(set(option.choices))
            except TypeError as error:
                raise ValueError("choices must be scalar for {}.{}".format(definition.name, option.name)) from error
            if not choices_are_unique:
                raise ValueError("choices must be unique for {}.{}".format(definition.name, option.name))
            operation_defaults = [operation for operation, _ in option.operation_defaults]
            if len(operation_defaults) != len(set(operation_defaults)):
                raise ValueError("operation defaults must be unique for {}.{}".format(definition.name, option.name))
            for operation, value in ((None, option.default),) + option.operation_defaults:
                if operation is not None and operation not in definition.operations:
                    raise ValueError("option default refers to unknown operation {}".format(operation))
                try:
                    _validate_option_value(option, value, "{}.{}".format(definition.name, option.name))
                except BenchmarkError as error:
                    raise ValueError(
                        "invalid default for {}.{}: {}".format(definition.name, option.name, error)
                    ) from error
            for choice in option.choices:
                try:
                    _validate_option_value(option, choice, "{}.{}".format(definition.name, option.name))
                except BenchmarkError as error:
                    raise ValueError(
                        "invalid choice for {}.{}: {}".format(definition.name, option.name, error)
                    ) from error
            schema = option.config_schema()
            if option.name in option_schemas and option_schemas[option.name] != schema:
                raise ValueError("option {} has incompatible schemas across workloads".format(option.name))
            option_schemas[option.name] = schema


_DEFINITIONS = (
    WorkloadDefinition(
        name="kv",
        default_operation="upsert",
        operations=("upsert", "select", "read-rows", "mixed"),
        load_parameters=("rate", "threads"),
        options=(
            WorkloadOption("min-partitions", 40),
            WorkloadOption("max-partitions", 1000),
            WorkloadOption("partition-size-mb", 2000),
            WorkloadOption("init-upserts", 1000, operation_defaults=(("upsert", 0),), allow_zero=True),
            WorkloadOption("max-first-key", 65536),
            WorkloadOption("value-size", 64),
            WorkloadOption("columns", 2, schema_minimum=2),
            WorkloadOption("rows-per-query", 1),
        ),
        uses_path=True,
        table_name=None,
        init_builder=_kv_init_builder,
        run_builder=_kv_run_builder,
        options_validator=_validate_kv_options,
    ),
    WorkloadDefinition(
        name="stock",
        default_operation="put-rand-order",
        operations=("user-hist", "rand-user-hist", "add-rand-order", "put-rand-order", "put-same-order"),
        load_parameters=("rate", "threads"),
        options=(
            WorkloadOption("min-partitions", 40),
            WorkloadOption("products", 100, maximum=500000),
            WorkloadOption("quantity", 1000),
            WorkloadOption("orders", 100, allow_zero=True),
            WorkloadOption("auto-partition", 1, allow_zero=True, choices=(0, 1)),
            WorkloadOption("limit", 10),
        ),
        uses_path=False,
        table_name="stock",
        init_builder=_stock_init_builder,
        run_builder=_stock_run_builder,
        options_validator=_validate_stock_options,
    ),
)
_validate_catalog(_DEFINITIONS)
_WORKLOADS = MappingProxyType({definition.name: definition for definition in _DEFINITIONS})


def _definition(workload_type):
    try:
        return _WORKLOADS[workload_type]
    except (KeyError, TypeError) as error:
        raise BenchmarkError("unknown local YDB workload: {}".format(workload_type)) from error


def normalize_workload(raw, location):
    """Validate and fill defaults for one workload configuration mapping."""

    workload = _mapping(raw, location, ("type", "operation", "options"))
    for required in ("type", "operation"):
        if required not in workload:
            _config_error(location, "missing required field: {}".format(required))

    workload_type = workload["type"]
    if not isinstance(workload_type, str) or workload_type not in _WORKLOADS:
        _config_error(location + ".type", "must be one of {}".format(", ".join(_WORKLOADS)))
    definition = _WORKLOADS[workload_type]
    operation = workload["operation"]
    if operation not in definition.operations:
        _config_error(location + ".operation", "must be one of {}".format(", ".join(definition.operations)))

    options_location = location + ".options"
    raw_options = _mapping(
        workload.get("options"), options_location, tuple(option.name for option in definition.options)
    )
    options = {}
    for option in definition.options:
        value = raw_options.get(option.name, option.default_for(operation))
        _validate_option_value(option, value, options_location + "." + option.name)
        options[option.name] = value
    definition.options_validator(options, options_location)
    return {"type": definition.name, "operation": operation, "options": options}


def workload_config_schema():
    """Return the structurally compatible config-schema fragment for workloads."""

    option_schemas = {}
    for definition in _DEFINITIONS:
        for option in definition.options:
            option_schemas[option.name] = option.config_schema()
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["type", "operation"],
        "properties": {
            "type": {"enum": [definition.name for definition in _DEFINITIONS]},
            "operation": {"enum": [operation for definition in _DEFINITIONS for operation in definition.operations]},
            "options": {
                "type": "object",
                "additionalProperties": False,
                "properties": option_schemas,
            },
        },
    }


def web_workload_catalog():
    """Return a stable JSON-compatible workload description for the web builder."""

    return [
        {
            "type": definition.name,
            "default_operation": definition.default_operation,
            "operations": list(definition.operations),
            "load_parameters": list(definition.load_parameters),
            "options": [
                {
                    "name": option.name,
                    "kind": option.kind,
                    "default": option.default,
                    "operation_defaults": dict(option.operation_defaults),
                    "allow_zero": option.allow_zero,
                    "minimum": option.minimum,
                    "maximum": option.maximum,
                    "choices": list(option.choices),
                    "pattern": option.pattern,
                    "allow_empty": option.allow_empty,
                    "schema": option.config_schema(),
                }
                for option in definition.options
            ],
        }
        for definition in _DEFINITIONS
    ]


def allowed_load_parameters(workload_type):
    return _definition(workload_type).load_parameters


def all_load_parameters():
    return tuple(dict.fromkeys(parameter for definition in _DEFINITIONS for parameter in definition.load_parameters))


def build_init_argv(cli, path, workload):
    definition = _definition(workload["type"])
    return definition.init_builder(cli, definition, path, workload)


def build_run_argv(cli, path, workload, load_parameter, load, seconds, client_threads):
    definition = _definition(workload["type"])
    if load_parameter not in definition.load_parameters:
        raise BenchmarkError(
            "local YDB workload {} does not support load parameter {}".format(definition.name, load_parameter)
        )
    return definition.run_builder(cli, definition, path, workload, load_parameter, load, seconds, client_threads)


def build_clean_argv(cli, path, workload_type):
    definition = _definition(workload_type)
    return _workload_base(cli, definition, path) + ["clean"]


def workload_table_path(workload_type, path):
    definition = _definition(workload_type)
    return definition.table_name if definition.table_name is not None else path
