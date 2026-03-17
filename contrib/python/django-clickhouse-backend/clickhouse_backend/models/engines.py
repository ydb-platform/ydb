from collections.abc import Iterable

from django.db.models import Func, Value

__all__ = [
    "Distributed",
    "Engine",
    "BaseMergeTree",
    "MergeTree",
    "ReplacingMergeTree",
    "GraphiteMergeTree",
    "CollapsingMergeTree",
    "VersionedCollapsingMergeTree",
    "SummingMergeTree",
    "AggregatingMergeTree",
    "ReplicatedMergeTree",
    "ReplicatedReplacingMergeTree",
    "ReplicatedGraphiteMergeTree",
    "ReplicatedCollapsingMergeTree",
    "ReplicatedVersionedCollapsingMergeTree",
    "ReplicatedSummingMergeTree",
    "ReplicatedAggregatingMergeTree",
]


def _check_positive(value, name):
    if not isinstance(value, int) and value <= 0:
        raise ValueError(f"{name} must be positive integer.")
    return value


def _check_not_negative(value, name):
    if not isinstance(value, int) and value < 0:
        raise ValueError(f"{name} must not be negative.")
    return value


def _check_bool(value, name):
    if value not in (0, 1):
        raise ValueError(f"{name} must be one of (0, 1, True, False)")
    return int(value)


def _check_str(value, name):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be string")
    return value


def value_if_string(value):
    if isinstance(value, str):
        return Value(value)
    return value


class Engine(Func):
    max_arity = None  # The max number of arguments the function accepts.
    setting_types = {}

    def __init__(self, *expressions, **settings):
        if self.max_arity is not None and len(expressions) > self.max_arity:
            raise TypeError(
                "'%s' takes at most %s %s (%s given)"
                % (
                    self.__class__.__name__,
                    self.max_arity,
                    "argument" if self.max_arity == 1 else "arguments",
                    len(expressions),
                )
            )

        normalized_settings = {}
        for setting, value in settings.items():
            for validate, keys in self.setting_types.items():
                if setting in keys:
                    normalized_settings[setting] = validate(value, setting)
                    break
            else:
                raise TypeError(f"{setting} is not a valid setting.")
        self.settings = normalized_settings
        super().__init__(*expressions)

    @property
    def function(self):
        return self.__class__.__name__

    def deconstruct(self):
        path, args, kwargs = super().deconstruct()
        if path.startswith("clickhouse_backend.models.engines"):
            path = path.replace(
                "clickhouse_backend.models.engines", "clickhouse_backend.models"
            )
        return path, args, kwargs


class BaseMergeTree(Engine):
    # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#settings
    setting_types = {
        _check_positive: [
            "index_granularity",
            "min_index_granularity_bytes",
            "merge_max_block_size",
            "min_bytes_for_wide_part",
            "min_rows_for_wide_part",
            "max_parts_in_total",
            "max_compress_block_size",
            "min_compress_block_size",
            "max_partitions_to_read",
        ],
        _check_not_negative: [
            "index_granularity_bytes",
            "min_merge_bytes_to_use_direct_io",
            "merge_with_ttl_timeout",
            "merge_with_recompression_ttl_timeout",
            "try_fetch_recompressed_part_timeout",
        ],
        _check_bool: [
            "enable_mixed_granularity_parts",
            "use_minimalistic_part_header_in_zookeeper",
            "write_final_mark",
        ],
        _check_str: ["storage_policy"],
    }

    def __init__(
        self,
        *expressions,
        order_by=None,
        partition_by=None,
        primary_key=None,
        **settings,
    ):
        assert order_by is not None or primary_key is not None, (
            "At least one of order_by or primary_key must be provided"
        )
        self.order_by = order_by
        self.primary_key = primary_key
        self.partition_by = partition_by

        for key in ["order_by", "primary_key", "partition_by"]:
            value = getattr(self, key)
            if value is not None:
                if isinstance(value, str) or not isinstance(value, Iterable):
                    value = (value,)
                    setattr(self, key, value)
                elif not isinstance(value, tuple):
                    value = tuple(value)
                    setattr(self, key, value)
                if any(i is None for i in value):
                    raise ValueError(f"None is not allowed in {key}")

        # https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#choosing-a-primary-key-that-differs-from-the-sorting-key
        # primary key expression tuple must be a prefix of the sorting key expression tuple.
        if (
            self.order_by is not None
            and self.primary_key is not None
            and self.order_by[: len(self.primary_key)] != self.primary_key
        ):
            raise ValueError("primary_key must be a prefix of order_by")

        super().__init__(*expressions, **settings)


class MergeTree(BaseMergeTree):
    arity = 0


class ReplacingMergeTree(BaseMergeTree):
    max_arity = 2


class SummingMergeTree(BaseMergeTree):
    pass


class AggregatingMergeTree(BaseMergeTree):
    arity = 0


class CollapsingMergeTree(BaseMergeTree):
    arity = 1


class VersionedCollapsingMergeTree(BaseMergeTree):
    arity = 2


class GraphiteMergeTree(BaseMergeTree):
    def __init__(self, config_section, **extra):
        super().__init__(value_if_string(config_section), **extra)


class ReplicatedMixin:
    def __init__(self, *replicated_parameters, other_parameters=(), **extra):
        """
        https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication#replicatedmergetree-parameters
        https://github.com/ClickHouse/ClickHouse/issues/8675
        https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication#creating-replicated-tables

        replicated_parameters: zoo_path and replica_name. Their default values can be specified in the server
            configuration file, in this case, they both can be omitted.
        other_parameters: Parameters of an engine which is used for creating the replicated version,
            for example, version in ReplacingMergeTree
        """
        if replicated_parameters:
            if len(replicated_parameters) != 2:
                raise TypeError(
                    "'ReplicatedMergeTree' takes 0 or 2 arguments (%s given)"
                    % len(replicated_parameters)
                )
            replicated_parameters = map(value_if_string, replicated_parameters)
        super().__init__(*other_parameters, **extra)
        self.source_expressions = (*replicated_parameters, *self.source_expressions)


class ReplicatedMergeTree(ReplicatedMixin, MergeTree):
    pass


class ReplicatedReplacingMergeTree(ReplicatedMixin, ReplacingMergeTree):
    pass


class ReplicatedSummingMergeTree(ReplicatedMixin, SummingMergeTree):
    pass


class ReplicatedAggregatingMergeTree(ReplicatedMixin, AggregatingMergeTree):
    pass


class ReplicatedCollapsingMergeTree(ReplicatedMixin, CollapsingMergeTree):
    pass


class ReplicatedVersionedCollapsingMergeTree(
    ReplicatedMixin, VersionedCollapsingMergeTree
):
    pass


class ReplicatedGraphiteMergeTree(ReplicatedMixin, GraphiteMergeTree):
    pass


class Distributed(Engine):
    # https://clickhouse.com/docs/en/engines/table-engines/special/distributed#distributed-settings
    setting_types = {
        _check_positive: ["monitor_sleep_time_ms", "monitor_max_sleep_time_ms"],
        _check_not_negative: [
            "bytes_to_throw_insert",
            "bytes_to_delay_insert",
            "max_delay_to_insert",
        ],
        _check_bool: [
            "fsync_after_insert",
            "fsync_directories",
            "monitor_batch_inserts",
            "monitor_split_batch_on_failure",
        ],
    }

    def __init__(self, *expressions, **settings):
        if len(expressions) < 3:
            raise TypeError(
                "'%s' takes at least 3 arguments (cluster, database, table)"
                % self.__class__.__name__
            )
        if len(expressions) > 5:
            raise TypeError(
                "'%s' takes at most 5 arguments (cluster, database, table[, sharding_key[, policy_name]])"
                % self.__class__.__name__
            )

        self.cluster, self.database, self.table = expressions[:3]
        if len(expressions) == 5:
            expressions = (
                *map(value_if_string, expressions[:3]),
                expressions[3],
                value_if_string(expressions[4]),
            )
        else:
            expressions = (*map(value_if_string, expressions[:3]), *expressions[3:])
        super().__init__(*expressions, **settings)
