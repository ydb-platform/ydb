import logging
from collections.abc import Sequence
from typing import Any

from sqlalchemy import Column
from sqlalchemy.exc import ArgumentError, SQLAlchemyError
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.visitors import Visitable

from clickhouse_connect.cc_sqlalchemy.sql.sqlparse import split_top_level, walk_sql
from clickhouse_connect.driver.binding import format_str, quote_identifier
from clickhouse_connect.driver.parser import parse_callable

logger = logging.getLogger(__name__)

engine_map: dict[str, type["TableEngine"]] = {}
EngineExpr = str | TextClause | Column
EngineParam = EngineExpr | Sequence[EngineExpr] | None
ENGINE_CLAUSES = ("ORDER BY", "PARTITION BY", "PRIMARY KEY", "SAMPLE BY", "TTL", "SETTINGS")


def _render_engine_expr(value: EngineExpr) -> str:
    if isinstance(value, TextClause):
        return value.text
    if isinstance(value, Column):
        return quote_identifier(value.name)
    return value


def _render_setting_value(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return format_str(str(value))


def tuple_expr(expr_name, value: EngineParam):
    """
    Create a table parameter with a tuple or list correctly formatted
    :param expr_name: parameter
    :param value: string or tuple of strings to format
    :return: formatted parameter string
    """
    if value is None:
        return ""
    v = f"{expr_name.strip()}"
    if isinstance(value, (tuple, list)):
        return f" {v} ({','.join(_render_engine_expr(item) for item in value)})"
    return f"{v} {_render_engine_expr(value)}"


def repr_engine_value(value: Any) -> str:
    if isinstance(value, str):
        return repr(value)
    if isinstance(value, TextClause):
        return f"sa.text({value.text!r})"
    if isinstance(value, Column):
        return repr(value.name)
    if isinstance(value, tuple):
        items = ", ".join(repr_engine_value(item) for item in value)
        if len(value) == 1:
            items += ","
        return f"({items})"
    if isinstance(value, list):
        return f"[{', '.join(repr_engine_value(item) for item in value)}]"
    return repr(value)


class TableEngine(SchemaEventTarget, Visitable):
    """
    SqlAlchemy Schema element to support ClickHouse table engines.  At the moment provides no real
    functionality other than the CREATE TABLE argument string
    """

    arg_names = ()
    quoted_args = set()
    optional_args = set()
    eng_params = ()

    def __init_subclass__(cls, **kwargs):
        engine_map[cls.__name__] = cls

    def __init__(self, kwargs):
        Visitable.__init__(self)
        self.name = self.__class__.__name__
        te_name = f"{self.name} Table Engine"
        self._orig_kwargs = kwargs.copy()
        engine_args = []
        for arg_name in self.arg_names:
            v = kwargs.pop(arg_name, None)
            if v is None:
                if arg_name in self.optional_args:
                    continue
                raise ValueError(f"Required engine parameter {arg_name} not provided for {te_name}")
            if arg_name in self.quoted_args:
                engine_args.append(f"'{v}'")
            else:
                engine_args.append(v)
        if engine_args:
            self.arg_str = f"({', '.join(engine_args)})"
        params = []
        for param_name in self.eng_params:
            v = kwargs.pop(param_name, None)
            if v is not None:
                params.append(tuple_expr(param_name.upper().replace("_", " "), v))
        settings = kwargs.pop("settings", None)
        self.settings = settings or {}

        self.full_engine = "Engine " + self.name
        if engine_args:
            self.full_engine += f"({', '.join(engine_args)})"
        if params:
            self.full_engine += " " + " ".join(params)
        if self.settings:
            settings_expr = ", ".join(f"{k} = {_render_setting_value(v)}" for k, v in self.settings.items())
            self.full_engine += f" SETTINGS {settings_expr}"

    def __repr__(self):
        """Produce Python code representation of the engine for Alembic autogeneration."""
        args = []
        for k, v in self._orig_kwargs.items():
            if k in {"self", "__class__"}:
                continue
            if v is None:
                continue
            args.append(f"{k}={repr_engine_value(v)}")
        return f"{self.name}({', '.join(args)})"

    def compile(self):
        return self.full_engine

    def check_primary_keys(self, primary_keys: Sequence):
        raise SQLAlchemyError(f"Table Engine {self.name} does not support primary keys")

    def _set_parent(self, parent, **_kwargs):
        parent.engine = self
        if parent.kwargs.get("clickhouse_engine") is None and parent.kwargs.get("clickhousedb_engine") is None:
            parent.kwargs["clickhouse_engine"] = self


class Memory(TableEngine):
    pass


class Log(TableEngine):
    pass


class StripeLog(TableEngine):
    pass


class TinyLog(TableEngine):
    pass


class Null(TableEngine):
    pass


class Set(TableEngine):
    pass


class Dictionary(TableEngine):
    arg_names = ["dictionary"]

    def __init__(self, dictionary: str = None):
        super().__init__(locals())


class Merge(TableEngine):
    arg_names = ["db_name, tables_regexp"]

    def __init__(self, db_name: str = None, tables_regexp: str = None):
        super().__init__(locals())


class File(TableEngine):
    arg_names = ["fmt"]

    def __init__(self, fmt: str = None):
        super().__init__(locals())


class Distributed(TableEngine):
    arg_names = ["cluster", "database", "table", "sharding_key", "policy_name"]
    optional_args = {"sharding_key", "policy_name"}

    def __init__(self, cluster: str = None, database: str = None, table=None, sharding_key: str = None, policy_name: str = None):
        super().__init__(locals())


class MergeTree(TableEngine):
    eng_params = ["order_by", "partition_by", "primary_key", "sample_by", "ttl"]

    def __init__(
        self,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class SharedMergeTree(MergeTree):
    pass


class SummingMergeTree(MergeTree):
    pass


class AggregatingMergeTree(MergeTree):
    pass


class ReplacingMergeTree(TableEngine):
    arg_names = ["version", "is_deleted"]
    optional_args = set(arg_names)
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        ver: str = None,
        version: str = None,
        is_deleted: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        kwargs = {
            "version": version or ver,
            "is_deleted": is_deleted,
            "order_by": order_by,
            "primary_key": primary_key,
            "partition_by": partition_by,
            "sample_by": sample_by,
            "ttl": ttl,
            "settings": settings,
        }
        super().__init__(kwargs)


class CollapsingMergeTree(TableEngine):
    arg_names = ["sign"]
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        sign: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class VersionedCollapsingMergeTree(TableEngine):
    arg_names = ["sign", "version"]
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        sign: str = None,
        version: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class GraphiteMergeTree(TableEngine):
    arg_names = ["config_section"]
    quoted_args = set(arg_names)
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        config_section: str = None,
        version: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class ReplicatedMergeTree(TableEngine):
    arg_names = ["zk_path", "replica"]
    quoted_args = set(arg_names)
    optional_args = quoted_args
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        zk_path: str = None,
        replica: str = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class ReplicatedAggregatingMergeTree(ReplicatedMergeTree):
    pass


class ReplicatedSummingMergeTree(ReplicatedMergeTree):
    pass


class ReplicatedReplacingMergeTree(TableEngine):
    arg_names = ["zk_path", "replica", "ver"]
    quoted_args = {"zk_path", "replica"}
    optional_args = {"zk_path", "replica", "ver"}
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        ver: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        zk_path: str = None,
        replica: str = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class ReplicatedCollapsingMergeTree(TableEngine):
    arg_names = ["zk_path", "replica", "sign"]
    quoted_args = {"zk_path", "replica"}
    optional_args = {"zk_path", "replica"}
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        sign: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        zk_path: str = None,
        replica: str = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class ReplicatedVersionedCollapsingMergeTree(TableEngine):
    arg_names = ["zk_path", "replica", "sign", "version"]
    quoted_args = {"zk_path", "replica"}
    optional_args = {"zk_path", "replica"}
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        sign: str = None,
        version: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        zk_path: str = None,
        replica: str = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class ReplicatedGraphiteMergeTree(TableEngine):
    arg_names = ["zk_path", "replica", "config_section"]
    quoted_args = {"zk_path", "replica", "config_section"}
    optional_args = {"zk_path", "replica"}
    eng_params = MergeTree.eng_params

    def __init__(
        self,
        config_section: str = None,
        order_by: EngineParam = None,
        primary_key: EngineParam = None,
        partition_by: EngineParam = None,
        sample_by: EngineParam = None,
        zk_path: str = None,
        replica: str = None,
        ttl: EngineExpr | None = None,
        settings: dict[str, Any] | None = None,
    ):
        if order_by is None and primary_key is None:
            raise ArgumentError(None, "Either PRIMARY KEY or ORDER BY must be specified")
        super().__init__(locals())


class SharedReplacingMergeTree(ReplacingMergeTree):
    pass


class SharedAggregatingMergeTree(AggregatingMergeTree):
    pass


class SharedSummingMergeTree(SummingMergeTree):
    pass


class SharedVersionedCollapsingMergeTree(VersionedCollapsingMergeTree):
    pass


class SharedGraphiteMergeTree(GraphiteMergeTree):
    pass


def _strip_string_quotes(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 1 and value[0] == value[-1] == "'":
        return value[1:-1]
    return value


def _parse_positional_engine_args(full_engine: str, engine_cls: type["TableEngine"]) -> dict[str, Any]:
    if not engine_cls.arg_names:
        return {}
    _, arg_values, _ = parse_callable(full_engine)
    return {arg_name: _strip_string_quotes(arg_value) for arg_name, arg_value in zip(engine_cls.arg_names, arg_values) if arg_value != ""}


def _find_clause_markers(sql: str) -> list[tuple[int, str]]:
    markers = []
    upper_sql = sql.upper()
    for i, _char, depth in walk_sql(sql):
        if depth != 0 or (i > 0 and not sql[i - 1].isspace()):
            continue
        for clause in ENGINE_CLAUSES:
            if upper_sql.startswith(clause, i):
                markers.append((i, clause))
                break
    return markers


_CH_STRING_ESCAPES = {"\\": "\\", "'": "'", '"': '"', "`": "`", "n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "0": "\0"}


def _decode_ch_string_literal(literal: str) -> str:
    inner = literal[1:-1].replace("''", "'")
    out: list[str] = []
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch == "\\" and i + 1 < len(inner):
            out.append(_CH_STRING_ESCAPES.get(inner[i + 1], inner[i + 1]))
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _parse_settings_clause(raw_settings: str) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    for pair in split_top_level(raw_settings):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            settings[key] = _decode_ch_string_literal(value)
            continue
        try:
            settings[key] = int(value)
            continue
        except ValueError:
            pass
        try:
            settings[key] = float(value)
            continue
        except ValueError:
            settings[key] = value
    return settings


def _parse_keyword_engine_clauses(clause_sql: str) -> dict[str, Any]:
    params: dict[str, Any] = {}
    markers = _find_clause_markers(clause_sql)
    for index, (start, clause) in enumerate(markers):
        value_start = start + len(clause)
        value_end = markers[index + 1][0] if index + 1 < len(markers) else len(clause_sql)
        value = clause_sql[value_start:value_end].strip()
        if not value:
            continue
        if clause == "SETTINGS":
            settings = _parse_settings_clause(value)
            if settings:
                params["settings"] = settings
            continue
        params[clause.lower().replace(" ", "_")] = value
    return params


def _parse_engine_params(full_engine: str, engine_cls: type["TableEngine"]) -> dict[str, Any]:
    """Extract engine parameters from a full_engine expression for repr().

    Parses both positional constructor args (e.g. the ``version`` in
    ``ReplacingMergeTree(version)``) and keyword clauses (``ORDER BY``,
    ``PARTITION BY``, etc.) so that reflected engines round-trip through
    ``repr()`` correctly.
    """
    params = _parse_positional_engine_args(full_engine, engine_cls)
    _, _, clause_sql = parse_callable(full_engine)
    params.update(_parse_keyword_engine_clauses(clause_sql))
    return params


def build_engine(full_engine: str) -> TableEngine | None:
    """
    Factory function to create TableEngine class from ClickHouse full_engine expression.

    ClickHouse Cloud transparently rewrites user-facing engines (e.g. MergeTree)
    to Shared* variants (e.g. SharedMergeTree) with Cloud-internal positional
    args for replication paths. When reflecting, we map back to the base engine
    class and drop those args so that repr() produces valid user-level DDL.
    """
    if not full_engine:
        return None
    name, _, _ = parse_callable(full_engine)
    try:
        engine_cls = engine_map[name]
    except KeyError:
        if not name.startswith("System"):
            logger.warning("Engine %s not found", name)
        return None

    # Map Shared* back to the base engine and discard Cloud-internal positional args.
    # Cloud prepends replication path args (zk_path, replica) before the base engine's
    # own positional args, e.g. SharedReplacingMergeTree('/path', '{replica}', ver).
    base_name = name
    if name.startswith("Shared"):
        base_name = name[len("Shared") :]
        base_cls = engine_map.get(base_name)
        if base_cls is not None:
            engine_cls = base_cls
            _, all_args, clause_tail = parse_callable(full_engine)
            # Cloud prepends exactly 2 args (zk_path, replica) — skip them
            base_args = all_args[2:] if len(all_args) > 2 else ()
            args_str = f"({','.join(str(a) for a in base_args)})" if base_args else ""
            full_engine = base_name + args_str + (" " + clause_tail if clause_tail.strip() else "")

    engine = engine_cls.__new__(engine_cls)
    engine.name = base_name
    engine.full_engine = full_engine
    engine._orig_kwargs = _parse_engine_params(full_engine, engine_cls)
    engine.settings = dict(engine._orig_kwargs.get("settings") or {})
    return engine


__all__ = sorted(engine_map) + ["build_engine", "engine_map"]
