import collections
import sqlalchemy as sa
import ydb
from ydb_dbapi import NotSupportedError

from sqlalchemy.exc import CompileError
from sqlalchemy.sql import ddl
from sqlalchemy.sql.compiler import (
    DDLCompiler,
    IdentifierPreparer,
    StrSQLCompiler,
    StrSQLTypeCompiler,
    selectable,
)
from sqlalchemy.sql.type_api import to_instance
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Sequence,
    Optional,
    Tuple,
    Type,
    Union,
)

try:
    from sqlalchemy.types import _Binary as _BinaryType
except ImportError:
    # For older sqlalchemy versions
    from sqlalchemy.sql.sqltypes import _Binary as _BinaryType


from .. import types


OLD_SA = sa.__version__ < "2."
if OLD_SA:
    from sqlalchemy import bindparam as _bindparam
    from sqlalchemy import cast as _cast
else:
    from sqlalchemy import BindParameter as _bindparam
    from sqlalchemy import Cast as _cast


COMPOUND_KEYWORDS = {
    selectable.CompoundSelect.UNION: "UNION ALL",
    selectable.CompoundSelect.UNION_ALL: "UNION ALL",
    selectable.CompoundSelect.EXCEPT: "EXCEPT",
    selectable.CompoundSelect.EXCEPT_ALL: "EXCEPT ALL",
    selectable.CompoundSelect.INTERSECT: "INTERSECT",
    selectable.CompoundSelect.INTERSECT_ALL: "INTERSECT ALL",
}


ESCAPE_RULES = [
    ("\\", "\\\\"),  # Must be first to avoid double escaping
    ("'", "\\'"),
    ("\0", "\\0"),
    ("\b", "\\b"),
    ("\f", "\\f"),
    ("\r", "\\r"),
    ("\n", "\\n"),
    ("\t", "\\t"),
    ("%", "%%"),
]


class BaseYqlTypeCompiler(StrSQLTypeCompiler):
    def visit_JSON(self, type_: Union[sa.JSON, types.YqlJSON], **kw):
        return "JSON"

    def visit_CHAR(self, type_: sa.CHAR, **kw):
        return "UTF8"

    def visit_VARCHAR(self, type_: sa.VARCHAR, **kw):
        return "UTF8"

    def visit_unicode(self, type_: sa.Unicode, **kw):
        return "UTF8"

    def visit_NVARCHAR(self, type_: sa.NVARCHAR, **kw):
        return "UTF8"

    def visit_TEXT(self, type_: sa.TEXT, **kw):
        return "UTF8"

    def visit_FLOAT(self, type_: sa.FLOAT, **kw):
        return "FLOAT"

    def visit_BOOLEAN(self, type_: sa.BOOLEAN, **kw):
        return "BOOL"

    def visit_uint64(self, type_: types.UInt64, **kw):
        return "UInt64"

    def visit_uint32(self, type_: types.UInt32, **kw):
        return "UInt32"

    def visit_uint16(self, type_: types.UInt16, **kw):
        return "UInt16"

    def visit_uint8(self, type_: types.UInt8, **kw):
        return "UInt8"

    def visit_int64(self, type_: types.Int64, **kw):
        return "Int64"

    def visit_int32(self, type_: types.Int32, **kw):
        return "Int32"

    def visit_int16(self, type_: types.Int16, **kw):
        return "Int16"

    def visit_int8(self, type_: types.Int8, **kw):
        return "Int8"

    def visit_INTEGER(self, type_: sa.INTEGER, **kw):
        return "Int64"

    def visit_NUMERIC(self, type_: sa.Numeric, **kw):
        return f"Decimal({type_.precision}, {type_.scale})"

    def visit_DECIMAL(self, type_: sa.DECIMAL, **kw):
        precision = getattr(type_, "precision", None) or 22
        scale = getattr(type_, "scale", None) or 9
        return f"Decimal({precision}, {scale})"

    def visit_BINARY(self, type_: sa.BINARY, **kw):
        return "String"

    def visit_BLOB(self, type_: sa.BLOB, **kw):
        return "String"

    def visit_datetime(self, type_: sa.TIMESTAMP, **kw):
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_DATETIME(self, type_: sa.DATETIME, **kw):
        return "DateTime"

    def visit_TIMESTAMP(self, type_: sa.TIMESTAMP, **kw):
        return "Timestamp"

    def visit_date32(self, type_: types.YqlDate32, **kw):
        return "Date32"

    def visit_timestamp64(self, type_: types.YqlTimestamp64, **kw):
        return "Timestamp64"

    def visit_datetime64(self, type_: types.YqlDateTime64, **kw):
        return "DateTime64"

    def visit_list_type(self, type_: types.ListType, **kw):
        inner = self.process(type_.item_type, **kw)
        return f"List<{inner}>"

    def visit_ARRAY(self, type_: sa.ARRAY, **kw):
        inner = self.process(type_.item_type, **kw)
        return f"List<{inner}>"

    def visit_optional(self, type_: types.Optional, **kw):
        el = to_instance(type_.element_type)
        inner = self.process(el, **kw)
        return f"Optional<{inner}>"

    def visit_struct_type(self, type_: types.StructType, **kw):
        rendered_types = []
        for field, field_type in type_.fields_types.items():
            type_str = self.process(field_type, **kw)
            rendered_types.append(f"{field}:{type_str}")
        return f"Struct<{','.join(rendered_types)}>"

    def get_ydb_type(
        self, type_: sa.types.TypeEngine, is_optional: bool
    ) -> Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]:
        if isinstance(type_, sa.TypeDecorator):
            type_ = type_.impl

        if isinstance(type_, (sa.Text, sa.String)):
            ydb_type = ydb.PrimitiveType.Utf8

        elif isinstance(type_, types.Optional):
            inner = to_instance(type_.element_type)
            return self.get_ydb_type(inner, is_optional=True)

        # Integers
        elif isinstance(type_, types.UInt64):
            ydb_type = ydb.PrimitiveType.Uint64
        elif isinstance(type_, types.UInt32):
            ydb_type = ydb.PrimitiveType.Uint32
        elif isinstance(type_, types.UInt16):
            ydb_type = ydb.PrimitiveType.Uint16
        elif isinstance(type_, types.UInt8):
            ydb_type = ydb.PrimitiveType.Uint8
        elif isinstance(type_, types.Int64):
            ydb_type = ydb.PrimitiveType.Int64
        elif isinstance(type_, types.Int32):
            ydb_type = ydb.PrimitiveType.Int32
        elif isinstance(type_, types.Int16):
            ydb_type = ydb.PrimitiveType.Int16
        elif isinstance(type_, types.Int8):
            ydb_type = ydb.PrimitiveType.Int8
        elif isinstance(type_, sa.Integer):
            ydb_type = ydb.PrimitiveType.Int64
        # Integers

        # Json
        elif isinstance(type_, sa.JSON):
            ydb_type = ydb.PrimitiveType.Json
        elif isinstance(type_, sa.JSON.JSONStrIndexType):
            ydb_type = ydb.PrimitiveType.Utf8
        elif isinstance(type_, sa.JSON.JSONIntIndexType):
            ydb_type = ydb.PrimitiveType.Int64
        elif isinstance(type_, sa.JSON.JSONPathType):
            ydb_type = ydb.PrimitiveType.Utf8
        elif isinstance(type_, types.YqlJSON):
            ydb_type = ydb.PrimitiveType.Json
        elif isinstance(type_, types.YqlJSON.YqlJSONPathType):
            ydb_type = ydb.PrimitiveType.Utf8
        # Json
        elif isinstance(type_, types.YqlDate32):
            ydb_type = ydb.PrimitiveType.Date32
        elif isinstance(type_, types.YqlTimestamp64):
            ydb_type = ydb.PrimitiveType.Timestamp64
        elif isinstance(type_, types.YqlDateTime64):
            ydb_type = ydb.PrimitiveType.Datetime64
        elif isinstance(type_, sa.DATETIME):
            ydb_type = ydb.PrimitiveType.Datetime
        elif isinstance(type_, sa.TIMESTAMP):
            ydb_type = ydb.PrimitiveType.Timestamp
        elif isinstance(type_, sa.DateTime):
            ydb_type = ydb.PrimitiveType.Timestamp
        elif isinstance(type_, sa.Date):
            ydb_type = ydb.PrimitiveType.Date
        elif isinstance(type_, _BinaryType):
            ydb_type = ydb.PrimitiveType.String
        elif isinstance(type_, sa.Float):
            ydb_type = ydb.PrimitiveType.Float
        elif isinstance(type_, sa.Boolean):
            ydb_type = ydb.PrimitiveType.Bool
        elif isinstance(type_, sa.Numeric):
            precision = getattr(type_, "precision", None) or 22
            scale = getattr(type_, "scale", None) or 9
            ydb_type = ydb.DecimalType(precision, scale)
        elif isinstance(type_, (types.ListType, sa.ARRAY)):
            ydb_type = ydb.ListType(self.get_ydb_type(type_.item_type, is_optional=False))
        elif isinstance(type_, sa.TupleType):
            ydb_type = ydb.TupleType()
            for item_type in type_.types:
                ydb_type.add_element(self.get_ydb_type(item_type, is_optional=False))
        elif isinstance(type_, types.StructType):
            ydb_type = ydb.StructType()
            for field, field_type in type_.fields_types.items():
                inner_type = to_instance(field_type)
                ydb_type.add_member(field, self.get_ydb_type(inner_type, is_optional=False))
        else:
            raise NotSupportedError(f"{type_} bind variables not supported")

        if is_optional:
            return ydb.OptionalType(ydb_type)

        return ydb_type


class BaseYqlCompiler(StrSQLCompiler):
    compound_keywords = COMPOUND_KEYWORDS
    _type_compiler_cls = BaseYqlTypeCompiler

    def get_from_hint_text(self, table, text):
        return text

    def group_by_clause(self, select, **kw):
        # Hack to ensure it is possible to define labels in groupby.
        kw.update(within_columns_clause=True)
        return super(BaseYqlCompiler, self).group_by_clause(select, **kw)

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit_clause is not None:
            limit_clause = self._maybe_cast(
                select._limit_clause, types.UInt64, skip_types=(types.UInt64, types.UInt32, types.UInt16, types.UInt8)
            )
            text += "\n LIMIT " + self.process(limit_clause, **kw)
        if select._offset_clause is not None:
            offset_clause = self._maybe_cast(
                select._offset_clause, types.UInt64, skip_types=(types.UInt64, types.UInt32, types.UInt16, types.UInt8)
            )
            if select._limit_clause is None:
                text += "\n LIMIT 1000"  # For some reason, YDB do not support LIMIT NULL OFFSET <num>
            text += " OFFSET " + self.process(offset_clause, **kw)
        return text

    def render_literal_value(self, value, type_):
        if isinstance(value, str):
            for pattern, replacement in ESCAPE_RULES:
                value = value.replace(pattern, replacement)
            return f"'{value}'"
        return super().render_literal_value(value, type_)

    def visit_parametrized_function(self, func, **kwargs):
        name = func.name
        name_parts = []
        for name in name.split("::"):
            fname = (
                self.preparer.quote(name)
                if self.preparer._requires_quotes_illegal_chars(name) or isinstance(name, sa.sql.elements.quoted_name)
                else name
            )

            name_parts.append(fname)

        name = "::".join(name_parts)
        params = func.params_expr._compiler_dispatch(self, **kwargs)
        args = self.function_argspec(func, **kwargs)
        return "%(name)s%(params)s%(args)s" % dict(name=name, params=params, args=args)

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        # Copypaste of `sa.sql.compiler.SQLCompiler.visit_function` with
        # `::` as namespace separator instead of `.`
        if add_to_result_map:
            add_to_result_map(func.name, func.name, (), func.type)

        disp = getattr(self, f"visit_{func.name.lower()}_func", None)
        if disp:
            return disp(func, **kwargs)

        name = sa.sql.compiler.FUNCTIONS.get(func.__class__)
        if name:
            if func._has_args:
                name += "%(expr)s"
        else:
            name = func.name
            name = (
                self.preparer.quote(name)
                if self.preparer._requires_quotes_illegal_chars(name) or isinstance(name, sa.sql.elements.quoted_name)
                else name
            )
            name += "%(expr)s"

        return "::".join(
            [
                (
                    self.preparer.quote(tok)
                    if self.preparer._requires_quotes_illegal_chars(tok)
                    or isinstance(name, sa.sql.elements.quoted_name)
                    else tok
                )
                for tok in func.packagenames
            ]
            + [name]
        ) % {"expr": self.function_argspec(func, **kwargs)}

    def visit_concat_func(self, func, **kwargs):
        arg_sql = " || ".join(self.process(arg, **kwargs) for arg in func.clauses)
        return arg_sql

    def _is_bound_to_nullable_column(self, bind_name: str) -> bool:
        if bind_name in self.column_keys and hasattr(self.compile_state, "dml_table"):
            if bind_name in self.compile_state.dml_table.c:
                column = self.compile_state.dml_table.c[bind_name]
                return column.nullable and not column.primary_key
        return False

    def _guess_bound_variable_type_by_parameters(
        self, bind, post_compile_bind_values: list
    ) -> Optional[sa.types.TypeEngine]:
        bind_type = bind.type
        if bind.expanding or (isinstance(bind.type, sa.types.NullType) and post_compile_bind_values):
            not_null_values = [v for v in post_compile_bind_values if v is not None]
            if not_null_values:
                bind_type = _bindparam("", not_null_values[0]).type

        if isinstance(bind_type, sa.types.NullType):
            return None

        return bind_type

    def _get_expanding_bind_names(self, bind_name: str, parameters_values: Mapping[str, List[Any]]) -> List[Any]:
        expanding_bind_names = []
        for parameter_name in parameters_values:
            parameter_bind_name = "_".join(parameter_name.split("_")[:-1])
            if parameter_bind_name == bind_name:
                expanding_bind_names.append(parameter_name)
        return expanding_bind_names

    def render_bind_cast(self, type_, dbapi_type, sqltext):
        pass

    def get_bind_types(
        self, post_compile_parameters: Optional[Union[Sequence[Mapping[str, Any]], Mapping[str, Any]]]
    ) -> Dict[str, Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]]:
        """
        This method extracts information about bound variables from the table definition and parameters.
        """
        if isinstance(post_compile_parameters, collections.abc.Mapping):
            post_compile_parameters = [post_compile_parameters]

        parameters_values = collections.defaultdict(list)
        for parameters_entry in post_compile_parameters:
            for parameter_name, parameter_value in parameters_entry.items():
                parameters_values[parameter_name].append(parameter_value)

        parameter_types = {}
        for bind_name in self.bind_names.values():
            bind = self.binds[bind_name]

            if bind.literal_execute:
                continue

            if not bind.expanding:
                post_compile_bind_names = [bind_name]
                post_compile_bind_values = parameters_values[bind_name]
            else:
                post_compile_bind_names = self._get_expanding_bind_names(bind_name, parameters_values)
                post_compile_bind_values = []
                for parameter_name, parameter_values in parameters_values.items():
                    if parameter_name in post_compile_bind_names:
                        post_compile_bind_values.extend(parameter_values)

            is_optional = self._is_bound_to_nullable_column(bind_name)
            if not post_compile_bind_values or None in post_compile_bind_values:
                is_optional = True

            bind_type = self._guess_bound_variable_type_by_parameters(bind, post_compile_bind_values)

            if bind_type:
                for post_compile_bind_name in post_compile_bind_names:
                    parameter_types[post_compile_bind_name] = self._type_compiler_cls(self.dialect).get_ydb_type(
                        bind_type, is_optional
                    )

        return parameter_types

    def _maybe_cast(
        self,
        element: Any,
        cast_to: Type[sa.types.TypeEngine],
        skip_types: Optional[Tuple[Type[sa.types.TypeEngine], ...]] = None,
    ) -> Any:
        if not skip_types:
            skip_types = (cast_to,)
        if cast_to not in skip_types:
            skip_types = (*skip_types, cast_to)
        if not hasattr(element, "type") or not isinstance(element.type, skip_types):
            return _cast(element, cast_to)
        return element


class BaseYqlDDLCompiler(DDLCompiler):
    def visit_create_index(self, create: ddl.CreateIndex, **kw) -> str:
        index: sa.Index = create.element
        ydb_opts = index.dialect_options.get("ydb", {})

        self._verify_index_table(index)

        if index.name is None:
            raise CompileError("ADD INDEX requires that the index has a name")

        table_name = self.preparer.format_table(index.table)
        index_name = self._prepared_index_name(index)

        text = f"ALTER TABLE {table_name} ADD INDEX {index_name} GLOBAL"

        text += " SYNC" if not ydb_opts.get("async", False) else " ASYNC"

        columns = [self.preparer.format_column(col) for col in index.columns.values()]
        cover_columns = [
            col if isinstance(col, str) else self.preparer.format_column(col) for col in ydb_opts.get("cover", [])
        ]
        cover_columns = list(dict.fromkeys(cover_columns))  # dict preserves order

        text += " ON (" + ", ".join(columns) + ")"

        if cover_columns:
            text += " COVER (" + ", ".join(cover_columns) + ")"

        return text

    def visit_drop_index(self, drop: ddl.DropIndex, **kw) -> str:
        index: sa.Index = drop.element

        self._verify_index_table(index)

        table_name = self.preparer.format_table(index.table)
        index_name = self._prepared_index_name(index)

        return f"ALTER TABLE {table_name} DROP INDEX {index_name}"

    def post_create_table(self, table: sa.Table) -> str:
        ydb_opts = table.dialect_options["ydb"]
        with_clause_list = self._render_table_partitioning_settings(ydb_opts)
        if with_clause_list:
            with_clause_text = ",\n".join(with_clause_list)
            return f"\nWITH (\n\t{with_clause_text}\n)"
        return ""

    def _render_table_partitioning_settings(self, ydb_opts: Dict[str, Any]) -> List[str]:
        table_partitioning_settings = []
        if ydb_opts["auto_partitioning_by_size"] is not None:
            auto_partitioning_by_size = "ENABLED" if ydb_opts["auto_partitioning_by_size"] else "DISABLED"
            table_partitioning_settings.append(f"AUTO_PARTITIONING_BY_SIZE = {auto_partitioning_by_size}")
        if ydb_opts["auto_partitioning_by_load"] is not None:
            auto_partitioning_by_load = "ENABLED" if ydb_opts["auto_partitioning_by_load"] else "DISABLED"
            table_partitioning_settings.append(f"AUTO_PARTITIONING_BY_LOAD = {auto_partitioning_by_load}")
        if ydb_opts["auto_partitioning_partition_size_mb"] is not None:
            table_partitioning_settings.append(
                f"AUTO_PARTITIONING_PARTITION_SIZE_MB = {ydb_opts['auto_partitioning_partition_size_mb']}"
            )
        if ydb_opts["auto_partitioning_min_partitions_count"] is not None:
            table_partitioning_settings.append(
                f"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {ydb_opts['auto_partitioning_min_partitions_count']}"
            )
        if ydb_opts["auto_partitioning_max_partitions_count"] is not None:
            table_partitioning_settings.append(
                f"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {ydb_opts['auto_partitioning_max_partitions_count']}"
            )
        if ydb_opts["uniform_partitions"] is not None:
            table_partitioning_settings.append(f"UNIFORM_PARTITIONS = {ydb_opts['uniform_partitions']}")
        if ydb_opts["partition_at_keys"] is not None:
            table_partitioning_settings.append(f"PARTITION_AT_KEYS = {ydb_opts['partition_at_keys']}")
        return table_partitioning_settings


class BaseYqlIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect):
        super(BaseYqlIdentifierPreparer, self).__init__(
            dialect,
            initial_quote="`",
            final_quote="`",
        )

    def format_index(self, index: sa.Index) -> str:
        return super().format_index(index).replace("/", "_")
