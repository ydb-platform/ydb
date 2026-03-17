from __future__ import annotations

import sys
from collections.abc import Sequence
from copy import copy
from functools import reduce
from typing import TYPE_CHECKING, Any, cast, overload

from .context import DEFAULT_SQL_CONTEXT, SqlContext
from .enums import Dialects, JoinType, SetOperation
from .exceptions import JoinException, QueryException, RollupException, SetOperationException
from .terms import (
    ArithmeticExpression,
    Criterion,
    EmptyCriterion,
    Field,
    Function,
    Index,
    Node,
    Order,
    Parameterizer,
    PeriodCriterion,
    Rollup,
    Star,
    Term,
    Tuple,
    ValueWrapper,
)
from .utils import builder, format_alias_sql, format_quotes, ignore_copy

if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self


class Selectable(Node):
    def __init__(self, alias: str) -> None:
        self.alias = alias

    @builder
    def as_(self, alias: str) -> Self:  # type:ignore[return]
        self.alias = alias

    def field(self, name: str) -> Field:
        return Field(name, table=self)

    @property
    def star(self) -> Star:
        return Star(self)

    @ignore_copy
    def __getattr__(self, name: str) -> Field:
        return self.field(name)

    @ignore_copy
    def __getitem__(self, name: str) -> Field:
        return self.field(name)

    def get_table_name(self) -> str:
        return self.alias

    def get_sql(self, ctx: SqlContext) -> str:
        raise NotImplementedError()


class AliasedQuery(Selectable):
    def __init__(
        self,
        name: str,
        query: Selectable | None = None,
    ) -> None:
        super().__init__(alias=name)
        self.name = name
        self.query = query

    def get_sql(self, ctx: SqlContext) -> str:
        if self.query is None:
            return self.name
        return self.query.get_sql(ctx)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, AliasedQuery) and self.name == other.name

    def __hash__(self) -> int:
        return hash(str(self.name))


class Cte(AliasedQuery):
    def __init__(self, name: str, query: QueryBuilder | None = None, *terms: Term) -> None:
        super().__init__(name, query)
        self.query = query
        self.terms = terms


class Schema:
    def __init__(self, name: str, parent: Schema | None = None) -> None:
        self._name = name
        self._parent = parent

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Schema)
            and self._name == other._name
            and self._parent == other._parent
        )

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    @ignore_copy
    def __getattr__(self, item: str) -> Table:
        return Table(item, schema=self)

    def get_sql(self, ctx: SqlContext) -> str:
        # FIXME escape
        schema_sql = format_quotes(self._name, ctx.quote_char)

        if self._parent is not None:
            return "{parent}.{schema}".format(
                parent=self._parent.get_sql(ctx),
                schema=schema_sql,
            )

        return schema_sql


class Database(Schema):
    @ignore_copy
    def __getattr__(self, item: str) -> Schema:  # type:ignore[override]
        return Schema(item, parent=self)


class Table(Selectable):
    @overload
    @staticmethod
    def _init_schema(
        schema: None,
    ) -> None: ...

    @overload
    @staticmethod
    def _init_schema(
        schema: str | list | tuple | Schema,
    ) -> Schema: ...

    @staticmethod
    def _init_schema(
        schema: str | list | tuple | Schema | None,
    ) -> Schema | None:
        # This is a bit complicated in order to support backwards compatibility. It should probably be cleaned up for
        # the next major release. Schema is accepted as a string, list/tuple, Schema instance, or None
        if isinstance(schema, Schema):
            return schema
        if isinstance(schema, (list, tuple)):
            return reduce(lambda obj, s: Schema(s, parent=obj), schema[1:], Schema(schema[0]))
        if schema is not None:
            return Schema(schema)
        return None

    def __init__(
        self,
        name: str,
        schema: Schema | str | None = None,
        alias: str | None = None,
        query_cls: type[Query] | None = None,
    ) -> None:
        super().__init__(alias)  # type:ignore[arg-type]
        self._table_name = name
        self._schema = self._init_schema(schema)
        if query_cls is None:
            query_cls = Query
        elif not issubclass(query_cls, Query):
            raise TypeError("Expected 'query_cls' to be subclass of Query")
        self._query_cls = query_cls
        self._for: Criterion | None = None
        self._for_portion: PeriodCriterion | None = None

    def get_table_name(self) -> str:
        return self.alias or self._table_name

    def get_sql(self, ctx: SqlContext) -> str:
        # FIXME escape
        table_sql = format_quotes(self._table_name, ctx.quote_char)

        if self._schema is not None:
            table_sql = "{schema}.{table}".format(schema=self._schema.get_sql(ctx), table=table_sql)

        if self._for:
            table_sql = "{table} FOR {criterion}".format(
                table=table_sql, criterion=self._for.get_sql(ctx)
            )
        elif self._for_portion:
            table_sql = "{table} FOR PORTION OF {criterion}".format(
                table=table_sql, criterion=self._for_portion.get_sql(ctx)
            )

        return format_alias_sql(table_sql, self.alias, ctx)

    @builder
    def for_(self, temporal_criterion: Criterion) -> Self:  # type:ignore[return]
        if self._for:
            raise AttributeError("'Query' object already has attribute for_")
        if self._for_portion:
            raise AttributeError("'Query' object already has attribute for_portion")
        self._for = temporal_criterion

    @builder
    def for_portion(self, period_criterion: PeriodCriterion) -> Self:  # type:ignore[return]
        if self._for_portion:
            raise AttributeError("'Query' object already has attribute for_portion")
        if self._for:
            raise AttributeError("'Query' object already has attribute for_")
        self._for_portion = period_criterion

    def __str__(self) -> str:
        return self.get_sql(DEFAULT_SQL_CONTEXT)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Table)
            and self._table_name == other._table_name
            and self._schema == other._schema
            and self.alias == other.alias
        )

    def __repr__(self) -> str:
        if self._schema:
            return "Table('{}', schema='{}')".format(self._table_name, self._schema)
        return "Table('{}')".format(self._table_name)

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(str(self))

    def select(self, *terms: Sequence[int | float | str | bool | Term | Field]) -> QueryBuilder:
        """
        Perform a SELECT operation on the current table

        :param terms:
            Type:  list[expression]

            A list of terms to select. These can be any type of int, float, str, bool or Term or a Field.

        :return:  QueryBuilder
        """
        return self._query_cls.from_(self).select(*terms)

    def update(self) -> QueryBuilder:
        """
        Perform an UPDATE operation on the current table

        :return: QueryBuilder
        """
        return self._query_cls.update(self)

    def insert(self, *terms: int | float | str | bool | Term | Field) -> QueryBuilder:
        """
        Perform an INSERT operation on the current table

        :param terms:
            Type: list[expression]

            A list of terms to select. These can be any type of int, float, str, bool or  any other valid data

        :return: QueryBuilder
        """
        return self._query_cls.into(self).insert(*terms)


def make_tables(*names: tuple[str, str] | str, **kwargs: Any) -> list[Table]:
    """
    Shortcut to create many tables. If `names` param is a tuple, the first
    position will refer to the `_table_name` while the second will be its `alias`.
    Any other data structure will be treated as a whole as the `_table_name`.
    """
    tables = []
    for name in names:
        alias = None
        if isinstance(name, tuple) and len(name) == 2:
            name, alias = name
        t = Table(
            name=name,
            alias=alias,
            schema=kwargs.get("schema"),
            query_cls=kwargs.get("query_cls"),
        )
        tables.append(t)
    return tables


class Column:
    """Represents a column."""

    def __init__(
        self,
        column_name: str,
        column_type: str | None = None,
        nullable: bool | None = None,
        default: Any | Term | None = None,
    ) -> None:
        self.name = column_name
        self.type = column_type
        self.nullable = nullable
        self.default = (
            default if default is None or isinstance(default, Term) else ValueWrapper(default)
        )

    def get_name_sql(self, ctx: SqlContext) -> str:
        column_sql = "{name}".format(
            name=format_quotes(self.name, ctx.quote_char),
        )

        return column_sql

    def get_sql(self, ctx: SqlContext) -> str:
        column_sql = "{name}{type}{nullable}{default}".format(
            name=self.get_name_sql(ctx),
            type=" {}".format(self.type) if self.type else "",
            nullable=(
                " {}".format("NULL" if self.nullable else "NOT NULL")
                if self.nullable is not None
                else ""
            ),
            default=(" {}".format("DEFAULT " + self.default.get_sql(ctx)) if self.default else ""),
        )

        return column_sql

    def __str__(self) -> str:
        return self.get_sql(DEFAULT_SQL_CONTEXT)


def make_columns(*names: tuple[str, str] | str) -> list[Column]:
    """
    Shortcut to create many columns. If `names` param is a tuple, the first
    position will refer to the `name` while the second will be its `type`.
    Any other data structure will be treated as a whole as the `name`.
    """
    columns = []
    for name in names:
        if isinstance(name, tuple) and len(name) == 2:
            column = Column(column_name=name[0], column_type=name[1])
        else:
            column = Column(column_name=name)
        columns.append(column)

    return columns


class PeriodFor:
    def __init__(
        self,
        name: str,
        start_column: str | Column,
        end_column: str | Column,
    ) -> None:
        self.name = name
        self.start_column = (
            start_column if isinstance(start_column, Column) else Column(start_column)
        )
        self.end_column = end_column if isinstance(end_column, Column) else Column(end_column)

    def get_sql(self, ctx: SqlContext) -> str:
        period_for_sql = "PERIOD FOR {name} ({start_column_name},{end_column_name})".format(
            name=format_quotes(self.name, ctx.quote_char),
            start_column_name=self.start_column.get_name_sql(ctx),
            end_column_name=self.end_column.get_name_sql(ctx),
        )

        return period_for_sql


# for typing in Query's methods
_TableClass = Table


class Query:
    """
    Query is the primary class and entry point in pypika. It is used to build queries iteratively using the builder
    design
    pattern.

    This class is immutable.
    """

    SQL_CONTEXT: SqlContext = DEFAULT_SQL_CONTEXT

    @classmethod
    def _builder(cls, **kwargs: Any) -> QueryBuilder:
        return QueryBuilder(**kwargs)

    @classmethod
    def from_(cls, table: Selectable | str, **kwargs: Any) -> QueryBuilder:
        """
        Query builder entry point.  Initializes query building and sets the table to select from.  When using this
        function, the query becomes a SELECT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).from_(table)

    @classmethod
    def create_table(cls, table: str | Table) -> CreateQueryBuilder:
        """
        Query builder entry point. Initializes query building and sets the table name to be created. When using this
        function, the query becomes a CREATE statement.

        :param table: An instance of a Table object or a string table name.

        :return: CreateQueryBuilder
        """
        return CreateQueryBuilder().create_table(table)

    @classmethod
    def drop_table(cls, table: str | Table) -> DropQueryBuilder:
        """
        Query builder entry point. Initializes query building and sets the table name to be dropped. When using this
        function, the query becomes a DROP statement.

        :param table: An instance of a Table object or a string table name.

        :return: DropQueryBuilder
        """
        return DropQueryBuilder().drop_table(table)

    @classmethod
    def into(cls, table: Table | str, **kwargs: Any) -> QueryBuilder:
        """
        Query builder entry point.  Initializes query building and sets the table to insert into.  When using this
        function, the query becomes an INSERT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).into(table)

    @classmethod
    def with_(cls, table: str | Selectable, name: str, *terms: Term, **kwargs: Any) -> QueryBuilder:
        return cls._builder(**kwargs).with_(table, name, *terms)

    @classmethod
    def select(cls, *terms: int | float | str | bool | Term, **kwargs: Any) -> QueryBuilder:
        """
        Query builder entry point.  Initializes query building without a table and selects fields.  Useful when testing
        SQL functions.

        :param terms:
            Type: list[expression]

            A list of terms to select.  These can be any type of int, float, str, bool, or Term.  They cannot be a Field
            unless the function ``Query.from_`` is called first.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).select(*terms)

    @classmethod
    def update(cls, table: str | Table, **kwargs) -> QueryBuilder:
        """
        Query builder entry point.  Initializes query building and sets the table to update.  When using this
        function, the query becomes an UPDATE query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).update(table)

    @classmethod
    def Table(cls, table_name: str, **kwargs) -> _TableClass:
        """
        Convenience method for creating a Table that uses this Query class.

        :param table_name:
            Type: str

            A string table name.

        :returns Table
        """
        kwargs["query_cls"] = cls
        return Table(table_name, **kwargs)

    @classmethod
    def Tables(cls, *names: tuple[str, str] | str, **kwargs: Any) -> list[_TableClass]:
        """
        Convenience method for creating many tables that uses this Query class.
        See ``Query.make_tables`` for details.

        :param names:
            Type: list[str or tuple]

            A list of string table names, or name and alias tuples.

        :returns Table
        """
        kwargs["query_cls"] = cls
        return make_tables(*names, **kwargs)


class _SetOperation(Selectable, Term):  # type:ignore[misc]
    """
    A Query class wrapper for a all set operations, Union DISTINCT or ALL, Intersect, Except or Minus

    Created via the functions `Query.union`,`Query.union_all`,`Query.intersect`, `Query.except_of`,`Query.minus`.

    This class should not be instantiated directly.
    """

    def __init__(
        self,
        base_query: QueryBuilder,
        set_operation_query: QueryBuilder,
        set_operation: SetOperation,
        alias: str | None = None,
        wrapper_cls: type[ValueWrapper] = ValueWrapper,
    ) -> None:
        super().__init__(alias)  # type:ignore[arg-type]
        self.base_query = base_query
        self._set_operation = [(set_operation, set_operation_query)]
        self._orderbys: list[tuple[Field, Order | None]] = []

        self._limit: ValueWrapper | None = None
        self._offset: ValueWrapper | None = None

        self._wrapper_cls = wrapper_cls

    @builder
    def orderby(self, *fields: Field, **kwargs: Any) -> Self:  # type:ignore[return]
        for field in fields:
            field = (
                Field(field, table=self.base_query._from[0])  # type:ignore[assignment]
                if isinstance(field, str)
                else self.base_query.wrap_constant(field)
            )

            self._orderbys.append((field, kwargs.get("order")))

    @builder
    def limit(self, limit: int) -> Self:  # type:ignore[return]
        self._limit = cast(ValueWrapper, self.wrap_constant(limit))

    @builder
    def offset(self, offset: int) -> Self:  # type:ignore[return]
        self._offset = cast(ValueWrapper, self.wrap_constant(offset))

    @builder
    def union(self, other: Selectable) -> Self:  # type:ignore[return]
        self._set_operation.append((SetOperation.union, other))  # type:ignore[arg-type]

    @builder
    def union_all(self, other: Selectable) -> Self:  # type:ignore[return]
        self._set_operation.append((SetOperation.union_all, other))  # type:ignore[arg-type]

    @builder
    def intersect(self, other: Selectable) -> Self:  # type:ignore[return]
        self._set_operation.append((SetOperation.intersect, other))  # type:ignore[arg-type]

    @builder
    def except_of(self, other: Selectable) -> Self:  # type:ignore[return]
        self._set_operation.append((SetOperation.except_of, other))  # type:ignore[arg-type]

    @builder
    def minus(self, other: Selectable) -> Self:  # type:ignore[return]
        self._set_operation.append((SetOperation.minus, other))  # type:ignore[arg-type]

    def __add__(self, other: Selectable) -> Self:  # type:ignore[override]
        return self.union(other)

    def __mul__(self, other: Selectable) -> Self:  # type:ignore[override]
        return self.union_all(other)

    def __sub__(self, other: QueryBuilder) -> Self:  # type:ignore[override]
        return self.minus(other)

    def __str__(self) -> str:
        return self.get_sql(DEFAULT_SQL_CONTEXT)

    def get_sql(self, ctx: SqlContext) -> str:
        set_operation_template = " {type} {query_string}"

        # Default to the base query's dialect and quote_char
        ctx = ctx.copy(
            dialect=self.base_query.dialect,
            quote_char=self.base_query.QUERY_CLS.SQL_CONTEXT.quote_char,
            parameterizer=ctx.parameterizer,
        )
        set_ctx = ctx.copy(subquery=self.base_query.wrap_set_operation_queries)
        base_querystring = self.base_query.get_sql(set_ctx)

        querystring = base_querystring
        for set_operation, set_operation_query in self._set_operation:
            set_operation_querystring = set_operation_query.get_sql(set_ctx)

            if len(self.base_query._selects) != len(set_operation_query._selects):
                raise SetOperationException(
                    "Queries must have an equal number of select statements in a set operation."
                    "\n\nMain Query:\n{query1}\n\nSet Operations Query:\n{query2}".format(
                        query1=base_querystring, query2=set_operation_querystring
                    )
                )

            querystring += set_operation_template.format(
                type=set_operation.value, query_string=set_operation_querystring
            )

        if self._orderbys:
            querystring += self._orderby_sql(ctx)

        querystring += self._limit_sql(ctx)
        querystring += self._offset_sql(ctx)

        if ctx.subquery:
            querystring = "({query})".format(query=querystring)

        if ctx.with_alias:
            return format_alias_sql(
                querystring,
                self.alias or self._table_name,  # type:ignore[arg-type]
                ctx,
            )

        return querystring

    def _orderby_sql(self, ctx: SqlContext) -> str:
        """
        Produces the ORDER BY part of the query.  This is a list of fields and possibly their directionality, ASC or
        DESC. The clauses are stored in the query under self._orderbys as a list of tuples containing the field and
        directionality (which can be None).

        If an order by field is used in the select clause, determined by a matching , then the ORDER BY clause will use
        the alias, otherwise the field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self.base_query._selects}
        for field, directionality in self._orderbys:
            term = (
                format_quotes(field.alias, ctx.quote_char)
                if field.alias and field.alias in selected_aliases
                else field.get_sql(ctx)
            )

            clauses.append(
                "{term} {orient}".format(term=term, orient=directionality.value)
                if directionality is not None
                else term
            )

        return " ORDER BY {orderby}".format(orderby=",".join(clauses))

    def _offset_sql(self, ctx: SqlContext) -> str:
        if self._offset is None:
            return ""
        return " OFFSET {offset}".format(offset=self._offset.get_sql(ctx))

    def _limit_sql(self, ctx: SqlContext) -> str:
        if self._limit is None:
            return ""
        return " LIMIT {limit}".format(limit=self._limit.get_sql(ctx))


class QueryBuilder(Selectable, Term):  # type:ignore[misc]
    """
    Query Builder is the main class in pypika which stores the state of a query and offers functions which allow the
    state to be branched immutably.
    """

    QUERY_CLS = Query

    def __init__(
        self,
        wrap_set_operation_queries: bool = True,
        wrapper_cls: type[ValueWrapper] = ValueWrapper,
        immutable: bool = True,
    ) -> None:
        super().__init__(None)  # type:ignore[arg-type]

        self._from: list[Table] = []
        self._insert_table: Table | None = None
        self._update_table: Table | None = None
        self._delete_from = False
        self._replace = False

        self._with: list[Cte] = []
        self._selects: list[Field | Function] = []
        self._force_indexes: list[Index] = []
        self._use_indexes: list[Index] = []
        self._columns: list[Field] = []
        self._values: list[list] = []
        self._distinct = False

        self._for_update = False
        self._for_update_nowait = False
        self._for_update_skip_locked = False
        self._for_update_of: set[str] = set()
        self._for_update_no_key = False

        self._wheres: QueryBuilder | Term | None = None
        self._prewheres: Criterion | None = None
        self._groupbys: list[Field] = []
        self._with_totals = False
        self._havings: Criterion | None = None
        self._orderbys: list[tuple] = []
        self._joins: list[Join] = []
        self._unions: list = []

        self._limit: ValueWrapper | None = None
        self._offset: ValueWrapper | None = None

        self._updates: list[tuple] = []

        self._select_star = False
        self._select_star_tables: set[Table] = set()
        self._mysql_rollup = False
        self._select_into = False

        self._subquery_count = 0
        self._foreign_table = False

        self.wrap_set_operation_queries = wrap_set_operation_queries

        self._wrapper_cls = wrapper_cls

        self.immutable = immutable

        self._on_conflict = False
        self._on_conflict_fields: list[str | Term | Field | None] = []
        self._on_conflict_do_nothing = False
        self._on_conflict_do_updates: list[tuple] = []
        self._on_conflict_wheres: Term | None = None
        self._on_conflict_do_update_wheres: Term | None = None

    def __copy__(self) -> Self:
        newone = type(self).__new__(type(self))
        newone.__dict__.update(self.__dict__)
        newone._from = copy(self._from)
        newone._with = copy(self._with)
        newone._selects = copy(self._selects)
        newone._columns = copy(self._columns)
        newone._values = copy(self._values)
        newone._groupbys = copy(self._groupbys)
        newone._orderbys = copy(self._orderbys)
        newone._joins = copy(self._joins)
        newone._unions = copy(self._unions)
        newone._updates = copy(self._updates)
        newone._select_star_tables = copy(self._select_star_tables)
        newone._on_conflict_fields = copy(self._on_conflict_fields)
        newone._on_conflict_do_updates = copy(self._on_conflict_do_updates)
        return newone

    @builder
    def on_conflict(self, *target_fields: str | Term) -> Self:  # type:ignore[return]
        if not self._insert_table:
            raise QueryException("On conflict only applies to insert query")

        self._on_conflict = True

        for target_field in target_fields:
            if isinstance(target_field, str):
                self._on_conflict_fields.append(self._conflict_field_str(target_field))
            elif isinstance(target_field, Term):
                self._on_conflict_fields.append(target_field)

    @builder
    def do_update(  # type:ignore[return]
        self, update_field: str | Field, update_value: Any | None = None
    ) -> Self:
        if self._on_conflict_do_nothing:
            raise QueryException("Can not have two conflict handlers")

        if isinstance(update_field, str):
            field = self._conflict_field_str(update_field)
        elif isinstance(update_field, Field):
            field = update_field
        else:
            raise QueryException("Unsupported update_field")

        if update_value is not None:
            self._on_conflict_do_updates.append((field, ValueWrapper(update_value)))
        else:
            self._on_conflict_do_updates.append((field, None))

    def _conflict_field_str(self, term: str) -> Field | None:
        if self._insert_table:
            return Field(term, table=self._insert_table)
        return None

    def _on_conflict_sql(self, ctx: SqlContext) -> str:
        if not self._on_conflict_do_nothing and len(self._on_conflict_do_updates) == 0:
            if not self._on_conflict_fields:
                return ""
            raise QueryException("No handler defined for on conflict")

        if self._on_conflict_do_updates and not self._on_conflict_fields:
            raise QueryException("Can not have fieldless on conflict do update")

        conflict_query = " ON CONFLICT"
        if self._on_conflict_fields:
            on_conflict_ctx = ctx.copy(with_alias=True)
            fields = [
                f.get_sql(on_conflict_ctx)  # type:ignore[union-attr]
                for f in self._on_conflict_fields
            ]
            conflict_query += " (" + ", ".join(fields) + ")"

        if self._on_conflict_wheres:
            where_ctx = ctx.copy(subquery=True)
            conflict_query += " WHERE {where}".format(
                where=self._on_conflict_wheres.get_sql(where_ctx)
            )

        return conflict_query

    def _on_conflict_action_sql(self, ctx: SqlContext) -> str:
        ctx = ctx.copy(with_namespace=False)
        if self._on_conflict_do_nothing:
            return " DO NOTHING"
        elif len(self._on_conflict_do_updates) > 0:
            updates = []
            value_ctx = ctx.copy(with_namespace=True)
            for field, value in self._on_conflict_do_updates:
                if value:
                    updates.append(
                        "{field}={value}".format(
                            field=field.get_sql(ctx), value=value.get_sql(value_ctx)
                        )
                    )
                else:
                    updates.append(
                        "{field}=EXCLUDED.{value}".format(
                            field=field.get_sql(ctx),
                            value=field.get_sql(ctx),
                        )
                    )
            action_sql = " DO UPDATE SET {updates}".format(updates=",".join(updates))  # nosec:B608

            if self._on_conflict_do_update_wheres:
                action_sql += " WHERE {where}".format(
                    where=self._on_conflict_do_update_wheres.get_sql(
                        ctx.copy(subquery=True, with_namespace=True)
                    )
                )
            return action_sql

        return ""

    @builder
    def from_(self, selectable: Selectable | Query | str) -> Self:  # type:ignore[return]
        """
        Adds a table to the query. This function can only be called once and will raise an AttributeError if called a
        second time.

        :param selectable:
            Type: ``Table``, ``Query``, or ``str``

            When a ``str`` is passed, a table with the name matching the ``str`` value is used.

        :returns
            A copy of the query with the table added.
        """

        self._from.append(
            Table(selectable) if isinstance(selectable, str) else selectable  # type:ignore[arg-type]
        )

        if isinstance(selectable, (QueryBuilder, _SetOperation)) and selectable.alias is None:
            if isinstance(selectable, QueryBuilder):
                sub_query_count = selectable._subquery_count
            else:
                sub_query_count = 0

            sub_query_count = max(self._subquery_count, sub_query_count)
            selectable.alias = "sq%d" % sub_query_count
            self._subquery_count = sub_query_count + 1

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across
        queries.

        :param current_table:
            The table instance to be replaces.
        :param new_table:
            The table instance to replace with.
        :return:
            A copy of the query with the tables replaced.
        """
        self._from = [
            new_table if table == current_table else table  # type:ignore[misc]
            for table in self._from
        ]
        if self._insert_table == current_table:
            self._insert_table = new_table
        if self._update_table == current_table:
            self._update_table = new_table

        self._with = [
            alias_query.replace_table(current_table, new_table)  # type:ignore[operator]
            for alias_query in self._with
        ]
        self._selects = [select.replace_table(current_table, new_table) for select in self._selects]
        self._columns = [column.replace_table(current_table, new_table) for column in self._columns]
        self._values = [
            [value.replace_table(current_table, new_table) for value in value_list]
            for value_list in self._values
        ]

        self._wheres = (
            self._wheres.replace_table(current_table, new_table) if self._wheres else None
        )
        self._prewheres = (
            self._prewheres.replace_table(current_table, new_table) if self._prewheres else None
        )
        self._groupbys = [
            groupby.replace_table(current_table, new_table) for groupby in self._groupbys
        ]
        self._havings = (
            self._havings.replace_table(current_table, new_table) if self._havings else None
        )
        self._orderbys = [
            (orderby[0].replace_table(current_table, new_table), orderby[1])
            for orderby in self._orderbys
        ]
        self._joins = [join.replace_table(current_table, new_table) for join in self._joins]

        if current_table in self._select_star_tables:
            self._select_star_tables.remove(current_table)
            self._select_star_tables.add(cast(Table, new_table))

    @builder
    def with_(  # type:ignore[return]
        self, selectable: QueryBuilder, name: str, *terms: Term
    ) -> Self:
        t = Cte(name, selectable, *terms)
        self._with.append(t)

    @builder
    def into(self, table: str | Table) -> Self:  # type:ignore[return]
        if self._insert_table is not None:
            raise AttributeError("'Query' object has no attribute '%s'" % "into")

        if self._selects:
            self._select_into = True

        self._insert_table = table if isinstance(table, Table) else Table(table)

    @builder
    def select(self, *terms: Any) -> Self:  # type:ignore[return]
        for term in terms:
            if isinstance(term, Field):
                self._select_field(term)
            elif isinstance(term, str):
                self._select_field_str(term)
            elif isinstance(term, (Function, ArithmeticExpression)):
                self._select_other(term)  # type:ignore[arg-type]
            else:
                self._select_other(
                    self.wrap_constant(term, wrapper_cls=self._wrapper_cls)  # type:ignore[arg-type]
                )

    @builder
    def delete(self) -> Self:  # type:ignore[return]
        if self._delete_from or self._selects or self._update_table:
            raise AttributeError("'Query' object has no attribute '%s'" % "delete")

        self._delete_from = True

    @builder
    def update(self, table: str | Table) -> Self:  # type:ignore[return]
        if self._update_table is not None or self._selects or self._delete_from:
            raise AttributeError("'Query' object has no attribute '%s'" % "update")

        self._update_table = table if isinstance(table, Table) else Table(table)

    @builder
    def columns(self, *terms: Any) -> Self:  # type:ignore[return]
        if self._insert_table is None:
            raise AttributeError("'Query' object has no attribute '%s'" % "insert")

        if terms and isinstance(terms[0], (list, tuple)):
            terms = terms[0]  # type:ignore[assignment]

        for term in terms:
            if isinstance(term, str):
                term = Field(term, table=self._insert_table)
            self._columns.append(term)

    @builder
    def insert(self, *terms: Any) -> Self:  # type:ignore[return]
        if self._insert_table is None:
            raise AttributeError("'Query' object has no attribute '%s'" % "insert")

        if terms:
            self._validate_terms_and_append(*terms)
            self._replace = False

    @builder
    def replace(self, *terms: Any) -> Self:  # type:ignore[return]
        if self._insert_table is None:
            raise AttributeError("'Query' object has no attribute '%s'" % "insert")

        if terms:
            self._validate_terms_and_append(*terms)
            self._replace = True

    @builder
    def force_index(  # type:ignore[return]
        self, term: str | Index, *terms: str | Index
    ) -> Self:
        for t in (term, *terms):
            if isinstance(t, Index):
                self._force_indexes.append(t)
            elif isinstance(t, str):
                self._force_indexes.append(Index(t))

    @builder
    def use_index(  # type:ignore[return]
        self, term: str | Index, *terms: str | Index
    ) -> Self:
        for t in (term, *terms):
            if isinstance(t, Index):
                self._use_indexes.append(t)
            elif isinstance(t, str):
                self._use_indexes.append(Index(t))

    @builder
    def distinct(self) -> Self:  # type:ignore[return]
        self._distinct = True

    @builder
    def for_update(  # type:ignore[return]
        self,
        nowait: bool = False,
        skip_locked: bool = False,
        of: tuple[str, ...] = (),
        no_key: bool = False,
    ) -> Self:
        self._for_update = True
        self._for_update_skip_locked = skip_locked
        self._for_update_nowait = nowait
        self._for_update_of = set(of)
        self._for_update_no_key = no_key

    @builder
    def do_nothing(self) -> Self:  # type:ignore[return]
        if len(self._on_conflict_do_updates) > 0:
            raise QueryException("Can not have two conflict handlers")
        self._on_conflict_do_nothing = True

    @builder
    def prewhere(self, criterion: Criterion) -> Self:  # type:ignore[return]
        if not self._validate_table(criterion):
            self._foreign_table = True

        if self._prewheres:
            self._prewheres &= criterion
        else:
            self._prewheres = criterion

    @builder
    def where(self, criterion: Term | EmptyCriterion) -> Self:  # type:ignore[return]
        if isinstance(criterion, EmptyCriterion):
            return  # type:ignore[return-value]
        if not self._on_conflict:
            if not self._validate_table(criterion):
                self._foreign_table = True
            if self._wheres:
                self._wheres &= criterion  # type:ignore[operator]
            else:
                self._wheres = criterion
        else:
            if self._on_conflict_do_nothing:
                raise QueryException("DO NOTHING doest not support WHERE")
            if self._on_conflict_fields and self._on_conflict_do_updates:
                if self._on_conflict_do_update_wheres:
                    self._on_conflict_do_update_wheres &= criterion  # type:ignore[operator]
                else:
                    self._on_conflict_do_update_wheres = criterion
            elif self._on_conflict_fields:
                if self._on_conflict_wheres:
                    self._on_conflict_wheres &= criterion  # type:ignore[operator]
                else:
                    self._on_conflict_wheres = criterion
            else:
                raise QueryException("Can not have fieldless ON CONFLICT WHERE")

    @builder
    def having(self, criterion: Criterion) -> Self:  # type:ignore[return]
        if self._havings:
            self._havings &= criterion
        else:
            self._havings = criterion

    @builder
    def groupby(self, *terms: str | int | Term) -> Self:  # type:ignore[return]
        for term in terms:
            if isinstance(term, str):
                term = Field(term, table=self._from[0])
            elif isinstance(term, int):
                field = Field(str(term), table=self._from[0])
                term = field.wrap_constant(term)

            self._groupbys.append(term)  # type:ignore[arg-type]

    @builder
    def with_totals(self) -> Self:  # type:ignore[return]
        self._with_totals = True

    @builder
    def rollup(  # type:ignore[return]
        self, *terms: list | tuple | set | Term, **kwargs: Any
    ) -> Self:
        for_mysql = kwargs.get("vendor") == "mysql"

        if self._mysql_rollup:
            raise AttributeError("'Query' object has no attribute '%s'" % "rollup")

        terms = [  # type:ignore[assignment]
            Tuple(*term) if isinstance(term, (list, tuple, set)) else term for term in terms
        ]

        if for_mysql:
            # MySQL rolls up all of the dimensions always
            if not terms and not self._groupbys:
                raise RollupException(
                    "At least one group is required. Call Query.groupby(term) or pass"
                    "as parameter to rollup."
                )

            self._mysql_rollup = True
            self._groupbys += terms  # type:ignore[arg-type]

        elif len(self._groupbys) > 0 and isinstance(self._groupbys[-1], Rollup):
            # If a rollup was added last, then append the new terms to the previous rollup
            self._groupbys[-1].args += terms

        else:
            self._groupbys.append(Rollup(*terms))  # type:ignore[arg-type]

    @builder
    def orderby(self, *fields: Any, **kwargs: Any) -> Self:  # type:ignore[return]
        for field in fields:
            field = (
                Field(field, table=self._from[0])
                if isinstance(field, str)
                else self.wrap_constant(field)
            )

            self._orderbys.append((field, kwargs.get("order")))

    @builder
    def join(
        self,
        item: Table | QueryBuilder | AliasedQuery | Selectable,
        how: JoinType = JoinType.inner,
    ) -> Joiner:
        if isinstance(item, Table):
            return Joiner(self, item, how, type_label="table")

        elif isinstance(item, QueryBuilder):
            if item.alias is None:
                self._tag_subquery(item)
            return Joiner(self, item, how, type_label="subquery")

        elif isinstance(item, AliasedQuery):
            return Joiner(self, item, how, type_label="table")

        elif isinstance(item, Selectable):
            return Joiner(self, item, how, type_label="subquery")

        raise ValueError("Cannot join on type '%s'" % type(item))

    def inner_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.inner)

    def left_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.left)

    def left_outer_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.left_outer)

    def right_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.right)

    def right_outer_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.right_outer)

    def outer_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.outer)

    def full_outer_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.full_outer)

    def cross_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.cross)

    def hash_join(self, item: Table | QueryBuilder | AliasedQuery) -> Joiner:
        return self.join(item, JoinType.hash)

    @builder
    def limit(self, limit: int) -> Self:  # type:ignore[return]
        self._limit = cast(ValueWrapper, self.wrap_constant(limit))

    @builder
    def offset(self, offset: int) -> Self:  # type:ignore[return]
        self._offset = cast(ValueWrapper, self.wrap_constant(offset))

    @builder
    def union(self, other: Self) -> _SetOperation:
        return _SetOperation(self, other, SetOperation.union, wrapper_cls=self._wrapper_cls)

    @builder
    def union_all(self, other: Self) -> _SetOperation:
        return _SetOperation(self, other, SetOperation.union_all, wrapper_cls=self._wrapper_cls)

    @builder
    def intersect(self, other: Self) -> _SetOperation:
        return _SetOperation(self, other, SetOperation.intersect, wrapper_cls=self._wrapper_cls)

    @builder
    def except_of(self, other: Self) -> _SetOperation:
        return _SetOperation(self, other, SetOperation.except_of, wrapper_cls=self._wrapper_cls)

    @builder
    def minus(self, other: Self) -> _SetOperation:
        return _SetOperation(self, other, SetOperation.minus, wrapper_cls=self._wrapper_cls)

    @builder
    def set(self, field: Field | str, value: Any) -> Self:  # type:ignore[return]
        field = Field(field) if not isinstance(field, Field) else field
        value = self.wrap_constant(value, wrapper_cls=self._wrapper_cls)
        self._updates.append((field, value))

    def __add__(self, other: Self) -> _SetOperation:  # type:ignore[override]
        return self.union(other)

    def __mul__(self, other: Self) -> _SetOperation:  # type:ignore[override]
        return self.union_all(other)

    def __sub__(self, other: Self) -> _SetOperation:  # type:ignore[override]
        return self.minus(other)

    @builder
    def slice(self, slice: slice) -> Self:  # type:ignore[return]
        if slice.start is not None:
            self._offset = cast(ValueWrapper, self.wrap_constant(slice.start))
        if slice.stop is not None:
            self._limit = cast(ValueWrapper, self.wrap_constant(slice.stop))

    def __getitem__(self, item: Any) -> Self | Field:  # type:ignore[override]
        if not isinstance(item, slice):
            return super().__getitem__(item)
        return self.slice(item)

    @staticmethod
    def _list_aliases(field_set: Sequence[Field], ctx: SqlContext) -> list[str]:
        return [field.alias or field.get_sql(ctx) for field in field_set]

    def _select_field_str(self, term: str) -> None:
        if len(self._from) == 0:
            raise QueryException(f"Cannot select {term}, no FROM table specified.")  # nosec:B608

        if term == "*":
            self._select_star = True
            self._selects = [Star()]
            return

        self._select_field(Field(term, table=self._from[0]))

    def _select_field(self, term: Field) -> None:
        if self._select_star:
            # Do not add select terms after a star is selected
            return

        if term.table in self._select_star_tables:
            # Do not add select terms for table after a table star is selected
            return

        if isinstance(term, Star):
            self._selects = [
                select
                for select in self._selects
                if not hasattr(select, "table") or term.table != select.table
            ]
            self._select_star_tables.add(cast(Table, term.table))

        self._selects.append(term)

    def _select_other(self, function: Function) -> None:
        self._selects.append(function)

    def fields_(self) -> list[Field]:  # type:ignore[override]
        # Don't return anything here. Subqueries have their own fields.
        return []

    def do_join(self, join: Join) -> None:
        base_tables = self._from + [self._update_table] + self._with
        join.validate(base_tables, self._joins)  # type:ignore[arg-type]

        table_in_query = any(
            isinstance(clause, Table) and join.item in base_tables for clause in base_tables
        )
        if isinstance(join.item, Table) and join.item.alias is None and table_in_query:
            # On the odd chance that we join the same table as the FROM table and don't set an alias
            # FIXME only works once
            join.item.alias = join.item._table_name + "2"

        self._joins.append(join)

    def is_joined(self, table: Table) -> bool:
        return any(table == join.item for join in self._joins)

    def _validate_table(self, term: Term) -> bool:
        """
        Returns False if the term references a table not already part of the
        FROM clause or JOINS and True otherwise.
        """
        base_tables = self._from + [self._update_table]

        for field in term.fields_():
            table_in_base_tables = field.table in base_tables
            table_in_joins = field.table in [join.item for join in self._joins]
            if all(
                [
                    field.table is not None,
                    not table_in_base_tables,
                    not table_in_joins,
                    field.table != self._update_table,
                ]
            ):
                return False
        return True

    def _tag_subquery(self, subquery: Self) -> None:
        subquery.alias = "sq%d" % self._subquery_count
        self._subquery_count += 1

    def _validate_terms_and_append(self, *terms: Any) -> None:
        """
        Handy function for INSERT and REPLACE statements in order to check if
        terms are introduced and how append them to `self._values`
        """
        if not isinstance(terms[0], (list, tuple, set)):
            terms = [terms]  # type:ignore[assignment]

        for values in terms:
            self._values.append(
                [
                    value if isinstance(value, Term) else self.wrap_constant(value)
                    for value in values
                ]
            )

    def __str__(self) -> str:
        return self.get_sql(self.QUERY_CLS.SQL_CONTEXT)

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: Any) -> bool:  # type:ignore[override]
        return isinstance(other, QueryBuilder) and self.alias == other.alias

    def __ne__(self, other: Any) -> bool:  # type:ignore[override]
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(self.alias) + sum(hash(clause) for clause in self._from)

    def get_sql(self, ctx: SqlContext | None = None) -> str:
        if not ctx:
            ctx = self.QUERY_CLS.SQL_CONTEXT

        if not (self._selects or self._insert_table or self._delete_from or self._update_table):
            return ""
        if self._insert_table and not (self._selects or self._values):
            return ""
        if self._update_table and not self._updates:
            return ""

        has_joins = bool(self._joins)
        has_multiple_from_clauses = len(self._from) > 1
        has_subquery_from_clause = len(self._from) > 0 and isinstance(self._from[0], QueryBuilder)
        has_reference_to_foreign_table = self._foreign_table
        has_update_from = self._update_table and self._from

        ctx = ctx.copy(
            with_namespace=any(
                [
                    has_joins,
                    has_multiple_from_clauses,
                    has_subquery_from_clause,
                    has_reference_to_foreign_table,
                    has_update_from,
                ]
            )
        )

        if self._update_table:
            querystring = self._with_sql(ctx) if self._with else ""
            querystring += self._update_sql(ctx)

            if self._joins:
                querystring += " " + " ".join(join.get_sql(ctx) for join in self._joins)

            querystring += self._set_sql(ctx)

            if self._from:
                querystring += self._from_sql(ctx)

            if self._wheres:
                querystring += self._where_sql(ctx)

            return querystring

        if self._delete_from:
            querystring = self._delete_sql(ctx)

        elif not self._select_into and self._insert_table:
            querystring = self._with_sql(ctx) if self._with else ""

            if self._replace:
                querystring += self._replace_sql(ctx)
            else:
                querystring += self._insert_sql(ctx)

            if self._columns:
                querystring += self._columns_sql(ctx)

            if self._values:
                querystring += self._values_sql(ctx)
                if self._on_conflict:
                    querystring += self._on_conflict_sql(ctx)
                    querystring += self._on_conflict_action_sql(ctx)
                return querystring
            else:
                querystring += " " + self._select_sql(ctx)

        else:
            querystring = self._with_sql(ctx) if self._with else ""
            querystring += self._select_sql(ctx)

            if self._insert_table:
                querystring += self._into_sql(ctx)

        if self._from:
            querystring += self._from_sql(ctx)

        if self._force_indexes:
            querystring += self._force_index_sql(ctx)

        if self._use_indexes:
            querystring += self._use_index_sql(ctx)

        if self._joins:
            querystring += " " + " ".join(join.get_sql(ctx) for join in self._joins)

        if self._prewheres:
            querystring += self._prewhere_sql(ctx)

        if self._wheres:
            querystring += self._where_sql(ctx)

        if self._groupbys:
            querystring += self._group_sql(ctx)
            if self._mysql_rollup:
                querystring += self._rollup_sql()

        if self._havings:
            querystring += self._having_sql(ctx)

        if self._orderbys:
            querystring += self._orderby_sql(ctx)

        querystring = self._apply_pagination(querystring, ctx)

        if self._for_update:
            querystring += self._for_update_sql(ctx)

        if ctx.subquery:
            querystring = "({query})".format(query=querystring)
        if self._on_conflict:
            querystring += self._on_conflict_sql(ctx)
            querystring += self._on_conflict_action_sql(ctx)
        if ctx.with_alias:
            return format_alias_sql(querystring, self.alias, ctx)

        return querystring

    def _apply_pagination(self, querystring: str, ctx: SqlContext) -> str:
        querystring += self._limit_sql(ctx)
        querystring += self._offset_sql(ctx)
        return querystring

    def _with_sql(self, ctx: SqlContext) -> str:
        all_alias = [with_.alias for with_ in self._with]
        recursive = False
        for with_ in self._with:
            if with_.query.from_ in all_alias:  # type:ignore[operator,union-attr]
                recursive = True
                break

        as_ctx = ctx.copy(subquery=False, with_alias=False)
        return f"WITH {'RECURSIVE ' if recursive else ''}" + ",".join(
            clause.alias
            + (
                "(" + ",".join([term.get_sql(ctx) for term in clause.terms]) + ")"
                if clause.terms
                else ""
            )
            + " AS ("
            + clause.get_sql(as_ctx)
            + ") "
            for clause in self._with
        )

    def get_parameterized_sql(self, ctx: SqlContext | None = None) -> tuple[str, list]:
        """
        Returns a tuple containing the query string and a list of parameters
        """
        if not ctx:
            ctx = self.QUERY_CLS.SQL_CONTEXT

        if not ctx.parameterizer:
            ctx = ctx.copy(parameterizer=Parameterizer())

        return (
            self.get_sql(ctx),
            ctx.parameterizer.values,  # type: ignore
        )

    def _distinct_sql(self, ctx: SqlContext) -> str:
        return "DISTINCT " if self._distinct else ""

    def _for_update_sql(self, ctx: SqlContext, lock_strength="UPDATE") -> str:
        if self._for_update:
            for_update = f" FOR {lock_strength}"
            if self._for_update_of:
                for_update += (
                    f" OF {', '.join([Table(item).get_sql(ctx) for item in self._for_update_of])}"
                )
            if self._for_update_nowait:
                for_update += " NOWAIT"
            elif self._for_update_skip_locked:
                for_update += " SKIP LOCKED"
        else:
            for_update = ""

        return for_update

    def _select_sql(self, ctx: SqlContext) -> str:
        select_ctx = ctx.copy(subquery=True, with_alias=True)
        return "SELECT {distinct}{select}".format(
            distinct=self._distinct_sql(ctx),
            select=",".join(term.get_sql(select_ctx) for term in self._selects),
        )

    def _insert_sql(self, ctx: SqlContext) -> str:
        table = self._insert_table.get_sql(ctx)  # type:ignore[union-attr]
        return f"INSERT INTO {table}"

    def _replace_sql(self, ctx: SqlContext) -> str:
        table = self._insert_table.get_sql(ctx)  # type:ignore[union-attr]
        return f"REPLACE INTO {table}"

    @staticmethod
    def _delete_sql(ctx: SqlContext) -> str:
        return "DELETE"

    def _update_sql(self, ctx: SqlContext) -> str:
        table = self._update_table.get_sql(ctx)  # type:ignore[union-attr]
        return f"UPDATE {table}"

    def _columns_sql(self, ctx: SqlContext) -> str:
        """
        SQL for Columns clause for INSERT queries
        """
        # Remove from ctx, never format the column terms with namespaces since only one table can be inserted into
        ctx = ctx.copy(with_namespace=False)
        return " ({columns})".format(columns=",".join(term.get_sql(ctx) for term in self._columns))

    def _values_sql(self, ctx: SqlContext) -> str:
        values_ctx = ctx.copy(subquery=True, with_alias=True)
        return " VALUES ({values})".format(
            values="),(".join(
                ",".join(term.get_sql(values_ctx) for term in row) for row in self._values
            )
        )

    def _into_sql(self, ctx: SqlContext) -> str:
        into_ctx = ctx.copy(with_alias=False)
        return " INTO {table}".format(
            table=self._insert_table.get_sql(into_ctx),  # type:ignore[union-attr]
        )

    def _from_sql(self, ctx: SqlContext) -> str:
        from_ctx = ctx.copy(subquery=True, with_alias=True)
        return " FROM {selectable}".format(
            selectable=",".join(clause.get_sql(from_ctx) for clause in self._from)
        )

    def _force_index_sql(self, ctx: SqlContext) -> str:
        return " FORCE INDEX ({indexes})".format(
            indexes=",".join(index.get_sql(ctx) for index in self._force_indexes),
        )

    def _use_index_sql(self, ctx: SqlContext) -> str:
        return " USE INDEX ({indexes})".format(
            indexes=",".join(index.get_sql(ctx) for index in self._use_indexes),
        )

    def _prewhere_sql(self, ctx: SqlContext) -> str:
        prewhere_sql = ctx.copy(subquery=True)
        prewheres = cast(QueryBuilder, self._prewheres)
        return " PREWHERE {prewhere}".format(prewhere=prewheres.get_sql(prewhere_sql))

    def _where_sql(self, ctx: SqlContext) -> str:
        where_ctx = ctx.copy(subquery=True)
        wheres = cast(QueryBuilder, self._wheres)
        return " WHERE {where}".format(where=wheres.get_sql(where_ctx))

    def _group_sql(
        self,
        ctx: SqlContext,
    ) -> str:
        """
        Produces the GROUP BY part of the query.  This is a list of fields. The clauses are stored in the query under
        self._groupbys as a list fields.

        If an groupby field is used in the select clause,
        determined by a matching alias, and the groupby_alias is set True
        then the GROUP BY clause will use the alias,
        otherwise the entire field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self._selects}
        for field in self._groupbys:
            if (alias := field.alias) and alias in selected_aliases:
                if ctx.groupby_alias:
                    clauses.append(format_quotes(alias, ctx.alias_quote_char or ctx.quote_char))
                else:
                    for select in self._selects:
                        if select.alias == alias:
                            clauses.append(select.get_sql(ctx))
                            break
            else:
                clauses.append(field.get_sql(ctx.copy(with_alias=False)))

        sql = " GROUP BY {groupby}".format(groupby=",".join(clauses))

        if self._with_totals:
            return sql + " WITH TOTALS"

        return sql

    def _orderby_sql(
        self,
        ctx: SqlContext,
    ) -> str:
        """
        Produces the ORDER BY part of the query.  This is a list of fields and possibly their directionality, ASC or
        DESC. The clauses are stored in the query under self._orderbys as a list of tuples containing the field and
        directionality (which can be None).

        If an order by field is used in the select clause,
        determined by a matching, and the orderby_alias
        is set True then the ORDER BY clause will use
        the alias, otherwise the field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self._selects}
        for field, directionality in self._orderbys:
            term = (
                format_quotes(field.alias, ctx.alias_quote_char or ctx.quote_char)
                if ctx.orderby_alias and field.alias and field.alias in selected_aliases
                else field.get_sql(ctx)
            )

            clauses.append(
                "{term} {orient}".format(term=term, orient=directionality.value)
                if directionality is not None
                else term
            )

        return " ORDER BY {orderby}".format(orderby=",".join(clauses))

    def _rollup_sql(self) -> str:
        return " WITH ROLLUP"

    def _having_sql(self, ctx: SqlContext) -> str:
        having = self._havings.get_sql(ctx)  # type:ignore[union-attr]
        return f" HAVING {having}"

    def _offset_sql(self, ctx: SqlContext) -> str:
        if self._offset is None:
            return ""
        return " OFFSET {offset}".format(offset=self._offset.get_sql(ctx))

    def _limit_sql(self, ctx: SqlContext) -> str:
        if self._limit is None:
            return ""
        return " LIMIT {limit}".format(limit=self._limit.get_sql(ctx))

    def _set_sql(self, ctx: SqlContext) -> str:
        field_ctx = ctx.copy(with_namespace=False)
        return " SET {set}".format(
            set=",".join(
                "{field}={value}".format(
                    field=field.get_sql(field_ctx),
                    value=value.get_sql(ctx),
                )
                for field, value in self._updates
            )
        )


class Joiner:
    def __init__(
        self,
        query: QueryBuilder,
        item: Selectable | QueryBuilder | AliasedQuery,
        how: JoinType,
        type_label: str,
    ) -> None:
        self.query = query
        self.item = item
        self.how = how
        self.type_label = type_label

    def on(self, criterion: Criterion | None, collate: str | None = None) -> QueryBuilder:
        if criterion is None:
            raise JoinException(
                "Parameter 'criterion' is required for a {type} JOIN but was not supplied.".format(
                    type=self.type_label
                )
            )

        self.query.do_join(JoinOn(self.item, self.how, criterion, collate))  # type:ignore[arg-type]
        return self.query

    def on_field(self, *fields: Any) -> QueryBuilder:
        if not fields:
            raise JoinException(
                "Parameter 'fields' is required for a {type} JOIN but was not supplied.".format(
                    type=self.type_label
                )
            )

        criterion = None
        for field in fields:
            consituent = Field(field, table=self.query._from[0]) == Field(field, table=self.item)
            criterion = consituent if criterion is None else (criterion & consituent)

        self.query.do_join(JoinOn(self.item, self.how, criterion))  # type:ignore[arg-type]
        return self.query

    def using(self, *fields: Any) -> QueryBuilder:
        if not fields:
            raise JoinException(
                "Parameter 'fields' is required when joining with {type}"
                "a using clause but was not supplied.".format(type=self.type_label)
            )

        self.query.do_join(
            JoinUsing(
                self.item,  # type:ignore[arg-type]
                self.how,
                [Field(field) for field in fields],
            )
        )
        return self.query

    def cross(self) -> QueryBuilder:
        """Return cross join"""
        self.query.do_join(Join(self.item, JoinType.cross))  # type:ignore[arg-type]

        return self.query


class Join:
    def __init__(self, item: Term, how: JoinType) -> None:
        self.item = item
        self.how = how

    def get_sql(self, ctx: SqlContext) -> str:
        join_ctx = ctx.copy(subquery=True, with_alias=True)
        sql = "JOIN {table}".format(
            table=self.item.get_sql(join_ctx),
        )

        if self.how.value:
            return "{type} {join}".format(join=sql, type=self.how.value)
        return sql

    def validate(self, _from: Sequence[Table], _joins: Sequence[Table]) -> None:
        pass

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing
        fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the join with the tables replaced.
        """
        self.item = self.item.replace_table(current_table, new_table)


class JoinOn(Join):
    def __init__(
        self,
        item: Term,
        how: JoinType,
        criteria: QueryBuilder,
        collate: str | None = None,
    ) -> None:
        super().__init__(item, how)
        self.criterion = criteria
        self.collate = collate

    def get_sql(self, ctx: SqlContext) -> str:
        join_sql = super().get_sql(ctx)
        criterion_ctx = ctx.copy(subquery=True)
        return "{join} ON {criterion}{collate}".format(
            join=join_sql,
            criterion=self.criterion.get_sql(criterion_ctx),
            collate=" COLLATE {}".format(self.collate) if self.collate else "",
        )

    def validate(self, _from: Sequence[Table], _joins: Sequence[Table]) -> None:
        criterion_tables = set([f.table for f in self.criterion.fields_()])
        available_tables = set(_from) | {join.item for join in _joins} | {self.item}
        missing_tables = criterion_tables - available_tables  # type:ignore[operator]
        if missing_tables:
            raise JoinException(
                "Invalid join criterion. One field is required from the joined item and "
                "another from the selected table or an existing join.  Found [{tables}]".format(
                    tables=", ".join(map(str, missing_tables))
                )
            )

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing
        fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the join with the tables replaced.
        """
        if self.item == current_table:
            self.item = new_table  # type:ignore[assignment]
        self.criterion = self.criterion.replace_table(current_table, new_table)


class JoinUsing(Join):
    def __init__(self, item: Term, how: JoinType, fields: Sequence[Field]) -> None:
        super().__init__(item, how)
        self.fields = fields

    def get_sql(self, ctx: SqlContext) -> str:
        join_sql = super().get_sql(ctx)
        return "{join} USING ({fields})".format(
            join=join_sql,
            fields=",".join(field.get_sql(ctx) for field in self.fields),
        )

    def validate(self, _from: Sequence[Table], _joins: Sequence[Table]) -> None:
        pass

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing
        fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the join with the tables replaced.
        """
        if self.item == current_table:
            self.item = new_table  # type:ignore[assignment]
        self.fields = [field.replace_table(current_table, new_table) for field in self.fields]


class CreateQueryBuilder:
    """
    Query builder used to build CREATE queries.
    """

    QUERY_CLS = Query

    def __init__(self, dialect: Dialects | None = None) -> None:
        self._create_table: Table | None = None
        self._temporary = False
        self._unlogged = False
        self._as_select: QueryBuilder | None = None
        self._columns: list = []
        self._period_fors: list = []
        self._with_system_versioning = False
        self._primary_key: list[Column] | None = None
        self._uniques: list[list[Column]] = []
        self._if_not_exists = False
        self.dialect = dialect

    @builder
    def create_table(self, table: Table | str) -> Self:  # type:ignore[return]
        """
        Creates the table.

        :param table:
            An instance of a Table object or a string table name.

        :raises AttributeError:
            If the table is already created.

        :return:
            CreateQueryBuilder.
        """
        if self._create_table:
            raise AttributeError("'Query' object already has attribute create_table")

        self._create_table = table if isinstance(table, Table) else Table(table)

    @builder
    def temporary(self) -> Self:  # type:ignore[return]
        """
        Makes the table temporary.

        :return:
            CreateQueryBuilder.
        """
        self._temporary = True

    @builder
    def unlogged(self) -> Self:  # type:ignore[return]
        """
        Makes the table unlogged.

        :return:
            CreateQueryBuilder.
        """
        self._unlogged = True

    @builder
    def with_system_versioning(self) -> Self:  # type:ignore[return]
        """
        Adds system versioning.

        :return:
            CreateQueryBuilder.
        """
        self._with_system_versioning = True

    @builder
    def columns(  # type:ignore[return]
        self, *columns: str | tuple[str, str] | Column
    ) -> Self:
        """
        Adds the columns.

        :param columns:
            Type:  str | tuple[str, str] | Column

            A list of columns.

        :raises AttributeError:
            If the table is an as_select table.

        :return:
            CreateQueryBuilder.
        """
        if self._as_select:
            raise AttributeError("'Query' object already has attribute as_select")

        for column in columns:
            if isinstance(column, str):
                column = Column(column)
            elif isinstance(column, tuple):
                column = Column(column_name=column[0], column_type=column[1])
            self._columns.append(column)

    @builder
    def period_for(  # type:ignore[return]
        self, name, start_column: str | Column, end_column: str | Column
    ) -> Self:
        """
        Adds a PERIOD FOR clause.

        :param name:
            The period name.

        :param start_column:
            The column that starts the period.

        :param end_column:
            The column that ends the period.

        :return:
            CreateQueryBuilder.
        """
        self._period_fors.append(PeriodFor(name, start_column, end_column))

    @builder
    def unique(self, *columns: str | Column) -> Self:  # type:ignore[return]
        """
        Adds a UNIQUE constraint.

        :param columns:
            Type:  str | tuple[str, str] | Column

            A list of columns.

        :return:
            CreateQueryBuilder.
        """
        self._uniques.append(
            [(column if isinstance(column, Column) else Column(column)) for column in columns]
        )

    @builder
    def primary_key(self, *columns: str | Column) -> Self:  # type:ignore[return]
        """
        Adds a primary key constraint.

        :param columns:
            Type:  str | tuple[str, str] | Column

            A list of columns.

        :raises AttributeError:
            If the primary key is already defined.

        :return:
            CreateQueryBuilder.
        """
        if self._primary_key:
            raise AttributeError("'Query' object already has attribute primary_key")
        self._primary_key = [
            (column if isinstance(column, Column) else Column(column)) for column in columns
        ]

    @builder
    def as_select(self, query_builder: QueryBuilder) -> Self:  # type:ignore[return]
        """
        Creates the table from a select statement.

        :param query_builder:
            The query.

        :raises AttributeError:
            If columns have been defined for the table.

        :return:
            CreateQueryBuilder.
        """
        if self._columns:
            raise AttributeError("'Query' object already has attribute columns")

        if not isinstance(query_builder, QueryBuilder):
            raise TypeError("Expected 'item' to be instance of QueryBuilder")

        self._as_select = query_builder

    @builder
    def if_not_exists(self) -> Self:  # type:ignore[return]
        self._if_not_exists = True

    def get_sql(self, ctx: SqlContext | None) -> str:
        """
        Gets the sql statement string.

        :return: The create table statement.
        :rtype: str
        """
        ctx = ctx or self.QUERY_CLS.SQL_CONTEXT

        if not self._create_table:
            return ""

        if not self._columns and not self._as_select:
            return ""

        create_table = self._create_table_sql(ctx)

        if self._as_select:
            return create_table + self._as_select_sql(ctx)

        body = self._body_sql(ctx)
        table_options = self._table_options_sql(ctx)

        return "{create_table} ({body}){table_options}".format(
            create_table=create_table, body=body, table_options=table_options
        )

    def _create_table_sql(self, ctx: SqlContext) -> str:
        table_type = ""
        if self._temporary:
            table_type = "TEMPORARY "
        elif self._unlogged:
            table_type = "UNLOGGED "

        if_not_exists = ""
        if self._if_not_exists:
            if_not_exists = "IF NOT EXISTS "

        return "CREATE {table_type}TABLE {if_not_exists}{table}".format(
            table_type=table_type,
            if_not_exists=if_not_exists,
            table=self._create_table.get_sql(ctx),  # type: ignore
        )

    def _table_options_sql(self, ctx: SqlContext) -> str:
        table_options = ""

        if self._with_system_versioning:
            table_options += " WITH SYSTEM VERSIONING"

        return table_options

    def _column_clauses(self, ctx: SqlContext) -> list[str]:
        return [column.get_sql(ctx) for column in self._columns]

    def _period_for_clauses(self, ctx: SqlContext) -> list[str]:
        return [period_for.get_sql(ctx) for period_for in self._period_fors]

    def _unique_key_clauses(self, ctx: SqlContext) -> list[str]:
        return [
            "UNIQUE ({unique})".format(
                unique=",".join(column.get_name_sql(ctx) for column in unique)
            )
            for unique in self._uniques
        ]

    def _primary_key_clause(self, ctx: SqlContext) -> str:
        columns = ",".join(
            column.get_name_sql(ctx)
            for column in self._primary_key  # type:ignore[union-attr]
        )
        return f"PRIMARY KEY ({columns})"

    def _body_sql(self, ctx: SqlContext) -> str:
        clauses = self._column_clauses(ctx)
        clauses += self._period_for_clauses(ctx)
        clauses += self._unique_key_clauses(ctx)

        # Primary keys
        if self._primary_key:
            clauses.append(self._primary_key_clause(ctx))

        return ",".join(clauses)

    def _as_select_sql(self, ctx: SqlContext) -> str:
        return " AS ({query})".format(
            query=self._as_select.get_sql(ctx),  # type:ignore[union-attr]
        )

    def __str__(self) -> str:
        return self.get_sql(self.QUERY_CLS.SQL_CONTEXT)

    def __repr__(self) -> str:
        return self.__str__()


class DropQueryBuilder:
    """
    Query builder used to build DROP queries.
    """

    SQL_CONTEXT = DEFAULT_SQL_CONTEXT
    QUERY_CLS = Query

    def __init__(self) -> None:
        self._drop_table: Table | None = None
        self._if_exists: bool | None = None

    def get_sql(self, ctx: SqlContext | None = None) -> str:
        ctx = ctx or self.SQL_CONTEXT

        if not self._drop_table:
            return ""

        querystring = self._drop_table_sql(ctx)

        return querystring

    @builder
    def drop_table(self, table: Table | str) -> Self:  # type:ignore[return]
        if self._drop_table:
            raise AttributeError("'Query' object already has attribute drop_table")

        self._drop_table = table if isinstance(table, Table) else Table(table)

    @builder
    def if_exists(self) -> Self:  # type:ignore[return]
        self._if_exists = True

    def _drop_table_sql(self, ctx: SqlContext) -> str:
        if_exists = "IF EXISTS " if self._if_exists else ""
        drop_table = cast(Table, self._drop_table)
        return "DROP TABLE {if_exists}{table}".format(
            if_exists=if_exists,
            table=drop_table.get_sql(ctx),
        )

    def __str__(self) -> str:
        return self.get_sql(self.QUERY_CLS.SQL_CONTEXT)

    def __repr__(self) -> str:
        return self.__str__()
