from __future__ import annotations

import inspect
import json
import re
import sys
import uuid
from collections.abc import Iterable, Iterator, Sequence
from datetime import date, time
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

from .context import DEFAULT_SQL_CONTEXT, SqlContext
from .enums import (
    Arithmetic,
    Boolean,
    Comparator,
    DatePart,
    Dialects,
    Equality,
    JSONOperators,
    Matching,
    Order,
)
from .exceptions import CaseException, FunctionException
from .utils import builder, format_alias_sql, format_quotes, ignore_copy, resolve_is_aggregate

if TYPE_CHECKING:
    from .queries import QueryBuilder, Selectable, Table

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

T = TypeVar("T")
NodeT = TypeVar("NodeT", bound="Node")


class Node:
    is_aggregate: bool | None = None

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]

    def find_(self, type: type[NodeT]) -> list[NodeT]:
        return [  # type:ignore[var-annotated]
            node for node in self.nodes_() if isinstance(node, type)
        ]


class Term(Node):
    is_aggregate: bool | None = False

    def __init__(self, alias: str | None = None) -> None:
        self.alias = alias

    @builder
    def as_(self, alias: str) -> Self:  # type:ignore[return]
        self.alias = alias

    @property
    def tables_(self) -> set[Table]:
        from . import Table

        return set(self.find_(Table))

    def fields_(self) -> set[Field]:
        return set(self.find_(Field))

    @staticmethod
    def wrap_constant(
        val: Any, wrapper_cls: type[Term] | None = None
    ) -> NodeT | LiteralValue | Array | Tuple | ValueWrapper:
        """
        Used for wrapping raw inputs such as numbers in Criterions and Operator.

        For example, the expression F('abc')+1 stores the integer part in a ValueWrapper object.

        :param val:
            Any value.
        :param wrapper_cls:
            A pypika class which wraps a constant value so it can be handled as a component of the query.
        :return:
            Raw string, number, or decimal values will be returned in a ValueWrapper.  Fields and other parts of the
            querybuilder will be returned as inputted.

        """

        if isinstance(val, Node):
            return cast(NodeT, val)
        if val is None:
            return NullValue()
        if isinstance(val, list):
            return Array(*val)
        if isinstance(val, tuple):
            return Tuple(*val)

        # Need to default here to avoid the recursion. ValueWrapper extends this class.
        wrapper_cls = wrapper_cls or ValueWrapper
        return wrapper_cls(val)  # type:ignore[return-value]

    @staticmethod
    def wrap_json(
        val: Term | QueryBuilder | str | int | bool | None,
        wrapper_cls: type[ValueWrapper] | None = None,
    ) -> Term | QueryBuilder | NullValue | ValueWrapper | JSON:
        from .queries import QueryBuilder

        if isinstance(val, (Term, QueryBuilder)):
            return val
        if val is None:
            return NullValue()
        if isinstance(val, (str, int, bool)):
            wrapper_cls = wrapper_cls or ValueWrapper
            return wrapper_cls(val)

        return JSON(val)

    def replace_table(self, current_table: Table | None, new_table: Table | None) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.
        The base implementation returns self because not all terms have a table property.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            Self.
        """
        return self

    def eq(self, other: Any) -> BasicCriterion:
        return self == other

    def isnull(self) -> NullCriterion:
        return NullCriterion(self)

    def notnull(self) -> Not:
        return self.isnull().negate()

    def bitwiseand(self, value: int) -> BitwiseAndCriterion:
        return BitwiseAndCriterion(self, self.wrap_constant(value))

    def gt(self, other: Any) -> BasicCriterion:
        return self > other

    def gte(self, other: Any) -> BasicCriterion:
        return self >= other

    def lt(self, other: Any) -> BasicCriterion:
        return self < other

    def lte(self, other: Any) -> BasicCriterion:
        return self <= other

    def ne(self, other: Any) -> BasicCriterion:
        return self != other

    def glob(self, expr: str) -> BasicCriterion:
        return BasicCriterion(Matching.glob, self, self.wrap_constant(expr))

    def like(self, expr: str) -> BasicCriterion:
        return BasicCriterion(Matching.like, self, self.wrap_constant(expr))

    def not_like(self, expr: str) -> BasicCriterion:
        return BasicCriterion(Matching.not_like, self, self.wrap_constant(expr))

    def ilike(self, expr: str) -> BasicCriterion:
        return BasicCriterion(Matching.ilike, self, self.wrap_constant(expr))

    def not_ilike(self, expr: str) -> BasicCriterion:
        return BasicCriterion(Matching.not_ilike, self, self.wrap_constant(expr))

    def rlike(self, expr: str) -> BasicCriterion:
        return BasicCriterion(Matching.rlike, self, self.wrap_constant(expr))

    def regex(self, pattern: str) -> BasicCriterion:
        return BasicCriterion(Matching.regex, self, self.wrap_constant(pattern))

    def between(self, lower: Any, upper: Any) -> BetweenCriterion:
        return BetweenCriterion(self, self.wrap_constant(lower), self.wrap_constant(upper))

    def from_to(self, start: Any, end: Any) -> PeriodCriterion:
        return PeriodCriterion(self, self.wrap_constant(start), self.wrap_constant(end))

    def as_of(self, expr: str) -> BasicCriterion:
        return BasicCriterion(Matching.as_of, self, self.wrap_constant(expr))

    def all_(self) -> All:
        return All(self)

    def isin(self, arg: list | tuple | set | Term) -> ContainsCriterion:
        if isinstance(arg, (list, tuple, set)):
            return ContainsCriterion(self, Tuple(*arg))
        return ContainsCriterion(self, arg)

    def notin(self, arg: list | tuple | set | Term) -> ContainsCriterion:
        return self.isin(arg).negate()

    def bin_regex(self, pattern: str) -> BasicCriterion:
        return BasicCriterion(Matching.bin_regex, self, self.wrap_constant(pattern))

    def negate(self) -> Not:
        return Not(self)

    def __invert__(self) -> Not:
        return Not(self)

    def __pos__(self) -> Self:
        return self

    def __neg__(self) -> Negative:
        return Negative(self)

    def __add__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.add, self, self.wrap_constant(other))

    def __sub__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.sub, self, self.wrap_constant(other))

    def __mul__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.mul, self, self.wrap_constant(other))

    def __truediv__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.div, self, self.wrap_constant(other))

    def __pow__(self, other: Any) -> Pow:
        return Pow(self, other)

    def __mod__(self, other: Any) -> Mod:
        return Mod(self, other)

    def __radd__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.add, self.wrap_constant(other), self)

    def __rsub__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.sub, self.wrap_constant(other), self)

    def __rmul__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.mul, self.wrap_constant(other), self)

    def __rtruediv__(self, other: Any) -> ArithmeticExpression:
        return ArithmeticExpression(Arithmetic.div, self.wrap_constant(other), self)

    def __eq__(self, other: Any) -> BasicCriterion:  # type:ignore[override]
        return BasicCriterion(Equality.eq, self, self.wrap_constant(other))

    def __ne__(self, other: Any) -> BasicCriterion:  # type:ignore[override]
        return BasicCriterion(Equality.ne, self, self.wrap_constant(other))

    def __gt__(self, other: Any) -> BasicCriterion:
        return BasicCriterion(Equality.gt, self, self.wrap_constant(other))

    def __ge__(self, other: Any) -> BasicCriterion:
        return BasicCriterion(Equality.gte, self, self.wrap_constant(other))

    def __lt__(self, other: Any) -> BasicCriterion:
        return BasicCriterion(Equality.lt, self, self.wrap_constant(other))

    def __le__(self, other: Any) -> BasicCriterion:
        return BasicCriterion(Equality.lte, self, self.wrap_constant(other))

    def __getitem__(self, item: slice) -> BetweenCriterion:
        if not isinstance(item, slice):
            raise TypeError("Field' object is not subscriptable")
        return self.between(item.start, item.stop)

    def __str__(self) -> str:
        return self.get_sql(DEFAULT_SQL_CONTEXT)

    def __hash__(self) -> int:
        ctx = DEFAULT_SQL_CONTEXT.copy(with_alias=True)
        return hash(self.get_sql(ctx))

    def get_sql(self, ctx: SqlContext) -> str:
        raise NotImplementedError()


class Parameter(Term):
    """
    Represents a parameter in a query. The placeholder can be specified with the `placeholder` argument or
    will be determined based on the dialect if not provided.
    """

    IDX_PLACEHOLDERS = {
        Dialects.ORACLE: lambda _: "?",
        Dialects.MSSQL: lambda _: "?",
        Dialects.MYSQL: lambda _: "%s",
        Dialects.POSTGRESQL: lambda idx: f"${idx}",
        Dialects.SQLITE: lambda _: "?",
    }
    DEFAULT_PLACEHOLDER = "?"
    is_aggregate = None

    def __init__(self, placeholder: str | None = None, idx: int | None = None) -> None:
        if not placeholder and idx is None:
            raise ValueError("Must provide either a placeholder or an idx")

        if idx is not None and idx < 1:
            raise ValueError("idx must start at 1")

        if placeholder and idx:
            raise ValueError("Cannot provide both a placeholder and an idx")

        self._placeholder = placeholder
        self._idx = idx

    def get_sql(self, ctx: SqlContext) -> str:
        if self._placeholder:
            return self._placeholder

        return self.IDX_PLACEHOLDERS.get(ctx.dialect, lambda _: self.DEFAULT_PLACEHOLDER)(self._idx)


class Parameterizer:
    """
    Parameterizer can be used to replace values with parameters in a query:
>>>>>>> 94fab2d (Replace ListParameter with Parameterizer)

        >>> parameterizer = Parameterizer()
        >>> customers = Table("customers")
        >>> sql = Query.from_(customers).select(customers.id)\
        ...             .where(customers.lname == "Mustermann")\
        ...             .get_sql(parameterizer=parameterizer, dialect=Dialects.SQLITE)
        >>> sql, parameterizer.values
        ('SELECT "id" FROM "customers" WHERE "lname"=?', ['Mustermann'])

    Parameterizer remembers the values it has seen and replaces them with parameters. The values can
    be accessed via the `values` attribute.
    """

    def __init__(self, placeholder_factory: Callable[[int], str] | None = None) -> None:
        self.placeholder_factory = placeholder_factory
        self.values: list = []

    def should_parameterize(self, value: Any) -> bool:
        if isinstance(value, Enum):
            return False

        if isinstance(value, str) and value == "*":
            return False
        return True

    def create_param(self, value: Any) -> Parameter:
        self.values.append(value)
        if self.placeholder_factory:
            return Parameter(self.placeholder_factory(len(self.values)))
        else:
            return Parameter(idx=len(self.values))


class Negative(Term):
    def __init__(self, term: Term) -> None:
        super().__init__()
        self.term = term

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        return self.term.is_aggregate

    def get_sql(self, ctx: SqlContext) -> str:
        return "-{term}".format(term=self.term.get_sql(ctx))


class ValueWrapper(Term):
    is_aggregate = None

    def __init__(
        self, value: Any, alias: str | None = None, allow_parametrize: bool = True
    ) -> None:
        """
        A wrapper for a constant value such as a string or number.

        :param value:
            The value to be wrapped.
        :param alias:
            An optional alias for the value.
        :param allow_parametrize:
            Whether the value should be replaced with a parameter in the query if parameterizer
            is used.
        """
        super().__init__(alias)
        self.value = value
        self.allow_parametrize = allow_parametrize

    def get_value_sql(self, ctx: SqlContext) -> str:
        return self.get_formatted_value(self.value, ctx)

    @classmethod
    def get_formatted_value(cls, value: Any, ctx: SqlContext) -> str:
        quote_char = ctx.secondary_quote_char or ""

        # FIXME escape values
        if isinstance(value, Term):
            return value.get_sql(ctx)
        if isinstance(value, Enum):
            if isinstance(value, DatePart):
                return value.value
            return cls.get_formatted_value(value.value, ctx)
        if isinstance(value, (date, time)):
            return cls.get_formatted_value(value.isoformat(), ctx)
        if isinstance(value, str):
            value = value.replace(quote_char, quote_char * 2)
            return format_quotes(value, quote_char)
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, uuid.UUID):
            return cls.get_formatted_value(str(value), ctx)
        if isinstance(value, (dict, list)):
            return format_quotes(json.dumps(value), quote_char)
        if value is None:
            return "null"
        return str(value)

    def get_sql(
        self,
        ctx: SqlContext,
    ) -> str:
        if (
            ctx.parameterizer is None
            or not ctx.parameterizer.should_parameterize(self.value)
            or not self.allow_parametrize
        ):
            sql = self.get_value_sql(ctx)
            return format_alias_sql(sql, self.alias, ctx)

        param = ctx.parameterizer.create_param(self.value)
        return format_alias_sql(param.get_sql(ctx), self.alias, ctx)


class JSON(Term):
    table: Table | None = None

    def __init__(self, value: Any = None, alias: str | None = None) -> None:
        super().__init__(alias)
        self.value = value

    def _recursive_get_sql(self, value: Any, **kwargs: Any) -> str:
        if isinstance(value, dict):
            return self._get_dict_sql(value, **kwargs)
        if isinstance(value, list):
            return self._get_list_sql(value, **kwargs)
        if isinstance(value, str):
            return self._get_str_sql(value, **kwargs)
        return str(value)

    def _get_dict_sql(self, value: dict, **kwargs: Any) -> str:
        pairs = [
            "{key}:{value}".format(
                key=self._recursive_get_sql(k, **kwargs),
                value=self._recursive_get_sql(v, **kwargs),
            )
            for k, v in value.items()
        ]
        return "".join(["{", ",".join(pairs), "}"])

    def _get_list_sql(self, value: list, **kwargs: Any) -> str:
        pairs = [self._recursive_get_sql(v, **kwargs) for v in value]
        return "".join(["[", ",".join(pairs), "]"])

    @staticmethod
    def _get_str_sql(value: str, quote_char: str = '"', **kwargs: Any) -> str:
        return format_quotes(value, quote_char)

    def get_sql(self, ctx: SqlContext) -> str:
        sql = format_quotes(self._recursive_get_sql(self.value), ctx.secondary_quote_char)
        return format_alias_sql(sql, self.alias, ctx)

    def get_json_value(self, key_or_index: str | int) -> BasicCriterion:
        return BasicCriterion(
            JSONOperators.GET_JSON_VALUE,
            self,
            self.wrap_constant(key_or_index),
        )

    def get_text_value(self, key_or_index: str | int) -> BasicCriterion:
        return BasicCriterion(
            JSONOperators.GET_TEXT_VALUE,
            self,
            self.wrap_constant(key_or_index),
        )

    def get_path_json_value(self, path_json: str) -> BasicCriterion:
        return BasicCriterion(
            JSONOperators.GET_PATH_JSON_VALUE,
            self,
            self.wrap_json(path_json),
        )

    def get_path_text_value(self, path_json: str) -> BasicCriterion:
        return BasicCriterion(
            JSONOperators.GET_PATH_TEXT_VALUE,
            self,
            self.wrap_json(path_json),
        )

    def has_key(self, other: Any) -> BasicCriterion:
        return BasicCriterion(
            JSONOperators.HAS_KEY,
            self,
            self.wrap_json(other),
        )

    def contains(self, other: Any) -> BasicCriterion:
        return BasicCriterion(
            JSONOperators.CONTAINS,
            self,
            self.wrap_json(other),
        )

    def contained_by(self, other: Any) -> BasicCriterion:
        return BasicCriterion(
            JSONOperators.CONTAINED_BY,
            self,
            self.wrap_json(other),
        )

    def has_keys(self, other: Iterable) -> BasicCriterion:
        return BasicCriterion(JSONOperators.HAS_KEYS, self, Array(*other))

    def has_any_keys(self, other: Iterable) -> BasicCriterion:
        return BasicCriterion(JSONOperators.HAS_ANY_KEYS, self, Array(*other))


class Values(Term):
    def __init__(self, field: str | Field) -> None:
        super().__init__(None)
        self.field = Field(field) if not isinstance(field, Field) else field

    def get_sql(self, ctx: SqlContext) -> str:
        return "VALUES({value})".format(value=self.field.get_sql(ctx))


class LiteralValue(Term):
    def __init__(self, value, alias: str | None = None) -> None:
        super().__init__(alias)
        self._value = value

    def get_sql(self, ctx: SqlContext) -> str:
        return format_alias_sql(self._value, self.alias, ctx)


class NullValue(LiteralValue):
    def __init__(self, alias: str | None = None) -> None:
        super().__init__("NULL", alias)


class SystemTimeValue(LiteralValue):
    def __init__(self, alias: str | None = None) -> None:
        super().__init__("SYSTEM_TIME", alias)


class Criterion(Term):
    def __and__(self, other: Any) -> ComplexCriterion:
        return ComplexCriterion(Boolean.and_, self, other)

    def __or__(self, other: Any) -> ComplexCriterion:
        return ComplexCriterion(Boolean.or_, self, other)

    def __xor__(self, other: Any) -> ComplexCriterion:
        return ComplexCriterion(Boolean.xor_, self, other)

    @staticmethod
    def any(terms: Iterable[Term] = ()) -> EmptyCriterion:
        crit = EmptyCriterion()

        for term in terms:
            crit |= term  # type:ignore[assignment]

        return crit

    @staticmethod
    def all(terms: Iterable[Any] = ()) -> EmptyCriterion:
        crit = EmptyCriterion()

        for term in terms:
            crit &= term

        return crit

    def get_sql(self, ctx: SqlContext) -> str:
        raise NotImplementedError()


class EmptyCriterion:
    is_aggregate: bool | None = None
    tables_: set[Table] = set()

    def fields_(self) -> set[Field]:
        return set()

    def __and__(self, other: T) -> T:
        return other

    def __or__(self, other: T) -> T:
        return other

    def __xor__(self, other: T) -> T:
        return other


class Field(Criterion, JSON):
    def __init__(
        self,
        name: str,
        alias: str | None = None,
        table: str | Selectable | None = None,
    ) -> None:
        super().__init__(alias=alias)
        self.name = name
        self.table = table  # type:ignore[assignment]

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        if self.table is not None:
            yield from self.table.nodes_()

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the field with the tables replaced.
        """
        if self.table == current_table:
            self.table = new_table

    def get_sql(self, ctx: SqlContext) -> str:
        field_sql = format_quotes(self.name, ctx.quote_char)

        # Need to add namespace if the table has an alias
        if self.table and (ctx.with_namespace or self.table.alias):
            table_name = self.table.get_table_name()
            field_sql = "{namespace}.{name}".format(
                namespace=format_quotes(table_name, ctx.quote_char),
                name=field_sql,
            )

        field_alias = getattr(self, "alias", None)
        if ctx.with_alias:
            return format_alias_sql(field_sql, field_alias, ctx)
        return field_sql


class Index(Term):
    def __init__(self, name: str, alias: str | None = None) -> None:
        super().__init__(alias)
        self.name = name

    def get_sql(self, ctx: SqlContext) -> str:
        return format_quotes(self.name, ctx.quote_char)


class Star(Field):
    def __init__(self, table: str | Selectable | None = None) -> None:
        super().__init__("*", table=table)

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        if self.table is not None:
            yield from self.table.nodes_()

    def get_sql(self, ctx: SqlContext) -> str:
        if self.table and (ctx.with_namespace or self.table.alias):
            namespace = self.table.alias or getattr(self.table, "_table_name")
            return "{}.*".format(format_quotes(namespace, ctx.quote_char))

        return "*"


class Tuple(Criterion):
    def __init__(self, *values: Any) -> None:
        super().__init__()
        self.values = [self.wrap_constant(value) for value in values]

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        for value in self.values:
            yield from value.nodes_()

    def get_sql(self, ctx: SqlContext) -> str:
        sql = "({})".format(",".join(term.get_sql(ctx) for term in self.values))
        return format_alias_sql(sql, self.alias, ctx)

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        return resolve_is_aggregate([val.is_aggregate for val in self.values])

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the field with the tables replaced.
        """
        self.values = [value.replace_table(current_table, new_table) for value in self.values]


class Array(Tuple):
    def __init__(self, *values: Any) -> None:
        super().__init__(*values)
        self.original_value = list(values)

    def get_sql(self, ctx: SqlContext) -> str:
        if ctx.parameterizer is None or not ctx.parameterizer.should_parameterize(
            self.original_value
        ):
            values = ",".join(term.get_sql(ctx) for term in self.values)

            sql = "[{}]".format(values)
            if ctx.dialect in (Dialects.POSTGRESQL, Dialects.REDSHIFT):
                sql = "ARRAY[{}]".format(values) if len(values) > 0 else "'{}'"

            return format_alias_sql(sql, self.alias, ctx)

        param = ctx.parameterizer.create_param(self.original_value)
        return param.get_sql(ctx)


class Bracket(Tuple):
    def __init__(self, term: Any) -> None:
        super().__init__(term)


class NestedCriterion(Criterion):
    def __init__(
        self,
        comparator: Comparator,
        nested_comparator: ComplexCriterion,
        left: Any,
        right: Any,
        nested: Any,
        alias: str | None = None,
    ) -> None:
        super().__init__(alias)
        self.left = left
        self.comparator = comparator
        self.nested_comparator = nested_comparator
        self.right = right
        self.nested = nested

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.right.nodes_()
        yield from self.left.nodes_()
        yield from self.nested.nodes_()

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        return resolve_is_aggregate(
            [term.is_aggregate for term in [self.left, self.right, self.nested]]
        )

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.left = self.left.replace_table(current_table, new_table)
        self.right = self.right.replace_table(current_table, new_table)
        self.nested = self.right.replace_table(current_table, new_table)

    def get_sql(self, ctx: SqlContext) -> str:
        sql = "{left}{comparator}{right}{nested_comparator}{nested}".format(
            left=self.left.get_sql(ctx),
            comparator=self.comparator.value,
            right=self.right.get_sql(ctx),
            nested_comparator=self.nested_comparator.value,  # type:ignore[attr-defined]
            nested=self.nested.get_sql(ctx),
        )

        if ctx.with_alias:
            return format_alias_sql(sql=sql, alias=self.alias, ctx=ctx)

        return sql


class BasicCriterion(Criterion):
    def __init__(
        self,
        comparator: Comparator | JSONOperators | Enum,
        left: Term,
        right: Term,
        alias: str | None = None,
    ) -> None:
        """
        A wrapper for a basic criterion such as equality or inequality. This wraps three parts, a left and right term
        and a comparator which defines the type of comparison.


        :param comparator:
            Type: Comparator
            This defines the type of comparison, such as {quote}={quote} or {quote}>{quote}.
        :param left:
            The term on the left side of the expression.
        :param right:
            The term on the right side of the expression.
        """
        super().__init__(alias)
        self.comparator = comparator
        self.left = left
        self.right = right

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.right.nodes_()
        yield from self.left.nodes_()

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        aggrs = [term.is_aggregate for term in (self.left, self.right)]
        return resolve_is_aggregate(aggrs)

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.left = self.left.replace_table(current_table, new_table)
        self.right = self.right.replace_table(current_table, new_table)

    def get_sql(self, ctx: SqlContext) -> str:
        sql = "{left}{comparator}{right}".format(
            comparator=self.comparator.value,
            left=self.left.get_sql(ctx),
            right=self.right.get_sql(ctx),
        )
        if ctx.with_alias:
            return format_alias_sql(sql, self.alias, ctx)
        return sql


class JSONAttributeCriterion(Criterion):
    """
    A specialized criterion for accessing JSON column attributes/paths.

    Generates database-specific SQL for accessing nested JSON attributes:
    - PostgreSQL: data->'user'->'details'->>'age' or data->'tags'->>0
    - MySQL and SQLite: data->>'$.user.details.age' or data->>'$.tags[0]'
    - MS SQL: JSON_VALUE(data, '$.user.details.age') or JSON_VALUE(data, '$.tags[0]')
    """

    def __init__(self, json_column: Term, path: list[str | int], alias: str | None = None) -> None:
        """
        Initialize a JSON attribute access criterion.

        :param json_column: The JSON column/field to access
        :param path: The path to the attribute as a list of keys/indices
        :param alias: Optional alias for the expression
        """
        super().__init__(alias)

        self.json_column = json_column
        self.path = path

    def get_sql(self, ctx: SqlContext) -> str:
        """Generate database-specific SQL for JSON attribute access."""
        if ctx.dialect == Dialects.MYSQL or ctx.dialect == Dialects.SQLITE:
            sql = self._get_mysql_sql(ctx)
        elif ctx.dialect == Dialects.MSSQL:
            sql = self._get_mssql_sql(ctx)
        else:
            # Fallback to PostgreSQL syntax for unsupported dialects
            sql = self._get_postgres_sql(ctx)

        if ctx.with_alias:
            return format_alias_sql(sql, self.alias, ctx)
        return sql

    def _get_postgres_sql(self, ctx: SqlContext) -> str:
        # PostgreSQL: data->'user'->'details'->>'age' or data->'tags'->>0
        sql = self.json_column.get_sql(ctx)
        for i, part in enumerate(self.path):
            is_last = i == len(self.path) - 1
            operator = "->>" if is_last else "->"

            if isinstance(part, int):
                sql += f"{operator}{part}"
            else:
                sql += f"{operator}'{part}'"
        return sql

    def _get_mysql_sql(self, ctx: SqlContext) -> str:
        # MySQL: data->'$.user.details.age' or data->'$.tags[0]'
        path_parts = []
        for part in self.path:
            path_parts.append(f"[{part}]" if isinstance(part, int) else f".{part}")
        path_str = "$" + "".join(path_parts)
        return f"{self.json_column.get_sql(ctx)}->>'{path_str}'"

    def _get_mssql_sql(self, ctx: SqlContext) -> str:
        # MS SQL: JSON_VALUE(data, '$.user.details.age') or JSON_VALUE(data, '$.tags[0]')
        path_parts = []
        for part in self.path:
            path_parts.append(f"[{part}]" if isinstance(part, int) else f".{part}")
        path_str = "$" + "".join(path_parts)
        return f"JSON_VALUE({self.json_column.get_sql(ctx)}, '{path_str}')"

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        self.json_column = self.json_column.replace_table(current_table, new_table)


class ContainsCriterion(Criterion):
    def __init__(self, term: Any, container: Term, alias: str | None = None) -> None:
        """
        A wrapper for a "IN" criterion.  This wraps two parts, a term and a container.  The term is the part of the
        expression that is checked for membership in the container.  The container can either be a list or a subquery.


        :param term:
            The term to assert membership for within the container.
        :param container:
            A list or subquery.
        """
        super().__init__(alias)
        self.term = term
        self.container = container
        self._is_negated = False

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.term.nodes_()
        yield from self.container.nodes_()

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        return self.term.is_aggregate

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, ctx: SqlContext) -> str:
        container_ctx = ctx.copy(subquery=True)
        sql = "{term} {not_}IN {container}".format(
            term=self.term.get_sql(ctx),
            container=self.container.get_sql(container_ctx),
            not_="NOT " if self._is_negated else "",
        )
        return format_alias_sql(sql, self.alias, ctx)

    @builder
    def negate(self) -> Self:  # type:ignore[return,override]
        self._is_negated = True


class RangeCriterion(Criterion):
    def __init__(self, term: Term, start: Any, end: Any, alias: str | None = None) -> None:
        super().__init__(alias)
        self.term = term
        self.start = start
        self.end = end

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.term.nodes_()
        yield from self.start.nodes_()
        yield from self.end.nodes_()

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        return self.term.is_aggregate


class BetweenCriterion(RangeCriterion):
    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, ctx: SqlContext) -> str:
        # FIXME escape
        sql = "{term} BETWEEN {start} AND {end}".format(
            term=self.term.get_sql(ctx),
            start=self.start.get_sql(ctx),
            end=self.end.get_sql(ctx),
        )
        return format_alias_sql(sql, self.alias, ctx)


class PeriodCriterion(RangeCriterion):
    def get_sql(self, ctx: SqlContext) -> str:
        sql = "{term} FROM {start} TO {end}".format(
            term=self.term.get_sql(ctx),
            start=self.start.get_sql(ctx),
            end=self.end.get_sql(ctx),
        )
        return format_alias_sql(sql, self.alias, ctx)


class BitwiseAndCriterion(Criterion):
    def __init__(self, term: Term, value: Any, alias: str | None = None) -> None:
        super().__init__(alias)
        self.term = term
        self.value = value

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.term.nodes_()
        yield from self.value.nodes_()

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, ctx: SqlContext) -> str:
        sql = "({term} & {value})".format(
            term=self.term.get_sql(ctx),
            value=self.value,
        )
        return format_alias_sql(sql, self.alias, ctx)


class NullCriterion(Criterion):
    def __init__(self, term: Term, alias: str | None = None) -> None:
        super().__init__(alias)
        self.term = term

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.term.nodes_()

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)

    def get_sql(self, ctx: SqlContext) -> str:
        sql = "{term} IS NULL".format(
            term=self.term.get_sql(ctx),
        )
        return format_alias_sql(sql, self.alias, ctx)


class ComplexCriterion(BasicCriterion):
    def get_sql(self, ctx: SqlContext) -> str:
        left_ctx = ctx.copy(subcriterion=self.needs_brackets(self.left))
        right_ctx = ctx.copy(subcriterion=self.needs_brackets(self.right))
        sql = "{left} {comparator} {right}".format(
            comparator=self.comparator.value,
            left=self.left.get_sql(left_ctx),
            right=self.right.get_sql(right_ctx),
        )

        if ctx.subcriterion:
            return "({criterion})".format(criterion=sql)

        return sql

    def needs_brackets(self, term: Term) -> bool:
        return isinstance(term, ComplexCriterion) and not term.comparator == self.comparator


class ArithmeticExpression(Term):
    """
    Wrapper for an arithmetic function.  Can be simple with two terms or complex with nested terms. Order of operations
    are also preserved.
    """

    add_order = [Arithmetic.add, Arithmetic.sub]

    def __init__(
        self, operator: Arithmetic, left: Any, right: Any, alias: str | None = None
    ) -> None:
        """
        Wrapper for an arithmetic expression.

        :param operator:
            Type: Arithmetic
            An operator for the expression such as {quote}+{quote} or {quote}/{quote}

        :param left:
            The term on the left side of the expression.
        :param right:
            The term on the right side of the expression.
        :param alias:
            (Optional) an alias for the term which can be used inside a select statement.
        :return:
        """
        super().__init__(alias)
        self.operator = operator
        self.left = left
        self.right = right

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.left.nodes_()
        yield from self.right.nodes_()

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        # True if both left and right terms are True or None. None if both terms are None. Otherwise, False
        return resolve_is_aggregate([self.left.is_aggregate, self.right.is_aggregate])

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the term with the tables replaced.
        """
        self.left = self.left.replace_table(current_table, new_table)
        self.right = self.right.replace_table(current_table, new_table)

    def left_needs_parens(self, curr_op: Arithmetic, left_op: Arithmetic | None) -> bool:
        """
        Returns true if the expression on the left of the current operator needs to be enclosed in parentheses.

        :param current_op:
            The current operator.
        :param left_op:
            The highest level operator of the left expression.
        """
        if left_op is None:
            # If the left expression is a single item.
            return False
        if curr_op in self.add_order:
            # If the current operator is '+' or '-'.
            return False
        # The current operator is '*' or '/'. If the left operator is '+' or '-', we need to add parentheses:
        # e.g. (A + B) / ..., (A - B) / ...
        # Otherwise, no parentheses are necessary:
        # e.g. A * B / ..., A / B / ...
        return left_op in self.add_order

    def right_needs_parens(self, curr_op: Arithmetic, right_op: Arithmetic | None) -> bool:
        """
        Returns true if the expression on the right of the current operator needs to be enclosed in parentheses.

        :param current_op:
            The current operator.
        :param right_op:
            The highest level operator of the right expression.
        """
        if right_op is None:
            # If the right expression is a single item.
            return False
        if curr_op == Arithmetic.add:
            return False
        if curr_op == Arithmetic.div:
            return True
        # The current operator is '*' or '-. If the right operator is '+' or '-', we need to add parentheses:
        # e.g. ... - (A + B), ... - (A - B)
        # Otherwise, no parentheses are necessary:
        # e.g. ... - A / B, ... - A * B
        return right_op in self.add_order

    def get_sql(self, ctx: SqlContext) -> str:
        left_op, right_op = [getattr(side, "operator", None) for side in [self.left, self.right]]

        arithmetic_sql = "{left}{operator}{right}".format(
            operator=self.operator.value,
            left=("({})" if self.left_needs_parens(self.operator, left_op) else "{}").format(
                self.left.get_sql(ctx)
            ),
            right=("({})" if self.right_needs_parens(self.operator, right_op) else "{}").format(
                self.right.get_sql(ctx)
            ),
        )

        if ctx.with_alias:
            return format_alias_sql(arithmetic_sql, self.alias, ctx)

        return arithmetic_sql


class Case(Term):
    def __init__(self, alias: str | None = None) -> None:
        super().__init__(alias=alias)
        self._cases: list = []
        self._else: Term | None = None

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]

        for criterion, term in self._cases:
            yield from criterion.nodes_()
            yield from term.nodes_()

        if self._else is not None:
            yield from self._else.nodes_()

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        # True if all criterions/cases are True or None. None all cases are None. Otherwise, False
        return resolve_is_aggregate(
            [criterion.is_aggregate or term.is_aggregate for criterion, term in self._cases]
            + [self._else.is_aggregate if self._else else None]
        )

    @builder
    def when(self, criterion: Any, term: Any) -> Self:  # type:ignore[return]
        self._cases.append((criterion, self.wrap_constant(term)))

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the term with the tables replaced.
        """
        self._cases = [
            [
                criterion.replace_table(current_table, new_table),
                term.replace_table(current_table, new_table),
            ]
            for criterion, term in self._cases
        ]
        self._else = self._else.replace_table(current_table, new_table) if self._else else None

    @builder
    def else_(self, term: Any) -> Self:
        self._else = self.wrap_constant(term)
        return self

    def get_sql(self, ctx: SqlContext) -> str:
        if not self._cases:
            raise CaseException("At least one 'when' case is required for a CASE statement.")

        when_then_else_ctx = ctx.copy(with_alias=False)
        cases = " ".join(
            "WHEN {when} THEN {then}".format(
                when=criterion.get_sql(when_then_else_ctx), then=term.get_sql(when_then_else_ctx)
            )
            for criterion, term in self._cases
        )
        else_ = " ELSE {}".format(self._else.get_sql(when_then_else_ctx)) if self._else else ""
        case_sql = "CASE {cases}{else_} END".format(cases=cases, else_=else_)

        if ctx.with_alias:
            return format_alias_sql(case_sql, self.alias, ctx)

        return case_sql


class Not(Criterion):
    def __init__(self, term: Any, alias: str | None = None) -> None:
        super().__init__(alias=alias)
        self.term = term

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.term.nodes_()

    def get_sql(self, ctx: SqlContext) -> str:
        not_ctx = ctx.copy(subcriterion=True)
        sql = "NOT {term}".format(term=self.term.get_sql(not_ctx))
        return format_alias_sql(sql, self.alias, ctx)

    @ignore_copy
    def __getattr__(self, name: str) -> Any:
        """
        Delegate method calls to the class wrapped by Not().
        Re-wrap methods on child classes of Term (e.g. isin, eg...) to retain 'NOT <term>' output.
        """
        item_func: Callable[..., T] = getattr(self.term, name)

        if not inspect.ismethod(item_func):
            return item_func

        def inner(inner_self: Any, *args: Any, **kwargs: Any) -> Not | T:
            result = item_func(inner_self, *args, **kwargs)
            if isinstance(result, (Term,)):
                return Not(result)
            return result

        return inner

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.term = self.term.replace_table(current_table, new_table)


class All(Criterion):
    def __init__(self, term: Any, alias: str | None = None) -> None:
        super().__init__(alias=alias)
        self.term = term

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        yield from self.term.nodes_()

    def get_sql(self, ctx: SqlContext) -> str:
        sql = "{term} ALL".format(term=self.term.get_sql(ctx))
        return format_alias_sql(sql, self.alias, ctx)


class CustomFunction:
    def __init__(self, name: str, params: Sequence | None = None) -> None:
        self.name = name
        self.params = params

    def __call__(self, *args: Any, **kwargs: Any) -> Function:
        if not self._has_params():
            return Function(self.name, alias=kwargs.get("alias"))

        if not self._is_valid_function_call(*args):
            raise FunctionException(
                "Function {name} require these arguments ({params}), ({args}) passed".format(
                    name=self.name,
                    params=", ".join(str(p) for p in cast(Sequence, self.params)),
                    args=", ".join(str(p) for p in args),
                )
            )

        return Function(self.name, *args, alias=kwargs.get("alias"))

    def _has_params(self) -> bool:
        return self.params is not None

    def _is_valid_function_call(self, *args: Any) -> bool:
        return len(args) == len(cast(Sequence, self.params))


class Function(Criterion):
    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(kwargs.get("alias"))
        self.name = name
        self.args: list = [self.wrap_constant(param) for param in args]
        self.schema = kwargs.get("schema")

    def nodes_(self) -> Iterator[NodeT]:
        yield self  # type:ignore[misc]
        for arg in self.args:
            yield from arg.nodes_()

    @property
    def is_aggregate(self) -> bool | None:  # type:ignore[override]
        """
        This is a shortcut that assumes if a function has a single argument and that argument is aggregated, then this
        function is also aggregated. A more sophisticated approach is needed, however it is unclear how that might work.
        :returns:
            True if the function accepts one argument and that argument is aggregate.
        """
        return resolve_is_aggregate([arg.is_aggregate for arg in self.args])

    @builder
    def replace_table(  # type:ignore[return]
        self, current_table: Table | None, new_table: Table | None
    ) -> Self:
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the criterion with the tables replaced.
        """
        self.args = [param.replace_table(current_table, new_table) for param in self.args]

    def get_special_params_sql(self, ctx: SqlContext) -> Any:
        pass

    @staticmethod
    def get_arg_sql(arg: Any, ctx: SqlContext) -> str:
        arg_ctx = ctx.copy(with_alias=False)
        return arg.get_sql(arg_ctx) if hasattr(arg, "get_sql") else str(arg)

    def get_dialect_special_name(self, dialect: Dialects) -> str:
        return ""

    def get_function_sql(self, ctx: SqlContext) -> str:
        # pylint: disable=E1111
        special_params_sql = self.get_special_params_sql(ctx)

        return "{name}({args}{special})".format(
            name=self.get_dialect_special_name(ctx.dialect) or self.name,
            args=",".join(self.get_arg_sql(arg, ctx) for arg in self.args),
            special=(" " + special_params_sql) if special_params_sql else "",
        )

    def get_sql(self, ctx: SqlContext) -> str:
        # FIXME escape
        function_sql = self.get_function_sql(ctx)

        if self.schema is not None:
            function_sql = "{schema}.{function}".format(
                schema=self.schema.get_sql(ctx),
                function=function_sql,
            )

        if ctx.with_alias:
            return format_alias_sql(function_sql, self.alias, ctx)

        return function_sql


class AggregateFunction(Function):
    is_aggregate = True

    def __init__(self, name, *args, **kwargs) -> None:
        super().__init__(name, *args, **kwargs)

        self._filters: list = []
        self._include_filter = False

    @builder
    def filter(self, *filters: Any) -> AnalyticFunction:  # type:ignore[return]
        self._include_filter = True
        self._filters += filters

    def get_filter_sql(self, ctx: SqlContext) -> str:  # type:ignore[return]
        if self._include_filter:
            criterions = Criterion.all(self._filters).get_sql(ctx)  # type:ignore[attr-defined]
            return f"WHERE {criterions}"
        # TODO: handle case of `not self._include_filter`

    def get_function_sql(self, ctx: SqlContext) -> str:
        sql = super().get_function_sql(ctx)
        filter_sql = self.get_filter_sql(ctx)

        if self._include_filter:
            sql += " FILTER({filter_sql})".format(filter_sql=filter_sql)

        return sql


class AnalyticFunction(AggregateFunction):
    is_aggregate = False
    is_analytic = True

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(name, *args, **kwargs)
        self._filters: list = []
        self._partition: list = []
        self._orderbys: list[tuple] = []
        self._include_filter = False
        self._include_over = False

    @builder
    def over(self, *terms: Any) -> Self:  # type:ignore[return]
        self._include_over = True
        self._partition += terms

    @builder
    def orderby(self, *terms: Any, **kwargs: Any) -> Self:  # type:ignore[return]
        self._include_over = True
        self._orderbys += [(term, kwargs.get("order")) for term in terms]

    def _orderby_field(self, field: Field, orient: Order | None, ctx: SqlContext) -> str:
        if orient is None:
            return field.get_sql(ctx)

        return "{field} {orient}".format(
            field=field.get_sql(ctx),
            orient=orient.value,
        )

    def get_partition_sql(self, ctx: SqlContext) -> str:
        terms = []
        if self._partition:
            terms.append(
                "PARTITION BY {args}".format(
                    args=",".join(
                        p.get_sql(ctx) if hasattr(p, "get_sql") else str(p) for p in self._partition
                    )
                )
            )

        if self._orderbys:
            terms.append(
                "ORDER BY {orderby}".format(
                    orderby=",".join(
                        self._orderby_field(field, orient, ctx) for field, orient in self._orderbys
                    )
                )
            )

        return " ".join(terms)

    def get_function_sql(self, ctx: SqlContext) -> str:
        function_sql = super().get_function_sql(ctx)
        partition_sql = self.get_partition_sql(ctx)

        sql = function_sql
        if self._include_over:
            sql += " OVER({partition_sql})".format(partition_sql=partition_sql)

        return sql


EdgeT = TypeVar("EdgeT", bound="WindowFrameAnalyticFunction.Edge")


class WindowFrameAnalyticFunction(AnalyticFunction):
    class Edge:
        def __init__(self, value: str | int | None = None) -> None:
            self.value = value

        def __str__(self) -> str:
            # pylint: disable=E1101
            return "{value} {modifier}".format(
                value=self.value or "UNBOUNDED",
                modifier=self.modifier,  # type:ignore[attr-defined]
            )

    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(name, *args, **kwargs)
        self.frame: str | None = None
        self.bound: Any = None

    def _set_frame_and_bounds(
        self, frame: str, bound: str | EdgeT, and_bound: EdgeT | None
    ) -> None:
        if self.frame or self.bound:
            raise AttributeError()

        self.frame = frame
        self.bound = (bound, and_bound) if and_bound else bound

    @builder
    def rows(  # type:ignore[return]
        self, bound: str | EdgeT, and_bound: EdgeT | None = None
    ) -> Self:
        self._set_frame_and_bounds("ROWS", bound, and_bound)

    @builder
    def range(  # type:ignore[return]
        self, bound: str | EdgeT, and_bound: EdgeT | None = None
    ) -> Self:
        self._set_frame_and_bounds("RANGE", bound, and_bound)

    def get_frame_sql(self) -> str:
        if not isinstance(self.bound, tuple):
            return "{frame} {bound}".format(frame=self.frame, bound=self.bound)

        lower, upper = self.bound
        return "{frame} BETWEEN {lower} AND {upper}".format(
            frame=self.frame,
            lower=lower,
            upper=upper,
        )

    def get_partition_sql(self, ctx: SqlContext) -> str:
        partition_sql = super().get_partition_sql(ctx)

        if not self.frame and not self.bound:
            return partition_sql

        return "{over} {frame}".format(over=partition_sql, frame=self.get_frame_sql())


class IgnoreNullsAnalyticFunction(AnalyticFunction):
    def __init__(self, name: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(name, *args, **kwargs)
        self._ignore_nulls = False

    @builder
    def ignore_nulls(self) -> Self:  # type:ignore[return]
        self._ignore_nulls = True

    def get_special_params_sql(self, ctx: SqlContext) -> str | None:
        if self._ignore_nulls:
            return "IGNORE NULLS"

        # No special params unless ignoring nulls
        return None


class Interval(Term):
    templates = {
        # PostgreSQL, Redshift and Vertica require quotes around the expr and unit e.g. INTERVAL '1 week'
        Dialects.POSTGRESQL: "INTERVAL '{expr} {unit}'",
        Dialects.REDSHIFT: "INTERVAL '{expr} {unit}'",
        Dialects.VERTICA: "INTERVAL '{expr} {unit}'",
        # Oracle and MySQL requires just single quotes around the expr
        Dialects.ORACLE: "INTERVAL '{expr}' {unit}",
        Dialects.MYSQL: "INTERVAL '{expr}' {unit}",
    }

    units = ["years", "months", "days", "hours", "minutes", "seconds", "microseconds"]
    labels = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND"]

    trim_pattern = re.compile(r"(^0+\.)|(\.0+$)|(^[0\-.: ]+[\-: ])|([\-:. ][0\-.: ]+$)")

    def __init__(
        self,
        years: int = 0,
        months: int = 0,
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        seconds: int = 0,
        microseconds: int = 0,
        quarters: int = 0,
        weeks: int = 0,
        dialect: Dialects | None = None,
    ) -> None:
        self.dialect = dialect
        self.largest = None
        self.smallest = None
        self.is_negative = False

        if quarters:
            self.quarters = quarters
            return

        if weeks:
            self.weeks = weeks
            return

        for unit, label, value in zip(
            self.units,
            self.labels,
            [years, months, days, hours, minutes, seconds, microseconds],
        ):
            if value:
                int_value = int(value)
                setattr(self, unit, abs(int_value))
                if self.largest is None:
                    self.largest = label
                    self.is_negative = int_value < 0
                self.smallest = label

    def __str__(self) -> str:
        return self.get_sql(DEFAULT_SQL_CONTEXT)

    def get_sql(self, ctx: SqlContext) -> str:
        if self.largest == "MICROSECOND":
            expr = getattr(self, "microseconds")
            unit = "MICROSECOND"

        elif hasattr(self, "quarters"):
            expr = getattr(self, "quarters")
            unit = "QUARTER"

        elif hasattr(self, "weeks"):
            expr = getattr(self, "weeks")
            unit = "WEEK"

        else:
            # Create the whole expression but trim out the unnecessary fields
            expr = "{years}-{months}-{days} {hours}:{minutes}:{seconds}.{microseconds}".format(
                years=getattr(self, "years", 0),
                months=getattr(self, "months", 0),
                days=getattr(self, "days", 0),
                hours=getattr(self, "hours", 0),
                minutes=getattr(self, "minutes", 0),
                seconds=getattr(self, "seconds", 0),
                microseconds=getattr(self, "microseconds", 0),
            )
            expr = self.trim_pattern.sub("", expr)
            if self.is_negative:
                expr = "-" + expr

            if self.largest != self.smallest:
                unit = f"{self.largest}_{self.smallest}"
            elif self.largest is None:
                # Set default unit with DAY
                unit = "DAY"
            else:
                unit = self.largest

        return self.templates.get(ctx.dialect, "INTERVAL '{expr} {unit}'").format(
            expr=expr, unit=unit
        )


class Pow(Function):
    def __init__(self, term: Term, exponent: float, alias: str | None = None) -> None:
        super().__init__("POW", term, exponent, alias=alias)

    def get_dialect_special_name(self, dialect: Dialects) -> str:
        return "POWER" if dialect == Dialects.MSSQL else ""


class Mod(Function):
    def __init__(self, term: Term, modulus: float, alias: str | None = None) -> None:
        super().__init__("MOD", term, modulus, alias=alias)


class Rollup(Function):
    def __init__(self, *terms: Any) -> None:
        super().__init__("ROLLUP", *terms)


class PseudoColumn(Term):
    """
    Represents a pseudo column (a "column" which yields a value when selected
    but is not actually a real table column).
    """

    def __init__(self, name: str) -> None:
        super().__init__(alias=None)
        self.name = name

    def get_sql(self, ctx: SqlContext) -> str:
        return self.name


class AtTimezone(Term):
    """
    Generates AT TIME ZONE SQL.
    Examples:
        AT TIME ZONE 'US/Eastern'
        AT TIME ZONE INTERVAL '-06:00'
    """

    is_aggregate: bool | None = None

    def __init__(
        self, field: str | Field, zone: str, interval: bool = False, alias: str | None = None
    ) -> None:
        super().__init__(alias)
        self.field = Field(field) if not isinstance(field, Field) else field
        self.zone = zone
        self.interval = interval

    def get_sql(self, ctx: SqlContext) -> str:
        sql = "{name} AT TIME ZONE {interval}'{zone}'".format(
            name=self.field.get_sql(ctx),
            interval="INTERVAL " if self.interval else "",
            zone=self.zone,
        )
        return format_alias_sql(sql, self.alias, ctx)
