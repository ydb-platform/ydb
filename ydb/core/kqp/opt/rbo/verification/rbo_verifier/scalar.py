"""Nullable scalar and SQL three-valued Boolean semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from . import decimal, smt
from .ir import Expr
from .types import BOOL, DATE, MAX_DATE, VOID, family, integer_bounds, integer_width


def smt_sort(scalar_type: str) -> str:
    return smt.BOOL if family(scalar_type) in {"bool", "unit"} else smt.INT


def date_domain(value: smt.Term) -> smt.Term:
    return smt.and_(
        smt.not_(smt.lt(value, smt.ZERO)),
        smt.lt(value, smt.int_value(MAX_DATE)),
    )


def integer_domain(value: smt.Term, scalar_type: str) -> smt.Term:
    bounds = integer_bounds(scalar_type)
    if bounds is None:
        raise ValueError(f"not an integer type: {scalar_type!r}")
    lower, upper = bounds
    return smt.and_(
        smt.not_(smt.lt(value, smt.int_value(lower))),
        smt.lt(value, smt.int_value(upper)),
    )


@dataclass(frozen=True, slots=True)
class Value:
    type: str
    is_null: smt.Term
    value: smt.Term
    decimal_finite_abs_bound: int | None = None


@dataclass(frozen=True, slots=True)
class _OpaqueFunctions:
    is_null: smt.Function | None
    value: smt.Function


class Encoder:
    def __init__(self, script: smt.Script) -> None:
        self.script = script
        self._opaque: dict[tuple[object, ...], _OpaqueFunctions] = {}

    def evaluate(self, expression: Expr, row: Mapping[str, Value]) -> Value:
        if expression.kind == "column":
            assert expression.column is not None
            return row[expression.column]

        if expression.kind == "void":
            return Value(VOID, smt.FALSE, smt.FALSE)

        if expression.kind == "literal":
            assert expression.result_type is not None
            return Value(
                expression.result_type,
                smt.FALSE,
                self._literal(expression.result_type, expression.value),
            )

        if expression.kind == "null":
            assert expression.result_type is not None
            return self.null(expression.result_type)

        if expression.kind == "not":
            argument = self.evaluate(expression.args[0], row)
            return Value(BOOL, argument.is_null, smt.not_(argument.value))

        if expression.kind in {"and", "or"}:
            arguments = tuple(self.evaluate(argument, row) for argument in expression.args)
            return self._and(arguments) if expression.kind == "and" else self._or(arguments)

        if expression.kind == "in":
            lookup = self.evaluate(expression.args[0], row)
            items = tuple(self.evaluate(item, row) for item in expression.args[1:])
            comparisons = tuple(
                Value(
                    BOOL,
                    smt.or_(lookup.is_null, item.is_null),
                    smt.eq(lookup.value, item.value),
                )
                for item in items
            )
            return self._or(comparisons)

        if expression.kind in {"eq", "lt", "lte", "gt", "gte"}:
            left = self.evaluate(expression.args[0], row)
            right = self.evaluate(expression.args[1], row)
            return self._comparison(
                expression.kind,
                left,
                right,
                null_safe=expression.kind == "eq" and expression.null_safe,
            )

        if expression.kind in {"add", "sub", "mul"}:
            assert expression.result_type is not None
            left = self.evaluate(expression.args[0], row)
            right = self.evaluate(expression.args[1], row)
            is_null = smt.or_(left.is_null, right.is_null)
            if decimal.is_type(expression.result_type):
                if expression.kind == "add":
                    value = decimal.add(left.value, right.value, expression.result_type)
                elif expression.kind == "sub":
                    value = decimal.subtract(left.value, right.value, expression.result_type)
                else:
                    assert expression.kind == "mul"
                    value = decimal.multiply(
                        left.value,
                        right.value,
                        expression.result_type,
                        right.type,
                    )
                return Value(
                    expression.result_type,
                    is_null,
                    value,
                )
            if expression.kind == "add":
                raw = smt.add(left.value, right.value)
            elif expression.kind == "sub":
                raw = smt.sub(left.value, right.value)
            else:
                raw = smt.mul(left.value, right.value)
            return Value(
                expression.result_type,
                is_null,
                _wrap_integer(raw, expression.result_type),
            )

        if expression.kind == "opaque":
            return self._evaluate_opaque(expression, row)

        raise AssertionError(f"unknown expression kind {expression.kind!r}")

    def null(self, scalar_type: str) -> Value:
        return Value(scalar_type, smt.TRUE, _default(scalar_type))

    @staticmethod
    def is_true(value: Value) -> smt.Term:
        assert value.type == BOOL
        return smt.and_(smt.not_(value.is_null), value.value)

    @staticmethod
    def not_distinct(left: Value, right: Value) -> smt.Term:
        assert left.type == right.type
        return smt.or_(
            smt.and_(left.is_null, right.is_null),
            smt.and_(smt.not_(left.is_null), smt.not_(right.is_null), smt.eq(left.value, right.value)),
        )

    def _evaluate_opaque(self, expression: Expr, row: Mapping[str, Value]) -> Value:
        assert expression.fingerprint is not None
        assert expression.result_type is not None
        assert expression.nullable is not None
        arguments = tuple(self.evaluate(argument, row) for argument in expression.args)
        key = (
            expression.fingerprint,
            expression.result_type,
            expression.nullable,
            tuple(argument.type for argument in arguments),
        )
        functions = self._opaque.get(key)
        flat_sorts = tuple(
            sort
            for argument in arguments
            for sort in (smt.BOOL, smt_sort(argument.type))
        )
        if functions is None:
            functions = _OpaqueFunctions(
                is_null=(
                    self.script.fresh_function(
                        f"opaque_null:{expression.fingerprint}",
                        flat_sorts,
                        smt.BOOL,
                    )
                    if expression.nullable
                    else None
                ),
                value=self.script.fresh_function(
                    f"opaque_value:{expression.fingerprint}",
                    flat_sorts,
                    smt_sort(expression.result_type),
                ),
            )
            self._opaque[key] = functions
        flat_arguments = tuple(
            term
            for argument in arguments
            for term in (
                argument.is_null,
                smt.ite(argument.is_null, _default(argument.type), argument.value),
            )
        )
        result = Value(
            expression.result_type,
            functions.is_null(*flat_arguments) if functions.is_null is not None else smt.FALSE,
            functions.value(*flat_arguments),
        )
        if result.type == DATE:
            self.script.assert_(smt.or_(result.is_null, date_domain(result.value)))
        elif family(result.type) == "int":
            self.script.assert_(smt.or_(result.is_null, integer_domain(result.value, result.type)))
        elif decimal.is_type(result.type):
            self.script.assert_(smt.or_(result.is_null, decimal.domain(result.value, result.type)))
        return result

    def _literal(
        self,
        scalar_type: str,
        value: bool | int | str | decimal.Literal | None,
    ) -> smt.Term:
        if family(scalar_type) == "string":
            assert isinstance(value, str)
            return self.script.string_atom(value)
        if decimal.is_type(scalar_type):
            assert isinstance(value, decimal.Literal)
            return smt.int_value(decimal.literal_code(value, scalar_type))
        return _literal(scalar_type, value)

    @staticmethod
    def _comparison(kind: str, left: Value, right: Value, null_safe: bool) -> Value:
        decimal_operands = decimal.is_type(left.type) or decimal.is_type(right.type)
        left_value, right_value = (
            decimal.align(left.value, left.type, right.value, right.type)
            if decimal_operands
            else (left.value, right.value)
        )

        if null_safe:
            return Value(
                BOOL,
                smt.FALSE,
                smt.or_(
                    smt.and_(left.is_null, right.is_null),
                    smt.and_(
                        smt.not_(left.is_null),
                        smt.not_(right.is_null),
                        smt.eq(left_value, right_value),
                    ),
                ),
            )

        if decimal_operands:
            comparison = decimal.compare(kind, left_value, right_value)
        elif kind == "eq":
            comparison = smt.eq(left_value, right_value)
        elif kind == "lt":
            comparison = smt.lt(left_value, right_value)
        elif kind == "lte":
            comparison = smt.not_(smt.lt(right_value, left_value))
        elif kind == "gt":
            comparison = smt.lt(right_value, left_value)
        else:
            assert kind == "gte"
            comparison = smt.not_(smt.lt(left_value, right_value))
        return Value(BOOL, smt.or_(left.is_null, right.is_null), comparison)

    @staticmethod
    def _and(arguments: tuple[Value, ...]) -> Value:
        false = smt.or_(
            *(smt.and_(smt.not_(argument.is_null), smt.not_(argument.value)) for argument in arguments)
        )
        true = smt.and_(
            *(smt.and_(smt.not_(argument.is_null), argument.value) for argument in arguments)
        )
        return Value(BOOL, smt.not_(smt.or_(false, true)), true)

    @staticmethod
    def _or(arguments: tuple[Value, ...]) -> Value:
        true = smt.or_(
            *(smt.and_(smt.not_(argument.is_null), argument.value) for argument in arguments)
        )
        false = smt.and_(
            *(smt.and_(smt.not_(argument.is_null), smt.not_(argument.value)) for argument in arguments)
        )
        return Value(BOOL, smt.not_(smt.or_(true, false)), true)


def _literal(scalar_type: str, value: bool | int | str | None) -> smt.Term:
    scalar_family = family(scalar_type)
    if scalar_family == "bool":
        assert isinstance(value, bool)
        return smt.bool_value(value)
    if scalar_family in {"int", "date"}:
        assert isinstance(value, int) and not isinstance(value, bool)
        return smt.int_value(value)
    raise AssertionError(f"unknown scalar type {scalar_type!r}")


def _default(scalar_type: str) -> smt.Term:
    scalar_family = family(scalar_type)
    if scalar_family == "bool":
        return smt.FALSE
    if scalar_family in {"int", "date"}:
        return smt.ZERO
    if scalar_family in {"string", "atom"}:
        return smt.ZERO
    if scalar_family == "unit":
        return smt.FALSE
    raise AssertionError(f"unknown scalar type {scalar_type!r}")


def _wrap_integer(value: smt.Term, scalar_type: str) -> smt.Term:
    """Return the canonical value after fixed-width two's-complement wrap."""

    width = integer_width(scalar_type)
    assert width is not None
    modulus = 1 << width
    if scalar_type.startswith("Uint"):
        return smt.mod(value, modulus)
    sign = 1 << (width - 1)
    return smt.sub(
        smt.mod(smt.add(value, smt.int_value(sign)), modulus),
        smt.int_value(sign),
    )
