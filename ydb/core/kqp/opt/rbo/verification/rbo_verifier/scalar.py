"""Nullable scalar and SQL three-valued Boolean semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from . import smt
from .ir import Expr
from .types import BOOL, SCALAR_TYPES, family


SMT_SORT = {
    scalar_type: smt.BOOL if family(scalar_type) == "bool" else smt.INT
    for scalar_type in SCALAR_TYPES
}


@dataclass(frozen=True, slots=True)
class Value:
    type: str
    is_null: smt.Term
    value: smt.Term


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

        if expression.kind in {"eq", "lt", "lte", "gt", "gte"}:
            left = self.evaluate(expression.args[0], row)
            right = self.evaluate(expression.args[1], row)
            if expression.kind == "eq" and expression.null_safe:
                return Value(BOOL, smt.FALSE, self.not_distinct(left, right))
            if expression.kind == "eq":
                comparison = smt.eq(left.value, right.value)
            elif expression.kind == "lt":
                comparison = smt.lt(left.value, right.value)
            elif expression.kind == "lte":
                comparison = smt.not_(smt.lt(right.value, left.value))
            elif expression.kind == "gt":
                comparison = smt.lt(right.value, left.value)
            else:
                comparison = smt.not_(smt.lt(left.value, right.value))
            return Value(
                BOOL,
                smt.or_(left.is_null, right.is_null),
                comparison,
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
            for sort in (smt.BOOL, SMT_SORT[argument.type])
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
                    SMT_SORT[expression.result_type],
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
        return Value(
            expression.result_type,
            functions.is_null(*flat_arguments) if functions.is_null is not None else smt.FALSE,
            functions.value(*flat_arguments),
        )

    def _literal(self, scalar_type: str, value: bool | int | str | None) -> smt.Term:
        if family(scalar_type) == "string":
            assert isinstance(value, str)
            return self.script.string_atom(value)
        return _literal(scalar_type, value)

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
    if scalar_family == "int":
        assert isinstance(value, int) and not isinstance(value, bool)
        return smt.int_value(value)
    raise AssertionError(f"unknown scalar type {scalar_type!r}")


def _default(scalar_type: str) -> smt.Term:
    scalar_family = family(scalar_type)
    if scalar_family == "bool":
        return smt.FALSE
    if scalar_family == "int":
        return smt.ZERO
    if scalar_family == "string":
        return smt.ZERO
    raise AssertionError(f"unknown scalar type {scalar_type!r}")
