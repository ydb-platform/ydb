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
        return self._evaluate(expression, row, ())

    def _evaluate(
        self,
        expression: Expr,
        row: Mapping[str, Value],
        bindings: tuple[Value, ...],
    ) -> Value:
        if expression.kind == "column":
            assert expression.column is not None
            return row[expression.column]

        if expression.kind == "bound":
            assert expression.depth is not None and 0 <= expression.depth < len(bindings)
            return bindings[expression.depth]

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
            argument = self._evaluate(expression.args[0], row, bindings)
            return Value(BOOL, argument.is_null, smt.not_(argument.value))

        if expression.kind == "exists":
            argument = self._evaluate(expression.args[0], row, bindings)
            return Value(BOOL, smt.FALSE, smt.not_(argument.is_null))

        if expression.kind in {"and", "or"}:
            arguments = tuple(
                self._evaluate(argument, row, bindings)
                for argument in expression.args
            )
            return self._and(arguments) if expression.kind == "and" else self._or(arguments)

        if expression.kind == "in":
            lookup = self._evaluate(expression.args[0], row, bindings)
            items = tuple(
                self._evaluate(item, row, bindings)
                for item in expression.args[1:]
            )
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
            left = self._evaluate(expression.args[0], row, bindings)
            right = self._evaluate(expression.args[1], row, bindings)
            return self._comparison(
                expression.kind,
                left,
                right,
                null_safe=expression.kind == "eq" and expression.null_safe,
            )

        if expression.kind in {"add", "sub", "mul", "div"}:
            assert expression.result_type is not None
            left = self._evaluate(expression.args[0], row, bindings)
            right = self._evaluate(expression.args[1], row, bindings)
            is_null = smt.or_(left.is_null, right.is_null)
            if decimal.is_type(expression.result_type):
                if expression.kind == "add":
                    value = decimal.add(left.value, right.value, expression.result_type)
                elif expression.kind == "sub":
                    value = decimal.subtract(left.value, right.value, expression.result_type)
                elif expression.kind == "mul":
                    value = decimal.multiply(
                        left.value,
                        right.value,
                        expression.result_type,
                        right.type,
                    )
                else:
                    assert expression.kind == "div"
                    value = decimal.divide(
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
            elif expression.kind == "mul":
                raw = smt.mul(left.value, right.value)
            else:
                raise AssertionError("integer division is not part of the semantic snapshot IR")
            return Value(
                expression.result_type,
                is_null,
                _wrap_integer(raw, expression.result_type),
            )

        if expression.kind == "cast_decimal":
            assert expression.result_type is not None
            argument = self._evaluate(expression.args[0], row, bindings)
            return Value(
                expression.result_type,
                smt.FALSE,
                decimal.cast_integral(
                    argument.value,
                    argument.type,
                    expression.result_type,
                ),
            )

        if expression.kind == "cast_integral":
            assert expression.result_type is not None and expression.nullable is True
            argument = self._evaluate(expression.args[0], row, bindings)
            assert family(argument.type) == "int"
            in_range = integer_domain(argument.value, expression.result_type)
            is_null = smt.or_(argument.is_null, smt.not_(in_range))
            return Value(
                expression.result_type,
                is_null,
                smt.ite(is_null, _default(expression.result_type), argument.value),
            )

        if expression.kind == "if":
            assert expression.result_type is not None
            condition = self._evaluate(expression.args[0], row, bindings)
            then = self._evaluate(expression.args[1], row, bindings)
            otherwise = self._evaluate(expression.args[2], row, bindings)
            assert condition.type == BOOL
            assert then.type == otherwise.type == expression.result_type
            bound = (
                _selected_decimal_finite_abs_bound(then, otherwise)
                if decimal.is_type(expression.result_type)
                else None
            )
            # MiniKQL propagates a NULL optional condition without selecting a
            # branch.  Both branch terms are nevertheless safe to build because
            # the closed-world exporter admits only deterministic, total trees.
            return Value(
                expression.result_type,
                smt.or_(
                    condition.is_null,
                    smt.ite(condition.value, then.is_null, otherwise.is_null),
                ),
                smt.ite(condition.value, then.value, otherwise.value),
                bound,
            )

        if expression.kind == "if_present":
            assert expression.result_type is not None
            optional = self._evaluate(expression.args[0], row, bindings)
            payload = Value(
                optional.type,
                smt.FALSE,
                optional.value,
                optional.decimal_finite_abs_bound,
            )
            present = self._evaluate(
                expression.args[1],
                row,
                (payload, *bindings),
            )
            missing = self._evaluate(expression.args[2], row, bindings)
            assert present.type == missing.type == expression.result_type
            bound = (
                _selected_decimal_finite_abs_bound(present, missing)
                if decimal.is_type(expression.result_type)
                else None
            )
            # The result is exactly one already-typed branch value.  Its domain
            # constraints and String rank therefore come from the branches;
            # registering the derived ite would only enlarge the String quotient.
            return Value(
                expression.result_type,
                smt.ite(optional.is_null, missing.is_null, present.is_null),
                smt.ite(optional.is_null, missing.value, present.value),
                bound,
            )

        if expression.kind == "opaque":
            return self._evaluate_opaque(expression, row, bindings)

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

    def _evaluate_opaque(
        self,
        expression: Expr,
        row: Mapping[str, Value],
        bindings: tuple[Value, ...],
    ) -> Value:
        assert expression.fingerprint is not None
        assert expression.result_type is not None
        assert expression.nullable is not None
        arguments = tuple(
            self._evaluate(argument, row, bindings)
            for argument in expression.args
        )
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
        if family(result.type) == "string":
            self.script.register_string_term(result.value)
        elif result.type == DATE:
            self.script.assert_global(smt.or_(result.is_null, date_domain(result.value)))
        elif family(result.type) == "int":
            self.script.assert_global(
                smt.or_(result.is_null, integer_domain(result.value, result.type))
            )
        elif decimal.is_type(result.type):
            self.script.assert_global(
                smt.or_(result.is_null, decimal.domain(result.value, result.type))
            )
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


def _selected_decimal_finite_abs_bound(
    present: Value,
    missing: Value,
) -> int | None:
    """Bound either branch only when both alternatives carry exact headroom."""

    if (
        present.decimal_finite_abs_bound is None
        or missing.decimal_finite_abs_bound is None
    ):
        return None
    return max(
        present.decimal_finite_abs_bound,
        missing.decimal_finite_abs_bound,
    )


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
