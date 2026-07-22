"""Validate and bind one decoded verifier database for diagnostic tracing."""

from __future__ import annotations

from typing import Any, Mapping

from ..rbo_verifier import decimal, smt
from ..rbo_verifier.relation import WitnessCell
from ..rbo_verifier.types import MAX_DATE, family, integer_bounds
from ..rbo_verifier.verify import Problem
from .plan import InspectionError


MAX_WITNESS_STRING_BYTES = 1_000_000


class InvalidWitness(InspectionError):
    """A saved solver database cannot be bound to this obligation."""


def bind_witness(
    problem: Problem,
    value: Any,
) -> dict[str, list[dict[str, Any]]]:
    """Constrain an inspector obligation to one decoded input database.

    A verifier witness intentionally omits absent slots and the payload under a
    SQL NULL. Base tables are bags, so present rows are placed densely in the
    first slots; remaining slots are fixed absent. Routing, opaque functions,
    and plan choices remain free for reconstructing an execution of those rows.
    """

    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise InvalidWitness("witness must be an object with string table identities")
    if set(value) != set(problem.witness):
        raise InvalidWitness("witness table identities do not exactly match the catalog")

    # Validate and copy everything before adding assertions. A rejected input
    # must leave a reusable Problem unchanged.
    normalized: dict[str, list[dict[str, Any]]] = {}
    for table, symbolic_rows in problem.witness.items():
        raw_rows = value[table]
        if not isinstance(raw_rows, list):
            raise InvalidWitness(f"witness rows for table {table!r} must be an array")
        if len(raw_rows) > len(symbolic_rows):
            raise InvalidWitness(
                f"witness rows for table {table!r} exceed the declared row bound"
            )

        checked_rows: list[dict[str, Any]] = []
        for slot, raw_row in enumerate(raw_rows):
            symbolic_row = symbolic_rows[slot]
            if not isinstance(raw_row, Mapping) or any(
                not isinstance(key, str) for key in raw_row
            ):
                raise InvalidWitness(
                    f"witness row {slot} for table {table!r} must be an object"
                )
            if set(raw_row) != set(symbolic_row.cells):
                raise InvalidWitness(
                    f"witness row {slot} columns for table {table!r} do not match its schema"
                )
            checked_rows.append({
                name: _validate_cell(table, slot, name, cell, raw_row[name])
                for name, cell in symbolic_row.cells.items()
            })
        normalized[table] = checked_rows

    for table, symbolic_rows in problem.witness.items():
        rows = normalized[table]
        for slot, symbolic_row in enumerate(symbolic_rows):
            if slot >= len(rows):
                problem.script.assert_(smt.not_(symbolic_row.present))
                continue
            problem.script.assert_(symbolic_row.present)
            for name, cell in symbolic_row.cells.items():
                value = rows[slot][name]
                if value is None:
                    problem.script.assert_(cell.is_null)
                else:
                    problem.script.assert_(smt.not_(cell.is_null))
                    problem.script.assert_(smt.eq(
                        cell.value,
                        _encode_value(problem.script, cell.type, value),
                    ))
    return normalized


def _validate_cell(
    table: str,
    slot: int,
    name: str,
    cell: WitnessCell,
    value: Any,
) -> Any:
    context = f"witness[{table!r}][{slot}][{name!r}]"
    if value is None:
        if cell.is_null == smt.FALSE:
            raise InvalidWitness(f"{context} is NULL for a non-nullable column")
        return None

    scalar_family = family(cell.type)
    if scalar_family == "bool":
        if type(value) is not bool:
            raise InvalidWitness(f"{context} is not a Bool")
    elif scalar_family == "int":
        bounds = integer_bounds(cell.type)
        assert bounds is not None
        if type(value) is not int or not bounds[0] <= value < bounds[1]:
            raise InvalidWitness(f"{context} is outside {cell.type}")
    elif scalar_family == "date":
        if type(value) is not int or not 0 <= value < MAX_DATE:
            raise InvalidWitness(f"{context} is outside Date")
    elif scalar_family == "string":
        if not isinstance(value, str):
            raise InvalidWitness(f"{context} is not text")
        try:
            encoded = value.encode("utf-8", errors="strict")
        except UnicodeError as error:
            raise InvalidWitness(f"{context} is not valid Unicode") from error
        if len(encoded) > MAX_WITNESS_STRING_BYTES:
            raise InvalidWitness(f"{context} exceeds the witness cell-size audit cap")
    elif scalar_family == "atom" and decimal.is_type(cell.type):
        decimal_type = decimal.parse_type(cell.type)
        assert decimal_type is not None
        if type(value) is not int:
            raise InvalidWitness(f"{context} is outside {cell.type}")
        special = value in {-decimal.INF, decimal.INF, decimal.NAN}
        if not special and abs(value) >= 10**decimal_type.precision:
            raise InvalidWitness(f"{context} is outside {cell.type}")
    else:
        raise InvalidWitness(f"{context} has unsupported type {cell.type!r}")
    return value


def _encode_value(script: smt.Script, scalar_type: str, value: Any) -> smt.Term:
    scalar_family = family(scalar_type)
    if scalar_family == "bool":
        return smt.bool_value(value)
    if scalar_family == "string":
        return script.string_atom(value)
    return smt.int_value(value)
