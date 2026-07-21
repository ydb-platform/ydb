"""Bounded bag semantics for the version-one relational operators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from . import smt
from .ir import Column, EmptySource, Filter, Join, PlanNode, Project, Scan, Snapshot, UnionAll, validate_snapshot
from .scalar import Encoder as ScalarEncoder
from .scalar import SMT_SORT, Value


@dataclass(frozen=True, slots=True)
class Row:
    present: smt.Term
    values: Mapping[str, Value]


@dataclass(frozen=True, slots=True)
class Relation:
    columns: tuple[Column, ...]
    rows: tuple[Row, ...]


@dataclass(frozen=True, slots=True)
class WitnessCell:
    type: str
    is_null: smt.Term
    value: smt.Term


@dataclass(frozen=True, slots=True)
class WitnessRow:
    present: smt.Term
    cells: Mapping[str, WitnessCell]


class Database:
    """One shared symbolic catalog; self-joins reuse the same table rows."""

    def __init__(self, snapshot: Snapshot, row_bound: int, script: smt.Script) -> None:
        if row_bound < 0:
            raise ValueError("row bound must not be negative")
        self.relations: dict[str, Relation] = {}
        self.witness: dict[str, tuple[WitnessRow, ...]] = {}
        for table in snapshot.tables:
            rows: list[Row] = []
            witness_rows: list[WitnessRow] = []
            for slot in range(row_bound):
                present = script.fresh_constant(f"{table.name}_{slot}_present", smt.BOOL)
                values: dict[str, Value] = {}
                cells: dict[str, WitnessCell] = {}
                for column in table.columns:
                    is_null = (
                        script.fresh_constant(f"{table.name}_{slot}_{column.name}_null", smt.BOOL)
                        if column.nullable
                        else smt.FALSE
                    )
                    value = script.fresh_constant(
                        f"{table.name}_{slot}_{column.name}_value",
                        SMT_SORT[column.type],
                    )
                    values[column.name] = Value(column.type, is_null, value)
                    cells[column.name] = WitnessCell(column.type, is_null, value)
                rows.append(Row(present, values))
                witness_rows.append(WitnessRow(present, cells))
            self.relations[table.name] = Relation(table.columns, tuple(rows))
            self.witness[table.name] = tuple(witness_rows)
            for key in table.unique_keys:
                for left_index, left_row in enumerate(rows):
                    for right_row in rows[left_index + 1 :]:
                        equal_columns: list[smt.Term] = []
                        for column_name in key.columns:
                            left = left_row.values[column_name]
                            right = right_row.values[column_name]
                            if key.nulls_distinct:
                                equal_columns.append(
                                    smt.and_(
                                        smt.not_(left.is_null),
                                        smt.not_(right.is_null),
                                        smt.eq(left.value, right.value),
                                    )
                                )
                            else:
                                equal_columns.append(
                                    smt.or_(
                                        smt.and_(left.is_null, right.is_null),
                                        smt.and_(
                                            smt.not_(left.is_null),
                                            smt.not_(right.is_null),
                                            smt.eq(left.value, right.value),
                                        ),
                                    )
                                )
                        script.assert_(
                            smt.not_(
                                smt.and_(left_row.present, right_row.present, *equal_columns)
                            )
                        )


class Evaluator:
    def __init__(
        self,
        snapshot: Snapshot,
        database: Database,
        scalar: ScalarEncoder,
        edge_inputs: Mapping[tuple[str, int], Relation] | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.database = database
        self.scalar = scalar
        self.nodes = snapshot.plan.node_map()
        self.schemas = validate_snapshot(snapshot)
        self.cache: dict[str, Relation] = {}
        self.edge_inputs = edge_inputs or {}

    def root(self) -> Relation:
        relation = self.node(self.snapshot.plan.root)
        columns_by_name = {column.name: column for column in relation.columns}
        output = tuple(self.snapshot.plan.output)
        return Relation(
            columns=tuple(columns_by_name[name] for name in output),
            rows=tuple(Row(row.present, {name: row.values[name] for name in output}) for row in relation.rows),
        )

    def node(self, node_id: str) -> Relation:
        if node_id in self.cache:
            return self.cache[node_id]
        node = self.nodes[node_id]
        relation = self._evaluate(node)
        self.cache[node_id] = relation
        return relation

    def _evaluate(self, node: PlanNode) -> Relation:
        if isinstance(node, EmptySource):
            return Relation((), (Row(smt.TRUE, {}),))

        if isinstance(node, Scan):
            source = self.database.relations[node.table]
            source_columns = {column.name: column for column in source.columns}
            columns = tuple(
                Column(mapping.output, source_columns[mapping.source].type, source_columns[mapping.source].nullable)
                for mapping in node.columns
            )
            rows = tuple(
                Row(
                    row.present,
                    {mapping.output: row.values[mapping.source] for mapping in node.columns},
                )
                for row in source.rows
            )
            return Relation(columns, rows)

        if isinstance(node, Project):
            source = self._input(node.id, 0, node.input)
            columns = self._columns(node.id)
            return Relation(
                columns,
                tuple(
                    Row(
                        row.present,
                        {
                            projection.output: self.scalar.evaluate(projection.expression, row.values)
                            for projection in node.columns
                        },
                    )
                    for row in source.rows
                ),
            )

        if isinstance(node, Filter):
            source = self._input(node.id, 0, node.input)
            return Relation(
                source.columns,
                tuple(
                    Row(
                        smt.and_(row.present, self.scalar.is_true(self.scalar.evaluate(node.predicate, row.values))),
                        row.values,
                    )
                    for row in source.rows
                ),
            )

        if isinstance(node, Join):
            return self._join(
                node,
                self._input(node.id, 0, node.left),
                self._input(node.id, 1, node.right),
            )

        if isinstance(node, UnionAll):
            rows: list[Row] = []
            for index, item in enumerate(node.inputs):
                source = self._input(node.id, index, item.node)
                for row in source.rows:
                    rows.append(
                        Row(
                            row.present,
                            {
                                output: row.values[input_name]
                                for output, input_name in zip(node.output, item.columns)
                            },
                        )
                    )
            return Relation(self._columns(node.id), tuple(rows))

        raise AssertionError(f"unknown plan node {type(node).__name__}")

    def _input(self, parent: str, ordinal: int, child: str) -> Relation:
        key = (parent, ordinal)
        return self.edge_inputs[key] if key in self.edge_inputs else self.node(child)

    def _join(self, node: Join, left: Relation, right: Relation) -> Relation:
        matches: list[list[smt.Term]] = []
        for left_row in left.rows:
            match_row: list[smt.Term] = []
            for right_row in right.rows:
                values = dict(left_row.values) | dict(right_row.values)
                match_row.append(
                    smt.and_(
                        left_row.present,
                        right_row.present,
                        self.scalar.is_true(self.scalar.evaluate(node.predicate, values)),
                    )
                )
            matches.append(match_row)

        rows: list[Row] = []
        if node.kind not in {
            "left_semi",
            "right_semi",
            "left_anti",
            "right_anti",
            "exclusion",
        }:
            for left_index, left_row in enumerate(left.rows):
                for right_index, right_row in enumerate(right.rows):
                    rows.append(
                        Row(
                            matches[left_index][right_index],
                            dict(left_row.values) | dict(right_row.values),
                        )
                    )

        if node.kind in {"left", "full", "left_anti", "left_semi", "exclusion"}:
            right_nulls = {
                column.name: self.scalar.null(column.type)
                for column in right.columns
            }
            for index, left_row in enumerate(left.rows):
                matched = smt.or_(*matches[index])
                if node.kind == "left_semi":
                    present = smt.and_(left_row.present, matched)
                    values = left_row.values
                elif node.kind == "left_anti":
                    present = smt.and_(left_row.present, smt.not_(matched))
                    values = left_row.values
                else:
                    present = smt.and_(left_row.present, smt.not_(matched))
                    values = dict(left_row.values) | right_nulls
                rows.append(Row(present, values))

        if node.kind in {"right", "full", "right_anti", "right_semi", "exclusion"}:
            left_nulls = {
                column.name: self.scalar.null(column.type)
                for column in left.columns
            }
            for right_index, right_row in enumerate(right.rows):
                matched = smt.or_(*(matches[left_index][right_index] for left_index in range(len(left.rows))))
                if node.kind == "right_semi":
                    present = smt.and_(right_row.present, matched)
                    values = right_row.values
                elif node.kind == "right_anti":
                    present = smt.and_(right_row.present, smt.not_(matched))
                    values = right_row.values
                else:
                    present = smt.and_(right_row.present, smt.not_(matched))
                    values = left_nulls | dict(right_row.values)
                rows.append(Row(present, values))

        # Inner/cross joins with an empty side simply have no candidate rows.
        return Relation(self._columns(node.id), tuple(rows))

    def _columns(self, node_id: str) -> tuple[Column, ...]:
        return tuple(self.schemas[node_id].values())


def bag_equal(left: Relation, right: Relation, scalar: ScalarEncoder) -> smt.Term:
    if len(left.columns) != len(right.columns):
        return smt.FALSE
    if any(a.type != b.type for a, b in zip(left.columns, right.columns)):
        return smt.FALSE

    left_names = tuple(column.name for column in left.columns)
    right_names = tuple(column.name for column in right.columns)

    def row_equal(
        first: Row,
        first_names: tuple[str, ...],
        second: Row,
        second_names: tuple[str, ...],
    ) -> smt.Term:
        return smt.and_(
            *(
                scalar.not_distinct(first.values[first_name], second.values[second_name])
                for first_name, second_name in zip(first_names, second_names)
            )
        )

    def multiplicity(
        relation: Relation,
        names: tuple[str, ...],
        candidate: Row,
        candidate_names: tuple[str, ...],
    ) -> smt.Term:
        return smt.add(
            *(
                smt.ite(
                    smt.and_(row.present, row_equal(row, names, candidate, candidate_names)),
                    smt.ONE,
                    smt.ZERO,
                )
                for row in relation.rows
            )
        )

    equalities: list[smt.Term] = []
    for candidate, candidate_names in (
        *((row, left_names) for row in left.rows),
        *((row, right_names) for row in right.rows),
    ):
        counts_equal = smt.eq(
            multiplicity(left, left_names, candidate, candidate_names),
            multiplicity(right, right_names, candidate, candidate_names),
        )
        equalities.append(smt.or_(smt.not_(candidate.present), counts_equal))
    return smt.and_(*equalities)
