"""Concrete counterexample traces from the verifier's exact SAT obligation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from ..rbo_verifier import ir, smt
from ..rbo_verifier.relation import FamilyComparison, Outcome, RelationFamily
from ..rbo_verifier.scalar import (
    DecimalAverageState,
    IntegralAverageCertificate,
    IntegralAverageState,
    Value,
    average_metadata_terms,
)
from ..rbo_verifier.stages import TASKS
from ..rbo_verifier.types import family as type_family
from ..rbo_verifier.verify import (
    Problem,
    build_problem,
    decode_string_atom,
    decode_witness,
    query_solver,
    term_value,
)
from .plan import InspectionError, snapshot_digest
from .witness import bind_witness


TRACE_FORMAT = "ydb-rbo-concrete-trace"
TRACE_VERSION = 1
MAX_TRACE_PROBES = 100_000


@dataclass(frozen=True, slots=True)
class NodeEvent:
    scope: str
    node: str
    result: RelationFamily


@dataclass(frozen=True, slots=True)
class EdgeEvent:
    edge: ir.StageEdge
    consumer_task: int
    result: RelationFamily


@dataclass(slots=True)
class Execution:
    nodes: list[NodeEvent] = field(default_factory=list)
    edges: list[EdgeEvent] = field(default_factory=list)

    def node(self, scope: str, node: str, result: RelationFamily) -> None:
        self.nodes.append(NodeEvent(scope, node, result))

    def edge(self, edge: ir.StageEdge, task: int, result: RelationFamily) -> None:
        self.edges.append(EdgeEvent(edge, task, result))


@dataclass(slots=True)
class Observation:
    before: Execution = field(default_factory=Execution)
    after: Execution = field(default_factory=Execution)
    boundaries: dict[str, RelationFamily] = field(default_factory=dict)
    comparison: FamilyComparison | None = None

    def boundary(self, side: str, result: RelationFamily) -> None:
        if side in self.boundaries:
            raise InspectionError(f"duplicate {side!r} boundary observation")
        self.boundaries[side] = result

    def compared(self, comparison: FamilyComparison) -> None:
        if self.comparison is not None:
            raise InspectionError("duplicate family comparison observation")
        self.comparison = comparison


class Probes:
    """Named, equisatisfiable aliases for every concrete value we print."""

    def __init__(self, script: smt.Script) -> None:
        self.script = script
        self._terms: dict[int, smt.Term] = {}
        self._bound: dict[int, smt.Term] = {}
        self._requested: list[smt.Term] = []
        self._sealed = False

    @property
    def requested(self) -> tuple[smt.Term, ...]:
        self.seal()
        return tuple(self._requested)

    def add(self, term: smt.Term) -> None:
        if self._sealed:
            raise InspectionError("cannot add a concrete trace probe after sealing")
        identity = id(term)
        if identity in self._terms:
            return
        # Retaining the term keeps its identity stable until exact structural
        # compaction creates aliases in one iterative pass.
        self._terms[identity] = term

    def seal(self) -> None:
        if self._sealed:
            return
        items = tuple(self._terms.items())
        terms = tuple(term for _, term in items)
        structural = smt.structural_ids(terms)
        representatives: dict[int, smt.Term] = {}
        for structural_id, term in zip(structural, terms):
            representatives.setdefault(structural_id, term)
        if len(representatives) > MAX_TRACE_PROBES:
            raise InspectionError(
                f"concrete trace exceeds the {MAX_TRACE_PROBES} unique-probe audit bound"
            )

        bound_by_structure: dict[int, smt.Term] = {}
        for structural_id, term in representatives.items():
            if term.operation in {"bool", "int"}:
                bound = term
            elif term.operation == "symbol":
                bound = term
                self._requested.append(bound)
            else:
                bound = self.script.fresh_constant(
                    f"concrete_trace_probe:{len(bound_by_structure)}",
                    term.sort,
                )
                self.script.assert_term(smt.eq(bound, term))
                self._requested.append(bound)
            bound_by_structure[structural_id] = bound
        self._bound = {
            identity: bound_by_structure[structural_id]
            for (identity, _), structural_id in zip(items, structural)
        }
        self._sealed = True

    def value(
        self,
        term: smt.Term,
        values: Mapping[str, bool | int | str],
    ) -> bool | int | str:
        self.seal()
        try:
            return term_value(self._bound[id(term)], values)
        except KeyError as error:
            raise InspectionError("attempted to decode an unregistered trace term") from error


@dataclass(frozen=True, slots=True)
class PreparedInspection:
    before: ir.Snapshot
    after: ir.Snapshot
    row_bound: int
    problem: Problem
    observation: Observation
    probes: Probes
    fixed_witness: Mapping[str, list[dict[str, Any]]] | None = None

    def formula(self) -> str:
        return self.problem.formula()

    def solve(self, solver: str | Path, timeout_ms: int | None) -> dict[str, Any]:
        requested = tuple(
            dict.fromkeys(self.problem.witness_values() + self.probes.requested)
        )
        query = query_solver(self.problem, solver, requested, timeout_ms)
        common: dict[str, Any] = {
            "format": TRACE_FORMAT,
            "version": TRACE_VERSION,
            "row_bound": self.row_bound,
            "task_bound": TASKS,
            "inputs": {
                "before_semantic_sha256": snapshot_digest(self.before),
                "after_semantic_sha256": snapshot_digest(self.after),
            },
        }
        if query.status == "unsat":
            if self.fixed_witness is not None:
                return common | {
                    "status": "WITNESS_NOT_REPRODUCED",
                    "reason": "the saved verifier witness makes the counterexample obligation unsatisfiable",
                }
            return common | {"status": "VERIFIED_BOUNDED"}
        if query.status == "unknown":
            if query.phase == "model":
                return common | {
                    "status": "COUNTEREXAMPLE",
                    "trace_status": "UNKNOWN",
                    "reason": query.reason,
                }
            return common | {"status": "UNKNOWN", "reason": query.reason}
        if query.status != "sat":
            raise InspectionError(f"unexpected solver status {query.status!r}")

        comparison = self.observation.comparison
        before_boundary = self.observation.boundaries.get("before")
        after_boundary = self.observation.boundaries.get("after")
        if comparison is None or before_boundary is None or after_boundary is None:
            raise InspectionError("verification did not emit all boundary observations")
        literals = self.problem.script.string_literals
        mismatches = _mismatches(comparison, self.probes, query.values)
        if not mismatches:
            raise InspectionError("SAT model has no unmatched root outcome")
        witness = decode_witness(self.problem.witness, query.values, literals)
        if self.fixed_witness is not None and witness != self.fixed_witness:
            raise InspectionError("solver model differs from the fixed verifier witness")
        return common | {
            "status": "COUNTEREXAMPLE",
            "witness": witness,
            "mismatches": mismatches,
            "trace": {
                "before": _execution_json(
                    self.before,
                    self.observation.before,
                    before_boundary,
                    self.probes,
                    query.values,
                    literals,
                ),
                "after": _execution_json(
                    self.after,
                    self.observation.after,
                    after_boundary,
                    self.probes,
                    query.values,
                    literals,
                ),
                "comparison": {
                    "semantics": "sequence" if comparison.ordered else "bag",
                    "before": _family_json(
                        comparison.left, self.probes, query.values, literals
                    ),
                    "after": _family_json(
                        comparison.right, self.probes, query.values, literals
                    ),
                },
            },
        }


def prepare(
    before: ir.Snapshot,
    after: ir.Snapshot,
    row_bound: int,
    timeout_ms: int | None = None,
    fixed_witness: Any | None = None,
) -> PreparedInspection:
    """Build one obligation, observe it read-only, then add trace aliases."""

    observation = Observation()
    problem = build_problem(
        before,
        after,
        row_bound,
        timeout_ms,
        before_node_observer=observation.before.node,
        after_node_observer=observation.after.node,
        after_edge_observer=observation.after.edge,
        boundary_observer=observation.boundary,
        comparison_observer=observation.compared,
    )
    normalized_witness = (
        None if fixed_witness is None else bind_witness(problem, fixed_witness)
    )
    comparison = observation.comparison
    if comparison is None or set(observation.boundaries) != {"before", "after"}:
        raise InspectionError("verification did not emit all trace observations")

    probes = Probes(problem.script)
    for execution in (observation.before, observation.after):
        for event in execution.nodes:
            _add_family(probes, event.result)
        for event in execution.edges:
            _add_family(probes, event.result)
    for result in observation.boundaries.values():
        _add_family(probes, result)
    _add_family(probes, comparison.left)
    _add_family(probes, comparison.right)
    for row in comparison.pair_equal:
        for term in row:
            probes.add(term)
    probes.seal()
    return PreparedInspection(
        before,
        after,
        row_bound,
        problem,
        observation,
        probes,
        normalized_witness,
    )


def _add_family(probes: Probes, result: RelationFamily) -> None:
    for outcome in result.outcomes:
        probes.add(outcome.enabled)
        probes.add(outcome.error)
        for choice in outcome.choices:
            probes.add(choice.term)
        if outcome.relation.ordinals is not None:
            for ordinal in outcome.relation.ordinals:
                probes.add(ordinal)
        for row in outcome.relation.rows:
            probes.add(row.present)
            for value in row.values.values():
                probes.add(value.is_null)
                probes.add(value.value)
                state = value.average_metadata
                if state is not None:
                    for term in average_metadata_terms(state):
                        probes.add(term)


def _execution_json(
    snapshot: ir.Snapshot,
    execution: Execution,
    boundary: RelationFamily,
    probes: Probes,
    values: Mapping[str, bool | int | str],
    literals: Mapping[int, str],
) -> dict[str, Any]:
    nodes = snapshot.plan.node_map()
    return {
        "operators": [
            {
                "scope": _scope_json(event.scope),
                "node": event.node,
                "op": _node_kind(nodes[event.node]),
                "result": _family_json(event.result, probes, values, literals),
            }
            for event in execution.nodes
        ],
        "connections": [
            _edge_json(event.edge)
            | {
                "consumer_task": event.consumer_task,
                "result": _family_json(event.result, probes, values, literals),
            }
            for event in execution.edges
        ],
        "boundary": _family_json(boundary, probes, values, literals),
    }


def _family_json(
    result: RelationFamily,
    probes: Probes,
    values: Mapping[str, bool | int | str],
    literals: Mapping[int, str],
) -> dict[str, Any]:
    enabled = []
    disabled = 0
    for index, outcome in enumerate(result.outcomes):
        if not _outcome_enabled(outcome, probes, values):
            disabled += 1
            continue
        relation = outcome.relation
        indices = list(range(len(relation.rows)))
        if relation.ordinals is not None:
            present = [
                index
                for index in indices
                if probes.value(relation.rows[index].present, values) is True
            ]
            absent = [index for index in indices if index not in present]
            present.sort(
                key=lambda index: probes.value(
                    relation.ordinals[index],
                    values,
                )
            )
            indices = present + absent
        rows = []
        for slot, row_index in enumerate(indices):
            row = relation.rows[row_index]
            present = probes.value(row.present, values) is True
            rendered_row: dict[str, Any] = {"slot": slot, "present": present}
            if present:
                rendered_row["values"] = [
                    _cell_json(
                        column,
                        row.values[column.name],
                        probes,
                        values,
                        literals,
                    )
                    for column in relation.columns
                ]
            rows.append(rendered_row)
        enabled.append(
            {
                "index": index,
                "status": (
                    "error"
                    if probes.value(outcome.error, values) is True
                    else "success"
                ),
                "decisions": [
                    {"id": decision, "choice": choice}
                    for decision, choice in outcome.decisions
                ],
                "choices": _choices_json(outcome, probes, values),
                "sequence": relation.sequence,
                "order": _order_json(relation.order),
                "rows": rows,
            }
        )
    return {
        "columns": [
            {"name": column.name, "type": column.type, "nullable": column.nullable}
            for column in result.columns
        ],
        "disabled_outcome_count": disabled,
        "outcomes": enabled,
    }


def _outcome_enabled(
    outcome: Outcome,
    probes: Probes,
    values: Mapping[str, bool | int | str],
) -> bool:
    if probes.value(outcome.enabled, values) is not True:
        return False
    for choice in outcome.choices:
        value = probes.value(choice.term, values)
        if type(value) is not int or not 0 <= value < choice.bound:
            return False
    return True


def _cell_json(
    column: ir.Column,
    cell: Value,
    probes: Probes,
    values: Mapping[str, bool | int | str],
    literals: Mapping[int, str],
) -> dict[str, Any]:
    result = {
        "column": column.name,
        "type": column.type,
        "value": _cell_value(
            column.type,
            cell.is_null,
            cell.value,
            probes,
            values,
            literals,
        ),
    }
    if isinstance(cell.average_metadata, IntegralAverageCertificate):
        result["integral_average_certificate"] = (
            _integral_average_certificate_json(
                cell.is_null,
                cell.average_metadata,
                probes,
                values,
            )
        )
    elif cell.average_metadata is not None:
        result["average_state"] = _average_state_json(
            cell.is_null,
            cell.average_metadata,
            probes,
            values,
        )
    return result


def _average_state_json(
    is_null: smt.Term,
    state: (
        DecimalAverageState
        | IntegralAverageState
    ),
    probes: Probes,
    values: Mapping[str, bool | int | str],
) -> dict[str, Any]:
    optional_value = None
    if isinstance(state, DecimalAverageState):
        if probes.value(is_null, values) is not True:
            optional_value = {
                "sum": probes.value(state.sum, values),
                "count": probes.value(state.count, values),
            }
        return {
            "sum_type": state.sum_type,
            "count_type": "Uint64",
            "value": optional_value,
            "proof_bounds": {
                "finite_sum_abs": state.finite_abs_bound,
                "count": state.count_bound,
            },
        }

    if probes.value(is_null, values) is not True:
        optional_value = {
            "count": probes.value(state.count, values),
            "minimum": probes.value(state.minimum, values),
            "maximum": probes.value(state.maximum, values),
        }
    return {
        "kind": "integral_double_v1",
        "source_type": "Int64",
        "count_type": "Uint64",
        "value": optional_value,
        "proof_bounds": {
            "count": state.count_bound,
        },
    }


def _integral_average_certificate_json(
    is_null: smt.Term,
    certificate: IntegralAverageCertificate,
    probes: Probes,
    values: Mapping[str, bool | int | str],
) -> dict[str, Any]:
    return {
        "kind": "integral_double_v1",
        "count": (
            None
            if probes.value(is_null, values) is True
            else probes.value(certificate.count, values)
        ),
    }


def _cell_value(
    scalar_type: str,
    is_null: smt.Term,
    value: smt.Term,
    probes: Probes,
    values: Mapping[str, bool | int | str],
    literals: Mapping[int, str],
) -> bool | int | str | None:
    if probes.value(is_null, values) is True:
        return None
    raw = probes.value(value, values)
    return (
        decode_string_atom(raw, literals)
        if type_family(scalar_type) == "string"
        else raw
    )


def _mismatches(
    comparison: FamilyComparison,
    probes: Probes,
    values: Mapping[str, bool | int | str],
) -> list[dict[str, Any]]:
    left_enabled = tuple(
        probes.value(outcome.enabled, values) is True
        for outcome in comparison.left.outcomes
    )
    right_enabled = tuple(
        probes.value(outcome.enabled, values) is True
        for outcome in comparison.right.outcomes
    )
    result: list[dict[str, Any]] = []
    if not any(left_enabled):
        result.append({"source": "before", "reason": "no_enabled_outcomes"})
    if not any(right_enabled):
        result.append({"source": "after", "reason": "no_enabled_outcomes"})
    for index, enabled in enumerate(left_enabled):
        matches = [
            target
            for target, target_enabled in enumerate(right_enabled)
            if target_enabled
            and probes.value(comparison.pair_equal[index][target], values) is True
        ]
        if enabled and not matches:
            result.append(
                _unmatched(
                    "before",
                    index,
                    comparison.left,
                    probes,
                    values,
                )
            )
    for index, enabled in enumerate(right_enabled):
        matches = [
            source
            for source, source_enabled in enumerate(left_enabled)
            if source_enabled
            and probes.value(comparison.pair_equal[source][index], values) is True
        ]
        if enabled and not matches:
            result.append(
                _unmatched(
                    "after",
                    index,
                    comparison.right,
                    probes,
                    values,
                )
            )
    return result


def _unmatched(
    side: str,
    index: int,
    result: RelationFamily,
    probes: Probes,
    values: Mapping[str, bool | int | str],
) -> dict[str, Any]:
    outcome = result.outcomes[index]
    return {
        "source": side,
        "outcome": index,
        "decisions": [
            {"id": decision, "choice": choice}
            for decision, choice in outcome.decisions
        ],
        "choices": _choices_json(outcome, probes, values),
        "matching_outcomes": [],
    }


def _choices_json(
    outcome: Outcome,
    probes: Probes,
    values: Mapping[str, bool | int | str],
) -> list[dict[str, int]]:
    return [
        {
            "value": int(probes.value(choice.term, values)),
            "bound": choice.bound,
        }
        for choice in outcome.choices
    ]


def _scope_json(scope: str) -> dict[str, Any]:
    if scope in {"before:logical", "after:logical"}:
        return {"kind": "logical"}
    invocation, separator, row = scope.rpartition(":row:")
    if separator and row.isdigit():
        parent, marker, invocation_id = invocation.partition(
            ":correlated_scalar:"
        )
        binding, outcome_separator, outer_outcome = invocation_id.rpartition(
            ":outcome:"
        )
        if (
            marker
            and parent
            and binding
            and outcome_separator
            and outer_outcome.isdigit()
        ):
            return {
                "kind": "correlated_scalar_invocation",
                "parent": _scope_json(parent),
                "binding": binding,
                "outer_outcome": int(outer_outcome),
                "row": int(row),
            }
    if scope.startswith("stage:"):
        stage, separator, task = scope[len("stage:") :].rpartition(":task:")
        if separator and stage and task.isdigit():
            return {"kind": "stage_task", "stage": stage, "task": int(task)}
    raise InspectionError(f"unknown evaluator scope {scope!r}")


def _node_kind(node: ir.PlanNode) -> str:
    kinds = {
        ir.EmptySource: "empty_source",
        ir.Scan: "scan",
        ir.Project: "project",
        ir.Filter: "filter",
        ir.OuterBind: "outer_bind",
        ir.Limit: "limit",
        ir.Sort: "sort",
        ir.Aggregate: "aggregate",
        ir.Join: "join",
        ir.UnionAll: "union_all",
    }
    try:
        return kinds[type(node)]
    except KeyError as error:
        raise InspectionError(f"unknown plan node class {type(node).__name__!r}") from error


def _edge_json(edge: ir.StageEdge) -> dict[str, Any]:
    result: dict[str, Any] = {
        "edge": edge.id,
        "producer": edge.producer,
        "producer_output": edge.producer_output,
        "consumer": edge.consumer,
        "consumer_input": edge.consumer_input,
        "occurrence": edge.occurrence,
        "kind": edge.kind,
    }
    if edge.kind in {"map", "broadcast"}:
        return result
    if edge.kind == "hash_shuffle":
        return result | {
            "keys": list(edge.keys),
            "hash_function": edge.hash_function,
            "use_spilling": edge.use_spilling,
        }
    if edge.kind == "union_all":
        return result | {"parallel": edge.parallel}
    if edge.kind == "merge":
        return result | {"order": _order_json(edge.order)}
    raise InspectionError(f"unknown StageGraph connection kind {edge.kind!r}")


def _order_json(order: tuple[ir.SortOrder, ...] | None) -> list[dict[str, Any]] | None:
    if order is None:
        return None
    return [
        (
            {
                "column": item.column,
                "ascending": item.ascending,
                "nulls_first": item.nulls_first,
            }
            | (
                {}
                if item.comparison is None
                else {"comparison": item.comparison}
            )
        )
        for item in order
    ]
