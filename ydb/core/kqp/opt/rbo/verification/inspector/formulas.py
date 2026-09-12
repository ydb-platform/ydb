"""Lossless term references for observed operators, without solving or semantics.

The DAG is the kernel's typed AST, not a second relational encoding. References
are export-local and preserve argument order and quantifier scope: for forall
and exists, all arguments except the last bind symbols in the last (body).
Integer atoms and proof bounds are strings so browsers cannot round them.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from ..rbo_verifier import floating, ir, smt
from ..rbo_verifier.relation import RelationFamily
from ..rbo_verifier.scalar import (
    DecimalAverageState, IntegralAverageCertificate, IntegralAverageState, Value,
)
from ..rbo_verifier.stages import TASKS
from ..rbo_verifier.verify import Problem, build_problem
from .plan import InspectionError
from .trace import Observation, _edge_json, _node_kind, _order_json, _scope_json


FORMULAS_FORMAT = "ydb-rbo-operator-formulas"
FORMULAS_VERSION = 1
MAX_FORMULA_TERMS = 200_000
_BUILTINS = frozenset({
    "symbol", "bool", "int", "not", "and", "or", "=", "distinct", "<", "ite",
    "forall", "exists", "+", "-", "*", "div", "mod",
})


class TermDag:
    """Iterative identity sharing; never expand a shared subtree into text."""

    def __init__(self, declarations: tuple[smt.DeclarationRecord, ...]) -> None:
        self.nodes: list[dict[str, Any]] = []
        self._ids: dict[int, str] = {}
        self._retained: list[smt.Term] = []
        self._operations = set(_BUILTINS)
        for declaration in declarations:
            if isinstance(declaration, (smt.Declaration, smt.DefinitionDeclaration)):
                self._operations.add(declaration.name)
            elif isinstance(declaration, smt.ProductDeclaration):
                self._operations.add(declaration.constructor.name)
                self._operations.update(selector.name for selector in declaration.selectors)
            else:
                raise InspectionError(f"unknown SMT declaration {type(declaration).__name__!r}")

    def ref(self, root: smt.Term) -> str:
        pending = [(root, False)]
        while pending:
            term, expanded = pending.pop()
            identity = id(term)
            if identity in self._ids:
                continue
            if not expanded:
                pending.append((term, True))
                pending.extend((child, False) for child in reversed(term.arguments))
                continue
            if len(self.nodes) >= MAX_FORMULA_TERMS:
                raise InspectionError(f"formula DAG exceeds the {MAX_FORMULA_TERMS} term export limit")
            if term.operation not in self._operations:
                raise InspectionError(f"unknown SMT operation {term.operation!r}")
            reference = f"t{len(self.nodes)}"
            node: dict[str, Any] = {
                "id": reference, "sort": term.sort, "op": term.operation,
                "args": [self._ids[id(child)] for child in term.arguments],
            }
            if term.operation in {"symbol", "int", "bool"}:
                expected = {"symbol": str, "int": int, "bool": bool}[term.operation]
                if type(term.atom) is not expected or term.arguments:
                    raise InspectionError(f"malformed SMT {term.operation} atom")
                node["atom"] = str(term.atom) if term.operation == "int" else term.atom
            # Application atoms are private script-ownership tokens, not SMT
            # syntax; declaration names identify their meaning in this export.
            self._ids[identity] = reference
            self._retained.append(term)
            self.nodes.append(node)
        return self._ids[id(root)]

    def declaration(self, declaration: smt.DeclarationRecord) -> dict[str, Any]:
        common = {"name": declaration.name, "hint": declaration.hint}
        if isinstance(declaration, smt.Declaration):
            return common | {
                "kind": "function", "arguments": list(declaration.arguments),
                "result": declaration.result,
            }
        if isinstance(declaration, smt.ProductDeclaration):
            return common | {
                "kind": "product", "constructor": declaration.constructor.name,
                "fields": [
                    {"selector": selector.name, "sort": sort}
                    for selector, sort in zip(declaration.selectors, declaration.fields)
                ],
            }
        if isinstance(declaration, smt.DefinitionDeclaration):
            return common | {
                "kind": "definition", "result": declaration.result,
                "parameters": [self.ref(term) for term in declaration.parameters],
                "body": self.ref(declaration.body),
            }
        raise InspectionError(f"unknown SMT declaration {type(declaration).__name__!r}")

    def family(self, family: RelationFamily) -> dict[str, Any]:
        return {
            "columns": [
                {"name": column.name, "type": column.type, "nullable": column.nullable}
                for column in family.columns
            ],
            "outcomes": [
                {
                    "index": index, "enabled": self.ref(outcome.enabled),
                    "error": self.ref(outcome.error),
                    "decisions": [{"id": key, "choice": value} for key, value in outcome.decisions],
                    "choices": [
                        {"term": self.ref(choice.term), "bound": choice.bound}
                        for choice in outcome.choices
                    ],
                    "sequence": outcome.relation.sequence,
                    "order": _order_json(outcome.relation.order),
                    "rows": [
                        {
                            "slot": slot, "present": self.ref(row.present),
                            "ordinal": (
                                None if outcome.relation.ordinals is None
                                else self.ref(outcome.relation.ordinals[slot])
                            ),
                            "values": [
                                {"column": column.name} | self.cell(row.values[column.name])
                                for column in outcome.relation.columns
                            ],
                        }
                        for slot, row in enumerate(outcome.relation.rows)
                    ],
                }
                for index, outcome in enumerate(family.outcomes)
            ],
        }

    def cell(self, value: Value) -> dict[str, Any]:
        metadata = []
        average = value.average_metadata
        if isinstance(average, DecimalAverageState):
            metadata.append({
                "kind": "decimal_average_state", "role": "physical_state",
                "sum_type": average.sum_type,
                "terms": {"sum": self.ref(average.sum), "count": self.ref(average.count)},
                "bounds": {"finite_abs": str(average.finite_abs_bound), "count": str(average.count_bound)},
            })
        elif isinstance(average, IntegralAverageState):
            metadata.append({
                "kind": "integral_average_summary", "role": "proof_metadata",
                "terms": {
                    "count": self.ref(average.count), "minimum": self.ref(average.minimum),
                    "maximum": self.ref(average.maximum),
                },
                "bounds": {"count": str(average.count_bound)},
            })
        elif isinstance(average, IntegralAverageCertificate):
            metadata.append({
                "kind": "integral_average_certificate", "role": "proof_metadata",
                "terms": {"count": self.ref(average.count)},
            })
        elif average is not None:
            raise InspectionError(f"unknown average metadata {type(average).__name__!r}")
        summary = value.decimal_sum_state
        if summary is not None:
            metadata.append({
                "kind": "decimal_sum_summary", "role": "proof_metadata", "sum_type": summary.sum_type,
                "terms": {name: self.ref(getattr(summary, name)) for name in (
                    "any_non_null", "has_nan", "has_pos_inf", "has_neg_inf", "finite_total",
                )},
                "bounds": {"finite_abs": str(summary.finite_abs_bound)},
            })
        state = value.binary64_state
        if isinstance(state, floating.VarianceState):
            metadata.append({
                "kind": "binary64_variance_v1", "role": "physical_state",
                "terms": {name: self.ref(getattr(state, name).bits) for name in ("mean", "count", "m2")},
            })
        elif state is not None:
            raise InspectionError(f"unknown binary64 state {type(state).__name__!r}")
        return {
            "type": value.type, "is_null": self.ref(value.is_null), "value": self.ref(value.value),
            "decimal_finite_abs_bound": (
                None if value.decimal_finite_abs_bound is None else str(value.decimal_finite_abs_bound)
            ),
            "metadata": metadata,
        }


@dataclass(frozen=True, slots=True)
class PreparedFormulas:
    document: dict[str, Any]
    problem: Problem


def prepare_formulas(before_source: bytes, after_source: bytes, row_bound: int) -> PreparedFormulas:
    """Build from the same bytes whose hashes are bound into the export."""

    if type(row_bound) is not int or row_bound < 0:
        raise InspectionError("formula row bound must be a non-negative integer")
    before, after = (_parse_source(source) for source in (before_source, after_source))
    observation = Observation()
    problem = build_problem(
        before, after, row_bound,
        before_node_observer=observation.before.node,
        after_node_observer=observation.after.node,
        after_edge_observer=observation.after.edge,
        boundary_observer=observation.boundary,
        comparison_observer=observation.compared,
    )
    comparison = observation.comparison
    if comparison is None or set(observation.boundaries) != {"before", "after"}:
        raise InspectionError("verification did not emit all formula observations")
    # Rendering seals the same string domain. Do it before collecting AST roots
    # so no constraints or declarations are missing from this unsolved export.
    try:
        problem.script.seal_string_order()
    except smt.SmtError as error:
        raise InspectionError(str(error)) from error
    dag = TermDag(problem.script.declarations)
    declarations = [dag.declaration(item) for item in problem.script.declarations]
    assertions = [dag.ref(term) for term in problem.script.assertions]
    document: dict[str, Any] = {
        "format": FORMULAS_FORMAT, "version": FORMULAS_VERSION, "status": "FORMULAS_GENERATED",
        "row_bound": row_bound, "task_bound": TASKS,
        "semantic_mode": problem.semantic_mode,
        "abstract_integral_average": problem.abstract_integral_average,
        "inputs": {
            "before_sha256": hashlib.sha256(before_source).hexdigest(),
            "after_sha256": hashlib.sha256(after_source).hexdigest(),
        },
        "semantic_modes": {"before": before.semantic_mode, "after": after.semantic_mode},
        "declarations": declarations, "assertions": assertions,
        "string_literals": [
            {"atom": str(atom), "value": literal}
            for atom, literal in problem.script.string_literals.items()
        ],
        "comparison": {
            "semantics": "sequence" if comparison.ordered else "bag",
            "before": dag.family(comparison.left), "after": dag.family(comparison.right),
            "pair_equal": [[dag.ref(term) for term in row] for row in comparison.pair_equal],
            "counterexample": dag.ref(comparison.mismatch.counterexample),
            "soundness_exclusion": (
                None if problem.soundness_exclusion is None else dag.ref(problem.soundness_exclusion.predicate)
            ),
        },
    }
    for side, snapshot in (("before", before), ("after", after)):
        execution = getattr(observation, side)
        nodes = snapshot.plan.node_map()
        observed = {event.node for event in execution.nodes}
        document[side] = {
            "operators": [
                {
                    "scope": _scope_json(event.scope), "node": event.node,
                    "op": _node_kind(nodes[event.node]), "result": dag.family(event.result),
                }
                for event in execution.nodes
            ],
            "connections": [
                _edge_json(event.edge) | {
                    "consumer_task": event.consumer_task, "result": dag.family(event.result),
                }
                for event in execution.edges
            ],
            "boundary": dag.family(observation.boundaries[side]),
            "unobserved_nodes": [node.id for node in snapshot.plan.nodes if node.id not in observed],
        }
    document["terms"] = dag.nodes
    return PreparedFormulas(document, problem)


def _parse_source(source: bytes) -> ir.Snapshot:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for name, value in items:
            if name in result:
                raise InspectionError(f"duplicate snapshot JSON key {name!r}")
            result[name] = value
        return result

    def invalid_constant(value: str) -> None:
        raise InspectionError(f"non-standard snapshot JSON constant {value!r}")

    try:
        return ir.parse_snapshot(json.loads(source.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid_constant))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise InspectionError(f"invalid formula snapshot: {error}") from error
