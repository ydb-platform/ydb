"""Optional bounded-model reachability, outside equivalence classification.

SAT establishes only a modeled successful nonempty execution. Opaque functions,
AVG carriers and binary64 primitives can over-approximate runtime behavior.
UNSAT does not distinguish successful emptiness from unavoidable query errors,
nor establish coverage of intermediate operators or branches.
"""

from __future__ import annotations

from pathlib import Path

from . import smt
from .relation import (
    MismatchBranch, Relation, RelationError, RelationFamily, Row,
    combine_families, successful_family_reachable,
)
from .stages import TASKS
from .verify import Problem, SolverError, VerificationError, query_diagnostic_obligations


class NonemptyOutputObserver:
    """Collect already evaluated roots; repeated sides are buffered result slots."""

    def __init__(self) -> None:
        self._families: dict[str, list[RelationFamily]] = {"before": [], "after": []}

    def __call__(self, side: str, family: RelationFamily) -> None:
        self._families[side].append(family)

    def predicates(self, script: smt.Script) -> tuple[MismatchBranch, ...]:
        if not self._families["before"] or len(self._families["before"]) != len(self._families["after"]):
            raise VerificationError("nonempty-output diagnostic requires both boundaries of every result")

        def nonempty(relations: tuple[Relation, ...]) -> Relation:
            # Project the entire tuple to one presence bit. The existing family
            # product retains shared decisions, choice bounds and every slot's
            # error: one nonempty slot cannot hide another slot's failure.
            present = smt.or_(*(row.present for relation in relations for row in relation.rows))
            return Relation((), (Row(present, {}),))

        return tuple(MismatchBranch(
            side,
            successful_family_reachable(
                combine_families(tuple(families), nonempty), script,
                f"diagnostic:{side}:nonempty", lambda relation: relation.rows[0].present,
            ),
        ) for side, families in self._families.items())


def diagnose_nonempty_outputs(
    problem: Problem,
    observer: NonemptyOutputObserver,
    solver: str | Path,
    row_bound: int,
    timeout_ms: int = 10_000,
) -> dict[str, object]:
    """Check domain exclusions first, then each complete boundary separately.

    Before/after SAT may use different databases or schedules. This is not a
    mutation detector, runtime witness, or exhaustive branch-coverage claim.
    """
    report: dict[str, object] = {
        "observation": "successful_nonempty_output",
        "scope": "bounded_model",
        "row_bound": row_bound,
        "task_bound": TASKS,
        "meaning": "Some successful complete execution has a row in at least one result slot.",
        "caveat": "SAT is modeled reachability, not a runtime witness or exhaustive operator/branch coverage.",
        "model_domain": {"status": "NOT_REQUIRED" if problem.soundness_exclusion is None else "UNESTABLISHED"},
        "before": {"status": "UNKNOWN", "reason": "not checked"},
        "after": {"status": "UNKNOWN", "reason": "not checked"},
    }
    if problem.semantic_mode is not None:
        report["semantic_mode"] = problem.semantic_mode
    try:
        predicates = observer.predicates(problem.script)
        domain = () if problem.soundness_exclusion is None else (problem.soundness_exclusion,)
        branches = domain + predicates
        queries = query_diagnostic_obligations(problem, branches, solver, timeout_ms)
        for branch, query in zip(branches, queries):
            result = {"status": query.status.upper()}
            if query.reason is not None:
                result["reason"] = query.reason
            if branch is problem.soundness_exclusion:
                report["model_domain"] = result
                if query.status != "unsat":
                    for side in ("before", "after"):
                        report[side] = {"status": "UNKNOWN", "reason": "model-domain exclusions were not ruled out"}
                    break
            else:
                report[branch.name] = result
    except (SolverError, VerificationError, RelationError, smt.SmtError) as error:
        report["error"] = str(error)
    return report
