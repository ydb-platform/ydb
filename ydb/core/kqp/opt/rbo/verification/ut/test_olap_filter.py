import unittest
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator as RelationEvaluator,
    family_equal,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder as ScalarEncoder
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stages import (
    Evaluator as StageEvaluator,
    Router,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem


def comparison(kind, value):
    return {
        "kind": kind,
        "left": {"kind": "column", "column": "a.value"},
        "right": {"kind": "literal", "type": "Int64", "value": value},
    }


def scan(predicate=None, pushed_limit=None):
    return {
        "id": "scan",
        "op": "scan",
        "table": "A",
        "columns": [{"source": "value", "output": "a.value"}],
        "predicate": predicate,
        "pushed_limit": pushed_limit,
    }


def snapshot(nodes, root, stage_graph):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "value", "type": "Int64", "nullable": True}
                    ],
                    "unique_keys": [],
                }
            ]
        },
        "plan": {
            "nodes": nodes,
            "root": root,
            "output": ["a.value"],
            "subplans": [],
        },
        "stage_graph": stage_graph,
    }


def logical_filter(kind="gte", value=0):
    return parse_snapshot(
        snapshot(
            [
                scan(),
                {
                    "id": "filter",
                    "op": "filter",
                    "input": "scan",
                    "predicate": comparison(kind, value),
                },
            ],
            "filter",
            None,
        )
    )


def pushed_filter(kind="gte", value=0, limit=None):
    node = scan(
        comparison(kind, value),
        None
        if limit is None
        else {"kind": "literal", "type": "Uint64", "value": limit},
    )
    return parse_snapshot(
        snapshot(
            [node],
            "scan",
            {
                "root_stage": "source",
                "stages": [
                    {
                        "id": "source",
                        "nodes": ["scan"],
                        "inputs": [],
                        "outputs": [{"index": 0, "node": "scan"}],
                        "source_storage": "column",
                    }
                ],
                "edges": [],
                "assumptions": [],
            },
        )
    )


def _ground(term, constants):
    if term.operation == "symbol":
        return constants[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    if term.operation == "not":
        return not _ground(term.arguments[0], constants)
    if term.operation == "and":
        return all(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "or":
        return any(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "=":
        return _ground(term.arguments[0], constants) == _ground(term.arguments[1], constants)
    if term.operation == "<":
        return _ground(term.arguments[0], constants) < _ground(term.arguments[1], constants)
    if term.operation == "ite":
        branch = term.arguments[1] if _ground(term.arguments[0], constants) else term.arguments[2]
        return _ground(branch, constants)
    if term.operation == "+":
        return sum(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "mod":
        return _ground(term.arguments[0], constants) % _ground(term.arguments[1], constants)
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _constants(database, present, values):
    constants = {}
    for row, active, value in zip(database.witness["A"], present, values):
        constants[row.present.atom] = active
        cell = row.cells["value"]
        constants[cell.is_null.atom] = value is None
        constants[cell.value.atom] = 0 if value is None else value
    return constants


def _bags(family, constants):
    result = set()
    for outcome in family.outcomes:
        choices = tuple(
            choice
            for choice in outcome.choices
            if choice.term.atom not in constants
        )
        domains = tuple(range(choice.bound) for choice in choices)
        for assignment in product(*domains):
            grounded = constants | {
                choice.term.atom: value
                for choice, value in zip(choices, assignment)
            }
            if not _ground(outcome.enabled, grounded):
                continue
            values = []
            for row in outcome.relation.rows:
                if not _ground(row.present, grounded):
                    continue
                value = row.values["a.value"]
                values.append(
                    None
                    if _ground(value.is_null, grounded)
                    else _ground(value.value, grounded)
                )
            result.add(tuple(sorted(
                values,
                key=lambda item: (item is not None, repr(item)),
            )))
    return result


class OlapFilterSemanticsTest(unittest.TestCase):
    def test_explicit_filter_and_pushed_scan_predicate_are_exhaustively_equivalent(self):
        before = logical_filter()
        after = pushed_filter()
        script = smt.Script()
        database = Database(before, 2, script)
        scalar = ScalarEncoder(script)
        router = Router(script)
        equality = family_equal(
            RelationEvaluator(before, database, scalar).root(),
            StageEvaluator(after, database, scalar, router).root(),
            scalar,
        )

        for present, values, tasks in product(
            product((False, True), repeat=2),
            product((None, -1, 0, 1), repeat=2),
            product((False, True), repeat=2),
        ):
            constants = _constants(database, present, values)
            for slot, task in enumerate(tasks):
                constants[router.source_task("A", slot).atom] = task
            with self.subTest(present=present, values=values, tasks=tasks):
                self.assertTrue(_ground(equality, constants))

        problem = build_problem(before, after, 2)
        self.assertIn("(<", problem.script.render())

    def test_comparison_mutation_has_a_bounded_counterexample(self):
        before = logical_filter("gte", 0)
        after = pushed_filter("gt", 0)
        script = smt.Script()
        database = Database(before, 1, script)
        scalar = ScalarEncoder(script)
        router = Router(script)
        equality = family_equal(
            RelationEvaluator(before, database, scalar).root(),
            StageEvaluator(after, database, scalar, router).root(),
            scalar,
        )
        constants = _constants(database, (True,), (0,))
        constants[router.source_task("A", 0).atom] = False
        self.assertFalse(_ground(equality, constants))

    def test_scan_predicate_runs_before_each_tasks_pushed_limit(self):
        parsed = pushed_filter("gte", 1, limit=1)
        script = smt.Script()
        database = Database(parsed, 2, script)
        router = Router(script)
        family = StageEvaluator(
            parsed,
            database,
            ScalarEncoder(script),
            router,
        ).root()
        constants = _constants(database, (True, True), (0, 1))
        constants[router.source_task("A", 0).atom] = False
        constants[router.source_task("A", 1).atom] = False
        self.assertEqual(_bags(family, constants), {(1,)})

    def test_deleting_the_pushed_predicate_changes_the_plan(self):
        before = logical_filter()
        value = snapshot(
            [scan()],
            "scan",
            {
                "root_stage": "source",
                "stages": [
                    {
                        "id": "source",
                        "nodes": ["scan"],
                        "inputs": [],
                        "outputs": [{"index": 0, "node": "scan"}],
                        "source_storage": "column",
                    }
                ],
                "edges": [],
                "assumptions": [],
            },
        )
        after = parse_snapshot(value)
        script = smt.Script()
        database = Database(before, 1, script)
        scalar = ScalarEncoder(script)
        router = Router(script)
        equality = family_equal(
            RelationEvaluator(before, database, scalar).root(),
            StageEvaluator(after, database, scalar, router).root(),
            scalar,
        )
        constants = _constants(database, (True,), (-1,))
        constants[router.source_task("A", 0).atom] = False
        self.assertFalse(_ground(equality, constants))


if __name__ == "__main__":
    unittest.main()
