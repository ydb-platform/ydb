"""Exhaustive concrete reference checks for the logical relational kernel.

The reference evaluator below is deliberately independent of relation.py.  It
interprets the public snapshot JSON as concrete SQL bags, while the production
kernel builds a symbolic counterexample obligation.  Fully assigning every
catalog slot makes that obligation ground, so its truth must agree with the
reference bags being different.
"""

import copy
import unittest
from collections import Counter
from dataclasses import dataclass
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    JOIN_KINDS as ADMITTED_JOIN_KINDS,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    build_logical_kernel_problem_for_tests,
)


JOIN_KINDS = (
    "cross",
    "inner",
    "left",
    "right",
    "full",
    "left_semi",
    "right_semi",
    "left_anti",
    "right_anti",
    "exclusion",
)
TABLE_COLUMNS = {
    "A": ("v",),
    "B": ("v",),
    "E0": ("c0", "c1"),
}


@dataclass(frozen=True)
class Cell:
    is_null: bool
    value: int


@dataclass(frozen=True)
class Slot:
    present: bool
    cells: tuple[Cell, ...]


# Presence, nullness, and the hidden payload are independent solver variables.
# Enumerating their full Boolean x Boolean x {0, 1} product catches accidental
# use of null or absent payloads, rather than quotienting those assignments away.
ONE_COLUMN_SLOTS = tuple(
    Slot(present, (Cell(is_null, value),))
    for present, is_null, value in product((False, True), (False, True), (0, 1))
)
ABSENT_ONE = Slot(False, (Cell(False, 0),))
ABSENT_TWO = Slot(False, (Cell(False, 0), Cell(False, 0)))
PRESENT_ZERO = Slot(True, (Cell(False, 0),))


def _column(name):
    return {"kind": "column", "column": name}


def _literal(value):
    scalar_type = "Bool" if isinstance(value, bool) else "Int64"
    return {"kind": "literal", "type": scalar_type, "value": value}


def _binary(kind, left, right, **settings):
    return {"kind": kind, "left": left, "right": right, **settings}


def _scan(node_id, table, source, output):
    return {
        "id": node_id,
        "op": "scan",
        "table": table,
        "columns": [{"source": source, "output": output}],
    }


def _catalog():
    return {
        "tables": [
            {
                "name": table,
                "columns": [
                    {"name": name, "type": "Int64", "nullable": True}
                    for name in columns
                ],
                "unique_keys": [],
            }
            for table, columns in TABLE_COLUMNS.items()
        ]
    }


def _snapshot(plan):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": _catalog(),
            "plan": copy.deepcopy(plan),
            "stage_graph": None,
        }
    )


def _expected_plan(output):
    scan = {
        "id": "expected",
        "op": "scan",
        "table": "E0",
        "columns": [
            {"source": f"c{position}", "output": name}
            for position, name in enumerate(output)
        ],
    }
    return {
        "nodes": [scan],
        "root": "expected",
        "output": list(output),
        "subplans": [],
    }


def _pipeline_plan():
    # The scan aliases its nullable value, projection adds one, and the filter
    # keeps only input 1; NULL observes SQL UNKNOWN and is rejected.
    scan = _scan("a", "A", "v", "a")
    project = {
        "id": "project",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "y",
                "expression": {
                    **_binary("add", _column("a"), _literal(1)),
                    "type": "Int64",
                    "nullable": True,
                },
            }
        ],
    }
    filter_node = {
        "id": "filter",
        "op": "filter",
        "input": "project",
        "predicate": _binary("eq", _column("y"), _literal(2)),
    }
    return {
        "nodes": [scan, project, filter_node],
        "root": "filter",
        "output": ["y"],
        "subplans": [],
    }


def _constant_plan():
    return {
        "nodes": [
            {"id": "unit", "op": "empty_source"},
            {
                "id": "constant",
                "op": "project",
                "input": "unit",
                "ordered": False,
                "columns": [
                    {
                        "output": "value",
                        "expression": {"kind": "null", "type": "Int64"},
                    }
                ],
            },
        ],
        "root": "constant",
        "output": ["value"],
        "subplans": [],
    }


def _union_plan():
    left = _scan("a", "A", "v", "left_value")
    right = _scan("b", "B", "v", "right_value")
    union = {
        "id": "union",
        "op": "union_all",
        "inputs": [
            {"node": "a", "columns": ["left_value"]},
            {"node": "b", "columns": ["right_value"]},
        ],
        "output": ["value"],
        "ordered": False,
    }
    return {
        "nodes": [left, right, union],
        "root": "union",
        "output": ["value"],
        "subplans": [],
    }


def _join_plan(kind):
    left = _scan("a_scan", "A", "v", "a")
    right = _scan("b_scan", "B", "v", "b")
    predicate = (
        _literal(True)
        if kind == "cross"
        else _binary("eq", _column("a"), _column("b"))
    )
    join = {
        "id": "join",
        "op": "join",
        "left": "a_scan",
        "right": "b_scan",
        "kind": kind,
        "predicate": predicate,
    }
    if kind in {"left_semi", "left_anti"}:
        output = ["a"]
    elif kind in {"right_semi", "right_anti"}:
        output = ["b"]
    else:
        output = ["a", "b"]
    return {
        "nodes": [left, right, join],
        "root": "join",
        "output": output,
        "subplans": [],
    }


def _shared_join_plan(kind):
    left = _scan("a_scan", "A", "v", "shared")
    right = _scan("b_scan", "B", "v", "shared")
    join = {
        "id": "join",
        "op": "join",
        "left": "a_scan",
        "right": "b_scan",
        "kind": kind,
        "keys": [{"left": "shared", "right": "shared"}],
        "predicate": _literal(True),
    }
    return {
        "nodes": [left, right, join],
        "root": "join",
        "output": ["shared"],
        "subplans": [],
    }


def _expression(expression, row):
    kind = expression["kind"]
    if kind == "column":
        return row[expression["column"]]
    if kind == "literal":
        return expression["value"]
    if kind == "null":
        return None

    left = _expression(expression["left"], row)
    right = _expression(expression["right"], row)
    if kind == "eq" and expression.get("null_safe", False):
        return left is None and right is None or (
            left is not None and right is not None and left == right
        )
    if left is None or right is None:
        return None
    if kind == "eq":
        return left == right
    if kind == "add":
        return left + right
    raise AssertionError(f"reference evaluator does not implement {kind!r}")


class ConcreteEvaluator:
    """Small concrete bag interpreter, intentionally unrelated to relation.py."""

    def __init__(self, plan, database):
        self.plan = plan
        self.database = database
        self.nodes = {node["id"]: node for node in plan["nodes"]}
        self.rows = {}
        self.schemas = {}

    def result(self):
        rows = self._node(self.plan["root"])
        output = tuple(self.plan["output"])
        return Counter(tuple(row[name] for name in output) for row in rows)

    def _columns(self, node_id):
        if node_id not in self.schemas:
            node = self.nodes[node_id]
            operation = node["op"]
            if operation == "empty_source":
                columns = ()
            elif operation == "scan":
                columns = tuple(item["output"] for item in node["columns"])
            elif operation == "project":
                columns = tuple(item["output"] for item in node["columns"])
            elif operation == "filter":
                columns = self._columns(node["input"])
            elif operation == "union_all":
                columns = tuple(node["output"])
            elif operation == "join":
                left = self._columns(node["left"])
                right = self._columns(node["right"])
                if node["kind"] in {"left_semi", "left_anti"}:
                    columns = left
                elif node["kind"] in {"right_semi", "right_anti"}:
                    columns = right
                else:
                    columns = left + right
            else:
                raise AssertionError(f"reference evaluator does not implement {operation!r}")
            self.schemas[node_id] = columns
        return self.schemas[node_id]

    def _node(self, node_id):
        if node_id in self.rows:
            return self.rows[node_id]
        node = self.nodes[node_id]
        operation = node["op"]
        if operation == "empty_source":
            result = [{}]
        elif operation == "scan":
            result = []
            for source in self.database[node["table"]]:
                row = {
                    item["output"]: source[item["source"]]
                    for item in node["columns"]
                }
                if node.get("predicate") is None or _expression(node["predicate"], row) is True:
                    result.append(row)
        elif operation == "project":
            result = [
                {
                    item["output"]: _expression(item["expression"], source)
                    for item in node["columns"]
                }
                for source in self._node(node["input"])
            ]
        elif operation == "filter":
            result = [
                row
                for row in self._node(node["input"])
                if _expression(node["predicate"], row) is True
            ]
        elif operation == "union_all":
            result = []
            for item in node["inputs"]:
                for source in self._node(item["node"]):
                    result.append(
                        {
                            output: source[input_name]
                            for output, input_name in zip(node["output"], item["columns"])
                        }
                    )
        elif operation == "join":
            result = self._join(node)
        else:
            raise AssertionError(f"reference evaluator does not implement {operation!r}")
        self.rows[node_id] = result
        return result

    def _join(self, node):
        left = self._node(node["left"])
        right = self._node(node["right"])
        left_columns = self._columns(node["left"])
        right_columns = self._columns(node["right"])

        def keys_match(left_row, right_row):
            for key in node.get("keys", ()):
                left_value = left_row[key["left"]]
                right_value = right_row[key["right"]]
                if (
                    left_value is None
                    or right_value is None
                    or left_value != right_value
                ):
                    return False
            return True

        pairs = [
            (left_index, right_index, left_row | right_row)
            for left_index, left_row in enumerate(left)
            for right_index, right_row in enumerate(right)
            if keys_match(left_row, right_row)
            and _expression(node["predicate"], left_row | right_row) is True
        ]
        matched_left = {left_index for left_index, _, _ in pairs}
        matched_right = {right_index for _, right_index, _ in pairs}
        kind = node["kind"]

        if kind in {"inner", "cross"}:
            return [row for _, _, row in pairs]
        if kind == "left_semi":
            return [row for index, row in enumerate(left) if index in matched_left]
        if kind == "right_semi":
            return [row for index, row in enumerate(right) if index in matched_right]
        if kind == "left_anti":
            return [row for index, row in enumerate(left) if index not in matched_left]
        if kind == "right_anti":
            return [row for index, row in enumerate(right) if index not in matched_right]

        unmatched_left = [
            row | {name: None for name in right_columns}
            for index, row in enumerate(left)
            if index not in matched_left
        ]
        unmatched_right = [
            {name: None for name in left_columns} | row
            for index, row in enumerate(right)
            if index not in matched_right
        ]
        if kind == "left":
            return [row for _, _, row in pairs] + unmatched_left
        if kind == "right":
            return [row for _, _, row in pairs] + unmatched_right
        if kind == "full":
            return [row for _, _, row in pairs] + unmatched_left + unmatched_right
        if kind == "exclusion":
            return unmatched_left + unmatched_right
        raise AssertionError(f"reference evaluator does not implement join {kind!r}")


def _visible_database(ground):
    result = {}
    for table, slots in ground.items():
        columns = TABLE_COLUMNS[table]
        result[table] = [
            {
                name: None if cell.is_null else cell.value
                for name, cell in zip(columns, slot.cells)
            }
            for slot in slots
            if slot.present
        ]
    return result


def _slot(values):
    cells = tuple(Cell(value is None, 0 if value is None else value) for value in values)
    return Slot(True, cells + (Cell(False, 0),) * (2 - len(cells)))


def _ground_database(a, b, expected_rows):
    if len(expected_rows) > 2:
        raise AssertionError("expected-row harness has only two slots")
    expected = [_slot(row) for row in expected_rows]
    expected.extend([ABSENT_TWO] * (2 - len(expected)))
    return {
        "A": (a, ABSENT_ONE) if isinstance(a, Slot) else a,
        "B": (b, ABSENT_ONE) if isinstance(b, Slot) else b,
        "E0": tuple(expected),
    }


def _bind_symbol(environment, term, value):
    if term.operation == "symbol":
        environment[term.atom] = value
    else:
        if _ground_term(term, environment) != value:
            raise AssertionError(f"cannot bind constant {term.render()} to {value!r}")


def _witness_environment(problem, ground):
    environment = {}
    for table, rows in problem.witness.items():
        if len(rows) != len(ground[table]):
            raise AssertionError("symbolic and concrete row bounds differ")
        for row, slot in zip(rows, ground[table]):
            _bind_symbol(environment, row.present, slot.present)
            for name, cell in row.cells.items():
                ground_cell = slot.cells[TABLE_COLUMNS[table].index(name)]
                _bind_symbol(environment, cell.is_null, ground_cell.is_null)
                _bind_symbol(environment, cell.value, ground_cell.value)
    return environment


def _ground_term(term, environment):
    operation = term.operation
    if operation == "symbol":
        return environment[term.atom]
    if operation in {"bool", "int"}:
        return term.atom
    arguments = tuple(_ground_term(argument, environment) for argument in term.arguments)
    if operation == "not":
        return not arguments[0]
    if operation == "and":
        return all(arguments)
    if operation == "or":
        return any(arguments)
    if operation == "=":
        return arguments[0] == arguments[1]
    if operation == "<":
        return arguments[0] < arguments[1]
    if operation == "ite":
        return arguments[1] if arguments[0] else arguments[2]
    if operation == "+":
        return sum(arguments)
    if operation == "-":
        return arguments[0] - arguments[1]
    if operation == "*":
        result = 1
        for argument in arguments:
            result *= argument
        return result
    if operation == "mod":
        return arguments[0] % arguments[1]
    raise AssertionError(f"ground evaluator does not implement SMT operation {operation!r}")


def _ground_formula_is_satisfiable(problem, ground):
    environment = _witness_environment(problem, ground)
    return all(
        _ground_term(assertion, environment) is True
        for assertion in problem.script.assertions
    )


def _reference_variants(rows, arity):
    rows = list(rows)
    yield "exact", rows

    if rows:
        changed = list(rows)
        first = list(changed[0])
        first[0] = 0 if first[0] is None else first[0] + 1
        changed[0] = tuple(first)
        yield "changed_value", changed
        yield "changed_multiplicity", rows[:-1]
    else:
        yield "changed_value", [(0,) * arity]
        yield "changed_multiplicity", [(None,) * arity]


class LogicalKernelReferenceTest(unittest.TestCase):
    maxDiff = None

    def _check(self, plan, assignments):
        output = tuple(plan["output"])
        expected_plan = _expected_plan(output)
        problem = build_logical_kernel_problem_for_tests(
            _snapshot(plan), _snapshot(expected_plan), 2
        )

        for label, a, b in assignments:
            inputs = _ground_database(a, b, ())
            actual = ConcreteEvaluator(plan, _visible_database(inputs)).result()
            rows = list(actual.elements())
            self.assertLessEqual(len(rows), 2)
            for variant, expected_rows in _reference_variants(rows, len(output)):
                ground = _ground_database(a, b, expected_rows)
                visible = _visible_database(ground)
                reference_difference = (
                    ConcreteEvaluator(plan, visible).result()
                    != ConcreteEvaluator(expected_plan, visible).result()
                )
                with self.subTest(case=label, expected=variant):
                    self.assertEqual(
                        _ground_formula_is_satisfiable(problem, ground),
                        reference_difference,
                    )

    def test_scan_project_filter(self):
        self._check(
            _pipeline_plan(),
            ((f"a={a}", a, ABSENT_ONE) for a in ONE_COLUMN_SLOTS),
        )

    def test_empty_source_and_constant_projection(self):
        self._check(
            _constant_plan(),
            (("constant", ABSENT_ONE, ABSENT_ONE),),
        )

    def test_union_all(self):
        self._check(
            _union_plan(),
            (
                (f"a={a},b={b}", a, b)
                for a, b in product(ONE_COLUMN_SLOTS, repeat=2)
            ),
        )

    def test_every_admitted_join_kind(self):
        self.assertEqual(set(JOIN_KINDS), set(ADMITTED_JOIN_KINDS))
        for kind in JOIN_KINDS:
            with self.subTest(kind=kind):
                self._check(
                    _join_plan(kind),
                    [
                        (f"{kind}:a={a},b={b}", a, b)
                        for a, b in product(ONE_COLUMN_SLOTS, repeat=2)
                    ]
                    + [
                        (
                            f"{kind}:duplicate_left",
                            (PRESENT_ZERO, PRESENT_ZERO),
                            PRESENT_ZERO,
                        ),
                        (
                            f"{kind}:duplicate_right",
                            PRESENT_ZERO,
                            (PRESENT_ZERO, PRESENT_ZERO),
                        ),
                    ],
                )

    def test_side_explicit_keys_keep_shared_semi_join_inputs_distinct(self):
        for kind in ("left_semi", "left_anti", "right_semi", "right_anti"):
            with self.subTest(kind=kind):
                self._check(
                    _shared_join_plan(kind),
                    [
                        (f"{kind}:a={a},b={b}", a, b)
                        for a, b in product(ONE_COLUMN_SLOTS, repeat=2)
                    ],
                )

    def test_root_column_order(self):
        plan = _join_plan("cross")
        plan["output"] = ["b", "a"]
        self._check(
            plan,
            (
                (f"root_order:a={a},b={b}", a, b)
                for a, b in product(ONE_COLUMN_SLOTS, repeat=2)
            ),
        )


if __name__ == "__main__":
    unittest.main()
