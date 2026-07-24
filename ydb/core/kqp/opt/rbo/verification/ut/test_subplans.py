import copy
import os
import unittest
from dataclasses import replace
from itertools import product
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import relation, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator,
    Outcome,
    RelationFamily,
    RelationError,
    family_equal,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder as ScalarEncoder,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    build_logical_kernel_problem_for_tests,
    solve,
)


BINDING = "$scalar"
IN_BINDING = "$in"
SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


def _literal(scalar_type, value):
    return {"kind": "literal", "type": scalar_type, "value": value}


def _equality(left, right, *, null_safe=False):
    expression = {
        "kind": "eq",
        "left": {"kind": "column", "column": left},
        "right": {"kind": "column", "column": right},
    }
    if null_safe:
        expression["null_safe"] = True
    return expression


def _base_snapshot(expression=None):
    expression = expression or _literal("Int64", 7)
    output_type = expression["type"]
    output_nullable = expression["kind"] == "null"
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {"tables": []},
        "plan": {
            "nodes": [
                {"id": "main_source", "op": "empty_source"},
                {
                    "id": "main_project",
                    "op": "project",
                    "input": "main_source",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "column",
                                "column": BINDING,
                            },
                        }
                    ],
                },
                {"id": "sub_source", "op": "empty_source"},
                {
                    "id": "sub_value",
                    "op": "project",
                    "input": "sub_source",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "sub.value",
                            "expression": expression,
                        }
                    ],
                },
            ],
            "root": "main_project",
            "output": ["result"],
            "subplans": [
                {
                    "binding": BINDING,
                    "kind": "scalar",
                    "root": "sub_value",
                    "output": {
                        "column": "sub.value",
                        "type": output_type,
                        "nullable": output_nullable,
                    },
                    "type": output_type,
                    "nullable": True,
                    "dependencies": [],
                    "consumers": ["main_project"],
                }
            ],
        },
        "stage_graph": None,
    }


def _evaluate(raw, row_bound=0, observer=None):
    snapshot = parse_snapshot(raw)
    script = smt.Script()
    database = Database(snapshot, row_bound, script)
    evaluator = Evaluator(
        snapshot,
        database,
        ScalarEncoder(script),
        node_observer=observer,
    )
    return evaluator, evaluator.root().certain()


def _global_decimal_sum_snapshot():
    raw = _base_snapshot(
        {
            "kind": "literal",
            "type": "Decimal(35,2)",
            "value": {"kind": "finite", "scaled": "0"},
        }
    )
    raw["schema"]["tables"] = [
        {
            "name": "A",
            "columns": [
                {"name": "amount", "type": "Decimal(7,2)", "nullable": True}
            ],
            "unique_keys": [],
        }
    ]
    raw["plan"]["nodes"][2:] = [
        {
            "id": "sub_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "amount", "output": "sub.amount"}],
            "predicate": None,
            "pushed_limit": None,
        },
        {
            "id": "sub_value",
            "op": "aggregate",
            "input": "sub_scan",
            "keys": [],
            "aggregates": [
                {
                    "input": "sub.amount",
                    "function": "sum",
                    "output": "sub.value",
                    "type": "Decimal(35,2)",
                    "nullable": True,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        },
    ]
    raw["plan"]["subplans"][0]["output"]["nullable"] = True
    return raw


def _scan_scalar_snapshot():
    raw = _base_snapshot()
    raw["schema"]["tables"] = [
        {
            "name": "A",
            "columns": [
                {"name": "x", "type": "Int64", "nullable": False}
            ],
            "unique_keys": [],
        }
    ]
    raw["plan"]["nodes"][2:] = [
        {
            "id": "sub_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "x", "output": "sub.value"}],
            "predicate": None,
            "pushed_limit": None,
        }
    ]
    raw["plan"]["subplans"][0]["root"] = "sub_scan"
    return raw


def _exists_snapshot(
    *,
    inner_present=True,
    correlated=True,
    negate=False,
    repeat_binding=False,
    limit_inner=False,
):
    nodes = [
        {
            "id": "outer_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "x", "output": "outer.x"}],
            "predicate": None,
            "pushed_limit": None,
        },
        {
            "id": "inner_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "y", "output": "inner.x"}],
            "predicate": None,
            "pushed_limit": None,
        },
    ]
    inner_root = "inner_scan"
    if not inner_present:
        nodes.append(
            {
                "id": "inner_empty",
                "op": "filter",
                "input": inner_root,
                "predicate": _literal("Bool", False),
            }
        )
        inner_root = "inner_empty"
    if limit_inner:
        nodes.append(
            {
                "id": "inner_one",
                "op": "limit",
                "input": inner_root,
                "count": _literal("Uint64", 1),
                "offset": None,
                "phase": "undefined",
            }
        )
        inner_root = "inner_one"

    binding_expr = {"kind": "column", "column": "$exists"}
    if repeat_binding:
        binding_expr = {
            "kind": "or",
            "args": [binding_expr, copy.deepcopy(binding_expr)],
        }
    if negate:
        binding_expr = {"kind": "not", "arg": binding_expr}
    nodes.append(
        {
            "id": "main_filter",
            "op": "filter",
            "input": "outer_scan",
            "predicate": binding_expr,
        }
    )
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "x", "type": "Int64", "nullable": True},
                        {"name": "y", "type": "Int64", "nullable": True},
                    ],
                    "unique_keys": [],
                }
            ]
        },
        "plan": {
            "nodes": nodes,
            "root": "main_filter",
            "output": ["outer.x"],
            "subplans": [
                {
                    "binding": "$exists",
                    "kind": "exists",
                    "root": inner_root,
                    "predicate": (
                        _equality("outer.x", "inner.x")
                        if correlated
                        else None
                    ),
                    "type": "Bool",
                    "nullable": False,
                    "dependencies": ["outer.x"] if correlated else [],
                    "consumers": ["main_filter"],
                }
            ],
        },
        "stage_graph": None,
    }


def _in_snapshot(*, scalar_type="Int32", negate=False, repeat_binding=False):
    binding_expr = {"kind": "column", "column": IN_BINDING}
    if repeat_binding:
        binding_expr = {
            "kind": "or",
            "args": [binding_expr, copy.deepcopy(binding_expr)],
        }
    if negate:
        binding_expr = {"kind": "not", "arg": binding_expr}
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "Outer",
                    "columns": [
                        {"name": "k", "type": scalar_type, "nullable": False},
                    ],
                    "unique_keys": [],
                },
                {
                    "name": "Inner",
                    "columns": [
                        {"name": "k", "type": scalar_type, "nullable": False},
                    ],
                    "unique_keys": [],
                },
            ]
        },
        "plan": {
            "nodes": [
                {
                    "id": "outer_scan",
                    "op": "scan",
                    "table": "Outer",
                    "columns": [{"source": "k", "output": "outer.k"}],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "inner_scan",
                    "op": "scan",
                    "table": "Inner",
                    "columns": [{"source": "k", "output": "inner.k"}],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "main_filter",
                    "op": "filter",
                    "input": "outer_scan",
                    "predicate": binding_expr,
                },
            ],
            "root": "main_filter",
            "output": ["outer.k"],
            "subplans": [
                {
                    "binding": IN_BINDING,
                    "kind": "in",
                    "root": "inner_scan",
                    "lookup": {
                        "column": "outer.k",
                        "type": scalar_type,
                        "nullable": False,
                    },
                    "output": {
                        "column": "inner.k",
                        "type": scalar_type,
                        "nullable": False,
                    },
                    "type": "Bool",
                    "nullable": False,
                    "dependencies": [],
                    "consumers": ["main_filter"],
                }
            ],
        },
        "stage_graph": None,
    }


def _lower_in_snapshot(raw, join_kind):
    result = copy.deepcopy(raw)
    descriptor = result["plan"]["subplans"][0]
    consumer = next(
        node
        for node in result["plan"]["nodes"]
        if node["id"] == "main_filter"
    )
    consumer.clear()
    consumer.update(
        {
            "id": "main_filter",
            "op": "join",
            "left": "outer_scan",
            "right": descriptor["root"],
            "kind": join_kind,
            "keys": [
                {
                    "left": descriptor["lookup"]["column"],
                    "right": descriptor["output"]["column"],
                }
            ],
            "predicate": _literal("Bool", True),
        }
    )
    result["plan"]["subplans"] = []
    return result


def _correlated_scalar_snapshot(function="count"):
    if function not in {"avg", "count", "sum"}:
        raise AssertionError(f"unknown correlated aggregate {function!r}")
    input_type = "Decimal(7,2)" if function == "avg" else "Int64"
    output_type = (
        "Decimal(7,2)"
        if function == "avg"
        else "Uint64" if function == "count" else "Int64"
    )
    output_nullable = function in {"avg", "sum"}
    aggregate = {
        "input": "inner.v",
        "function": function,
        "output": "aggregate.value",
        "type": output_type,
        "nullable": output_nullable,
        "distinct": False,
        "unwrap": False,
    }
    if function == "avg":
        aggregate["state"] = {
            "sum_type": "Decimal(35,2)",
            "count_type": "Uint64",
            "nullable": True,
        }
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "Outer",
                    "columns": [
                        {"name": "k", "type": "Int64", "nullable": True},
                    ],
                    "unique_keys": [],
                },
                {
                    "name": "Inner",
                    "columns": [
                        {"name": "k", "type": "Int64", "nullable": True},
                        {"name": "v", "type": input_type, "nullable": True},
                    ],
                    "unique_keys": [],
                },
            ]
        },
        "plan": {
            "nodes": [
                {
                    "id": "outer_scan",
                    "op": "scan",
                    "table": "Outer",
                    "columns": [{"source": "k", "output": "outer.k"}],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "main_project",
                    "op": "project",
                    "input": "outer_scan",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "outer.k",
                            "expression": {
                                "kind": "column",
                                "column": "outer.k",
                            },
                        },
                        {
                            "output": "result",
                            "expression": {
                                "kind": "column",
                                "column": BINDING,
                            },
                        },
                    ],
                },
                {
                    "id": "inner_scan",
                    "op": "scan",
                    "table": "Inner",
                    "columns": [
                        {"source": "k", "output": "inner.k"},
                        {"source": "v", "output": "inner.v"},
                    ],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "outer_bind",
                    "op": "outer_bind",
                    "input": "inner_scan",
                    "dependency": "outer.k",
                    "type": "Int64",
                    "nullable": True,
                },
                {
                    "id": "correlation_filter",
                    "op": "filter",
                    "input": "outer_bind",
                    "predicate": _equality("outer.k", "inner.k"),
                },
                {
                    "id": "inner_project",
                    "op": "project",
                    "input": "correlation_filter",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "inner.k",
                            "expression": {
                                "kind": "column",
                                "column": "inner.k",
                            },
                        },
                        {
                            "output": "inner.v",
                            "expression": {
                                "kind": "column",
                                "column": "inner.v",
                            },
                        },
                        {
                            "output": "outer.k",
                            "expression": {
                                "kind": "column",
                                "column": "outer.k",
                            },
                        },
                    ],
                },
                {
                    "id": "scalar_aggregate",
                    "op": "aggregate",
                    "input": "inner_project",
                    "keys": [],
                    "aggregates": [aggregate],
                    "phase": "undefined",
                    "distinct_all": False,
                },
                {
                    "id": "scalar_result",
                    "op": "project",
                    "input": "scalar_aggregate",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "sub.value",
                            "expression": {
                                "kind": "column",
                                "column": "aggregate.value",
                            },
                        }
                    ],
                },
            ],
            "root": "main_project",
            "output": ["outer.k", "result"],
            "subplans": [
                {
                    "binding": BINDING,
                    "kind": "scalar",
                    "root": "scalar_result",
                    "output": {
                        "column": "sub.value",
                        "type": output_type,
                        "nullable": output_nullable,
                    },
                    "type": output_type,
                    "nullable": True,
                    "dependencies": ["outer.k"],
                    "consumers": ["main_project"],
                }
            ],
        },
        "stage_graph": None,
    }


def _lowered_scalar_snapshot(*, check_mode, empty_outer=False):
    if check_mode not in {"gated", "eager", "none"}:
        raise AssertionError(f"unknown scalar check mode {check_mode!r}")
    nodes = [{"id": "main_source", "op": "empty_source"}]
    outer = "main_source"
    if empty_outer:
        nodes.append(
            {
                "id": "empty_outer",
                "op": "filter",
                "input": outer,
                "predicate": _literal("Bool", False),
            }
        )
        outer = "empty_outer"
    nodes.append(
        {
            "id": "sub_scan",
            "op": "scan",
            "table": "A",
            "columns": [{"source": "x", "output": "sub.value"}],
            "predicate": None,
            "pushed_limit": None,
        }
    )
    checked_input = "sub_scan"
    if check_mode == "gated":
        nodes.extend(
            [
                {
                    "id": "outer_gate",
                    "op": "limit",
                    "input": outer,
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "scalar_bound",
                    "op": "limit",
                    "input": "sub_scan",
                    "count": _literal("Uint64", 2),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "gate_cross",
                    "op": "join",
                    "left": "outer_gate",
                    "right": "scalar_bound",
                    "kind": "cross",
                    "predicate": _literal("Bool", True),
                },
            ]
        )
        checked_input = "gate_cross"
    nodes.extend(
        [
            {
                "id": "checked",
                "op": "limit",
                "input": checked_input,
                "count": _literal("Uint64", 2),
                "offset": None,
                "phase": "undefined",
                "ensure_at_most_one": check_mode != "none",
            },
            {
                "id": "descriptor",
                "op": "project",
                "input": "checked",
                "ordered": False,
                "columns": [
                    {
                        "output": "sub.value",
                        "expression": {
                            "kind": "column",
                            "column": "sub.value",
                        },
                    }
                ],
            },
            {"id": "fallback_source", "op": "empty_source"},
            {
                "id": "fallback",
                "op": "project",
                "input": "fallback_source",
                "ordered": False,
                "columns": [
                    {
                        "output": "sub.value",
                        "expression": {"kind": "null", "type": "Int64"},
                    }
                ],
            },
            {
                "id": "value_or_null",
                "op": "union_all",
                "inputs": [
                    {"node": "descriptor", "columns": ["sub.value"]},
                    {"node": "fallback", "columns": ["sub.value"]},
                ],
                "output": ["sub.value"],
                "ordered": True,
            },
            {
                "id": "first",
                "op": "limit",
                "input": "value_or_null",
                "count": _literal("Uint64", 1),
                "offset": None,
                "phase": "undefined",
            },
            {
                "id": "cross",
                "op": "join",
                "left": outer,
                "right": "first",
                "kind": "cross",
                "predicate": _literal("Bool", True),
            },
            {
                "id": "result",
                "op": "project",
                "input": "cross",
                "ordered": False,
                "columns": [
                    {
                        "output": "result",
                        "expression": {
                            "kind": "column",
                            "column": "sub.value",
                        },
                    }
                ],
            },
        ]
    )
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": _scan_scalar_snapshot()["schema"],
        "plan": {
            "nodes": nodes,
            "root": "result",
            "output": ["result"],
            "subplans": [],
        },
        "stage_graph": None,
    }


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
        return _ground(term.arguments[0], constants) == _ground(
            term.arguments[1],
            constants,
        )
    if term.operation == "<":
        return _ground(term.arguments[0], constants) < _ground(
            term.arguments[1],
            constants,
        )
    if term.operation == "ite":
        branch = (
            term.arguments[1]
            if _ground(term.arguments[0], constants)
            else term.arguments[2]
        )
        return _ground(branch, constants)
    if term.operation == "+":
        return sum(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "-":
        return _ground(term.arguments[0], constants) - _ground(
            term.arguments[1],
            constants,
        )
    if term.operation == "*":
        return _ground(term.arguments[0], constants) * _ground(
            term.arguments[1],
            constants,
        )
    if term.operation == "div":
        return _ground(term.arguments[0], constants) // _ground(
            term.arguments[1],
            constants,
        )
    if term.operation == "mod":
        return _ground(term.arguments[0], constants) % _ground(
            term.arguments[1],
            constants,
        )
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _database_constants(database, present, values=(10, 20)):
    constants = {}
    for row, is_present, value in zip(
        database.witness["A"],
        present,
        values,
    ):
        constants[row.present.atom] = is_present
        constants[row.cells["x"].value.atom] = value
    return constants


def _exists_constants(database, outer, inner):
    constants = {}
    for row, outer_value, inner_value in zip(
        database.witness["A"],
        outer,
        inner,
    ):
        constants[row.present.atom] = True
        for name, value in (("x", outer_value), ("y", inner_value)):
            cell = row.cells[name]
            constants[cell.is_null.atom] = value is None
            constants[cell.value.atom] = 0 if value is None else value
    return constants


def _in_constants(
    database,
    outer,
    inner,
    *,
    outer_present=None,
    inner_present=None,
):
    if outer_present is None:
        outer_present = (True,) * len(outer)
    if inner_present is None:
        inner_present = (True,) * len(inner)
    constants = {}
    for table, values, present in (
        ("Outer", outer, outer_present),
        ("Inner", inner, inner_present),
    ):
        for row, value, is_present in zip(
            database.witness[table],
            values,
            present,
        ):
            constants[row.present.atom] = is_present
            constants[row.cells["k"].value.atom] = value
    return constants


def _string_in_constants(
    evaluator,
    database,
    outer,
    inner,
    *,
    outer_present=None,
    inner_present=None,
):
    script = evaluator.scalar.script
    for value in (*outer, *inner):
        script.string_atom(value)
    script.seal_string_order()
    rank_by_value = {
        value: rank
        for rank, value in script.string_literals.items()
    }
    constants = _in_constants(
        database,
        tuple(rank_by_value[value] for value in outer),
        tuple(rank_by_value[value] for value in inner),
        outer_present=outer_present,
        inner_present=inner_present,
    )
    return constants, rank_by_value


def _correlated_constants(
    database,
    outer,
    inner,
    *,
    outer_present=(True, True),
    inner_present=(True, True),
):
    constants = {}

    def bind_cell(cell, value):
        if cell.is_null.operation == "symbol":
            constants[cell.is_null.atom] = value is None
        if cell.value.operation == "symbol":
            constants[cell.value.atom] = 0 if value is None else value

    for row, present, key in zip(
        database.witness["Outer"],
        outer_present,
        outer,
    ):
        constants[row.present.atom] = present
        bind_cell(row.cells["k"], key)
    for row, present, (key, value) in zip(
        database.witness["Inner"],
        inner_present,
        inner,
    ):
        constants[row.present.atom] = present
        bind_cell(row.cells["k"], key)
        bind_cell(row.cells["v"], value)
    return constants


def _enabled_outcomes(family, constants):
    enabled = []
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
            if _ground(outcome.enabled, grounded):
                enabled.append((outcome, grounded))
    return tuple(enabled)


class ScalarSubplanEvaluationTest(unittest.TestCase):
    def test_present_scalar_value_is_injected_only_into_expression_scope(self):
        evaluator, relation = _evaluate(_base_snapshot())
        self.assertEqual(tuple(column.name for column in relation.columns), ("result",))
        self.assertEqual(set(relation.rows[0].values), {"result"})
        result = relation.rows[0].values["result"]
        self.assertEqual(result.type, "Int64")
        self.assertEqual(result.is_null, smt.FALSE)
        self.assertEqual(result.value, smt.int_value(7))
        self.assertEqual(set(evaluator.subplan_families), {BINDING})

    def test_nullable_scalar_value_remains_null(self):
        raw = _base_snapshot({"kind": "null", "type": "Int64"})
        _, relation = _evaluate(raw)
        result = relation.rows[0].values["result"]
        self.assertEqual(result.type, "Int64")
        self.assertEqual(result.is_null, smt.TRUE)

    def test_empty_scalar_relation_yields_typed_null(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"].insert(
            3,
            {
                "id": "sub_filter",
                "op": "filter",
                "input": "sub_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][4]["input"] = "sub_filter"
        _, relation = _evaluate(raw)
        result = relation.rows[0].values["result"]
        self.assertEqual(result.type, "Int64")
        self.assertEqual(result.is_null, smt.TRUE)

    def test_repeated_use_resolves_one_shared_scalar_value(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = {
            "kind": "add",
            "left": {"kind": "column", "column": BINDING},
            "right": {"kind": "column", "column": BINDING},
            "type": "Int64",
            "nullable": True,
        }
        observed = []
        evaluator, relation = _evaluate(
            raw,
            observer=lambda scope, node, family: observed.append(node),
        )
        result = relation.rows[0].values["result"]
        self.assertEqual(result.is_null, smt.FALSE)
        self.assertEqual(result.value, smt.int_value(14))
        self.assertEqual(observed.count("sub_value"), 1)
        self.assertEqual(len(evaluator.subplan_families), 1)

    def test_nondeterministic_binding_is_shared_across_consumers(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"][1]["columns"] = [
            {
                "output": "first",
                "expression": {"kind": "column", "column": BINDING},
            }
        ]
        raw["plan"]["nodes"].extend(
            [
                {
                    "id": "sub_one",
                    "op": "limit",
                    "input": "sub_scan",
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "main_second",
                    "op": "project",
                    "input": "main_project",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "eq",
                                "left": {"kind": "column", "column": "first"},
                                "right": {"kind": "column", "column": BINDING},
                            },
                        }
                    ],
                },
            ]
        )
        raw["plan"]["root"] = "main_second"
        raw["plan"]["output"] = ["result"]
        raw["plan"]["subplans"][0]["root"] = "sub_one"
        raw["plan"]["subplans"][0]["consumers"] = [
            "main_project",
            "main_second",
        ]

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = _enabled_outcomes(family, constants)

        self.assertEqual(len(enabled), 2)
        for outcome, grounded in enabled:
            self.assertFalse(_ground(outcome.error, grounded))
            self.assertTrue(
                _ground(
                    outcome.relation.rows[0].values["result"].value,
                    grounded,
                )
            )

    def test_enumerated_sequence_binding_is_shared_across_consumers(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"][1]["columns"] = [
            {
                "output": "first",
                "expression": {"kind": "column", "column": BINDING},
            }
        ]
        raw["plan"]["nodes"].extend(
            [
                {"id": "sub_empty_source", "op": "empty_source"},
                {
                    "id": "sub_empty",
                    "op": "filter",
                    "input": "sub_empty_source",
                    "predicate": _literal("Bool", False),
                },
                {
                    "id": "sub_empty_value",
                    "op": "project",
                    "input": "sub_empty",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "sub.value",
                            "expression": _literal("Int64", 0),
                        }
                    ],
                },
                {
                    "id": "sub_ordered",
                    "op": "union_all",
                    "inputs": [
                        {"node": "sub_scan", "columns": ["sub.value"]},
                        {"node": "sub_empty_value", "columns": ["sub.value"]},
                    ],
                    "output": ["sub.value"],
                    "ordered": True,
                },
                {
                    "id": "sub_one",
                    "op": "limit",
                    "input": "sub_ordered",
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                {
                    "id": "main_second",
                    "op": "project",
                    "input": "main_project",
                    "ordered": False,
                    "columns": [
                        {
                            "output": "result",
                            "expression": {
                                "kind": "eq",
                                "left": {"kind": "column", "column": "first"},
                                "right": {"kind": "column", "column": BINDING},
                            },
                        }
                    ],
                },
            ]
        )
        raw["plan"]["root"] = "main_second"
        raw["plan"]["output"] = ["result"]
        raw["plan"]["subplans"][0]["root"] = "sub_one"
        raw["plan"]["subplans"][0]["consumers"] = [
            "main_project",
            "main_second",
        ]

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = [
            outcome
            for outcome in family.outcomes
            if _ground(outcome.enabled, constants)
        ]

        self.assertEqual(len(enabled), 2)
        for outcome in enabled:
            self.assertFalse(_ground(outcome.error, constants))
            self.assertTrue(
                _ground(
                    outcome.relation.rows[0].values["result"].value,
                    constants,
                )
            )

    def test_global_aggregate_preserves_null_and_decimal_proof_metadata(self):
        raw = _global_decimal_sum_snapshot()
        evaluator, relation = _evaluate(raw, row_bound=2)
        scalar_value = relation.rows[0].values["result"]
        aggregate_value = (
            evaluator.node("sub_value")
            .certain()
            .rows[0]
            .values["sub.value"]
        )
        self.assertEqual(scalar_value.is_null, aggregate_value.is_null)
        self.assertEqual(scalar_value.value, aggregate_value.value)
        self.assertEqual(
            scalar_value.decimal_finite_abs_bound,
            aggregate_value.decimal_finite_abs_bound,
        )
        self.assertIsNotNone(scalar_value.decimal_finite_abs_bound)

    def test_filter_consumer_uses_binding_without_exposing_it(self):
        raw = _base_snapshot(_literal("Bool", False))
        raw["plan"]["nodes"][0] = {
            "id": "main_source",
            "op": "project",
            "input": "main_unit",
            "ordered": False,
            "columns": [
                {
                    "output": "carrier",
                    "expression": _literal("Int64", 1),
                }
            ],
        }
        raw["plan"]["nodes"].insert(
            0,
            {"id": "main_unit", "op": "empty_source"},
        )
        raw["plan"]["nodes"][2] = {
            "id": "main_project",
            "op": "filter",
            "input": "main_source",
            "predicate": {"kind": "column", "column": BINDING},
        }
        raw["plan"]["output"] = ["carrier"]
        _, relation = _evaluate(raw)
        self.assertEqual(tuple(column.name for column in relation.columns), ("carrier",))
        self.assertEqual(set(relation.rows[0].values), {"carrier"})
        self.assertEqual(relation.rows[0].present, smt.FALSE)

    def test_sort_and_limit_wrappers_over_one_candidate_are_deterministic(self):
        wrappers = (
            (
                "sort",
                {
                    "id": "sub_wrapper",
                    "op": "sort",
                    "input": "sub_value",
                    "order": [
                        {
                            "column": "sub.value",
                            "ascending": True,
                            "nulls_first": True,
                        }
                    ],
                    "limit": None,
                    "phase": "undefined",
                },
                False,
            ),
            (
                "limit-one",
                {
                    "id": "sub_wrapper",
                    "op": "limit",
                    "input": "sub_value",
                    "count": _literal("Uint64", 1),
                    "offset": None,
                    "phase": "undefined",
                },
                False,
            ),
            (
                "limit-offset-one",
                {
                    "id": "sub_wrapper",
                    "op": "limit",
                    "input": "sub_value",
                    "count": _literal("Uint64", 1),
                    "offset": _literal("Uint64", 1),
                    "phase": "undefined",
                },
                True,
            ),
        )
        for name, wrapper, expected_null in wrappers:
            with self.subTest(name=name):
                raw = _base_snapshot()
                raw["plan"]["nodes"].append(wrapper)
                raw["plan"]["subplans"][0]["root"] = "sub_wrapper"

                _, relation = _evaluate(raw)

                result = relation.rows[0].values["result"]
                self.assertEqual(result.is_null, smt.bool_value(expected_null))
                if not expected_null:
                    self.assertEqual(result.value, smt.int_value(7))

    def test_general_scalar_observes_null_value_and_multirow_error(self):
        raw = _scan_scalar_snapshot()
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        self.assertEqual(len(family.outcomes), 1)
        outcome = family.outcomes[0]
        result = outcome.relation.rows[0].values["result"]

        for present, error, is_null, value in (
            ((False, False), False, True, None),
            ((True, False), False, False, 10),
            ((False, True), False, False, 20),
            ((True, True), True, False, None),
        ):
            constants = _database_constants(database, present)
            with self.subTest(present=present):
                self.assertEqual(_ground(outcome.error, constants), error)
                if not error:
                    self.assertEqual(_ground(result.is_null, constants), is_null)
                    if value is not None:
                        self.assertEqual(_ground(result.value, constants), value)

    def test_scalar_error_is_suppressed_without_a_consumer_input_row(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][2]["input"] = "empty_outer"
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        outcome = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().outcomes[0]

        self.assertFalse(
            _ground(
                outcome.error,
                _database_constants(database, (True, True)),
            )
        )

    def test_inherited_scalar_error_is_observed_without_a_consumer_input_row(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "inner_cardinality_error",
                "op": "limit",
                "input": "sub_scan",
                "count": _literal("Uint64", 2),
                "offset": None,
                "phase": "undefined",
                "ensure_at_most_one": True,
            }
        )
        raw["plan"]["subplans"][0]["root"] = "inner_cardinality_error"
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][2]["input"] = "empty_outer"
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = [
            outcome
            for outcome in family.outcomes
            if _ground(outcome.enabled, constants)
        ]

        self.assertTrue(enabled)
        self.assertTrue(
            all(_ground(outcome.error, constants) for outcome in enabled)
        )

    def test_multiple_bindings_keep_inherited_and_local_errors_separate(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        raw["plan"]["nodes"][2]["input"] = "empty_outer"
        raw["plan"]["nodes"][2]["columns"][0]["expression"] = {
            "kind": "add",
            "left": {"kind": "column", "column": BINDING},
            "right": {"kind": "column", "column": "$scalar2"},
            "type": "Int64",
            "nullable": True,
        }
        raw["plan"]["nodes"].extend(
            [
                {
                    "id": "sub_scan_two",
                    "op": "scan",
                    "table": "A",
                    "columns": [{"source": "x", "output": "sub2.value"}],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "inner_cardinality_error",
                    "op": "limit",
                    "input": "sub_scan_two",
                    "count": _literal("Uint64", 2),
                    "offset": None,
                    "phase": "undefined",
                    "ensure_at_most_one": True,
                },
            ]
        )
        second = copy.deepcopy(raw["plan"]["subplans"][0])
        second.update(
            {
                "binding": "$scalar2",
                "root": "inner_cardinality_error",
                "output": {
                    "column": "sub2.value",
                    "type": "Int64",
                    "nullable": False,
                },
            }
        )
        raw["plan"]["subplans"].append(second)

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        family = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root()
        constants = _database_constants(database, (True, True))
        enabled = [
            outcome
            for outcome in family.outcomes
            if _ground(outcome.enabled, constants)
        ]

        self.assertTrue(enabled)
        self.assertTrue(
            all(_ground(outcome.error, constants) for outcome in enabled)
        )

    def test_scalar_error_is_consumer_eager_across_dead_if_branch(self):
        raw = _scan_scalar_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = {
            "kind": "if",
            "condition": _literal("Bool", False),
            "then": {"kind": "column", "column": BINDING},
            "else": _literal("Int64", 7),
            "type": "Int64",
            "nullable": True,
        }
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        outcome = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
        ).root().outcomes[0]

        self.assertTrue(
            _ground(
                outcome.error,
                _database_constants(database, (True, True)),
            )
        )

    def test_checked_scalar_lowering_matches_a_demanded_initial_binding(self):
        before = parse_snapshot(_scan_scalar_snapshot())
        after = parse_snapshot(
            _lowered_scalar_snapshot(check_mode="gated")
        )
        script = smt.Script()
        database = Database(before, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(
                before,
                database,
                scalar,
                choice_scope="before",
            ).root(),
            Evaluator(
                after,
                database,
                scalar,
                choice_scope="after",
            ).root(),
            scalar,
        )

        for present in (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ):
            with self.subTest(present=present):
                self.assertTrue(
                    _ground(equality, _database_constants(database, present))
                )

    def test_missing_and_eager_checks_are_counterexamples(self):
        before = parse_snapshot(_scan_scalar_snapshot())

        def equal_to(raw_after):
            script = smt.Script()
            database = Database(before, 2, script)
            scalar = ScalarEncoder(script)
            equality = family_equal(
                Evaluator(
                    before,
                    database,
                    scalar,
                    choice_scope="before",
                ).root(),
                Evaluator(
                    parse_snapshot(raw_after),
                    database,
                    scalar,
                    choice_scope="after",
                ).root(),
                scalar,
            )
            return _ground(
                equality,
                _database_constants(database, (True, True)),
            )

        self.assertFalse(
            equal_to(_lowered_scalar_snapshot(check_mode="none"))
        )

        empty_before_raw = _scan_scalar_snapshot()
        empty_before_raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        empty_before_raw["plan"]["nodes"][2]["input"] = "empty_outer"
        empty_before = parse_snapshot(empty_before_raw)
        script = smt.Script()
        database = Database(empty_before, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(empty_before, database, scalar).root(),
            Evaluator(
                parse_snapshot(
                    _lowered_scalar_snapshot(
                        check_mode="eager",
                        empty_outer=True,
                    )
                ),
                database,
                scalar,
            ).root(),
            scalar,
        )
        self.assertFalse(
            _ground(
                equality,
                _database_constants(database, (True, True)),
            )
        )

    def test_gated_check_matches_an_empty_consumer(self):
        before_raw = _scan_scalar_snapshot()
        before_raw["plan"]["nodes"].insert(
            1,
            {
                "id": "empty_outer",
                "op": "filter",
                "input": "main_source",
                "predicate": _literal("Bool", False),
            },
        )
        before_raw["plan"]["nodes"][2]["input"] = "empty_outer"
        before = parse_snapshot(before_raw)
        after = parse_snapshot(
            _lowered_scalar_snapshot(
                check_mode="gated",
                empty_outer=True,
            )
        )
        script = smt.Script()
        database = Database(before, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(
                before,
                database,
                scalar,
                choice_scope="before",
            ).root(),
            Evaluator(
                after,
                database,
                scalar,
                choice_scope="after",
            ).root(),
            scalar,
        )

        for present in (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ):
            with self.subTest(present=present):
                self.assertTrue(
                    _ground(
                        equality,
                        _database_constants(database, present),
                    )
                )


class CorrelatedScalarSubplanEvaluationTest(unittest.TestCase):
    @staticmethod
    def _evaluate(raw, row_bound=2):
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, row_bound, script)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script))
        return evaluator, database, evaluator.root()

    @staticmethod
    def _results(family, constants):
        self_or_outcome = family.outcomes[0]
        return [
            (
                _ground(row.values["result"].is_null, constants),
                _ground(row.values["result"].value, constants),
            )
            for row in self_or_outcome.relation.rows
            if _ground(row.present, constants)
        ]

    def test_count_is_evaluated_per_outer_row_with_strict_null_equality(self):
        evaluator, database, family = self._evaluate(
            _correlated_scalar_snapshot()
        )
        self.assertEqual(len(family.outcomes), 1)
        self.assertEqual(set(evaluator.scalar_outer_binds), {BINDING})

        cases = (
            (
                (1, 2),
                ((1, 7), (2, 8)),
                [(False, 1), (False, 1)],
            ),
            (
                (1, 3),
                ((1, 7), (2, 8)),
                [(False, 1), (False, 0)],
            ),
            (
                (None, 1),
                ((None, 9), (2, 8)),
                [(False, 0), (False, 0)],
            ),
        )
        for outer, inner, expected in cases:
            with self.subTest(outer=outer, inner=inner):
                constants = _correlated_constants(database, outer, inner)
                self.assertFalse(
                    _ground(family.outcomes[0].error, constants)
                )
                self.assertEqual(self._results(family, constants), expected)

    def test_decimal_avg_is_evaluated_per_outer_row(self):
        _, database, family = self._evaluate(
            _correlated_scalar_snapshot("avg")
        )
        constants = _correlated_constants(
            database,
            (1, 2),
            ((1, 100), (1, 300)),
        )
        outcome = family.outcomes[0]
        rows = [
            row
            for row in outcome.relation.rows
            if _ground(row.present, constants)
        ]
        self.assertFalse(_ground(outcome.error, constants))
        self.assertFalse(_ground(rows[0].values["result"].is_null, constants))
        self.assertEqual(
            _ground(rows[0].values["result"].value, constants),
            200,
        )
        self.assertTrue(_ground(rows[1].values["result"].is_null, constants))

    def test_repeated_binding_use_shares_one_row_value(self):
        raw = _correlated_scalar_snapshot()
        main = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "main_project"
        )
        main["columns"][1]["expression"] = {
            "kind": "add",
            "left": {"kind": "column", "column": BINDING},
            "right": {"kind": "column", "column": BINDING},
            "type": "Uint64",
            "nullable": True,
        }
        _, database, family = self._evaluate(raw)
        constants = _correlated_constants(
            database,
            (1, 2),
            ((1, 7), (2, 8)),
        )
        self.assertEqual(
            self._results(family, constants),
            [(False, 2), (False, 2)],
        )

    def test_correlated_inherited_error_is_row_gated_even_in_dead_branch(self):
        raw = _correlated_scalar_snapshot()
        main = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "main_project"
        )
        main["columns"][1]["expression"] = {
            "kind": "if",
            "condition": _literal("Bool", False),
            "then": {"kind": "column", "column": BINDING},
            "else": _literal("Uint64", 7),
            "type": "Uint64",
            "nullable": True,
        }

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, 2, script)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script))
        closed = evaluator.node("inner_scan")
        closed_outcome = closed.outcomes[0]
        evaluator.cache["inner_scan"] = RelationFamily(
            (
                Outcome(
                    closed_outcome.enabled,
                    closed_outcome.relation,
                    smt.TRUE,
                    closed_outcome.decisions,
                    closed_outcome.choices,
                ),
            )
        )
        family = evaluator.root()
        for outer_present, expected_error in (
            ((False, False), False),
            ((True, False), True),
        ):
            with self.subTest(outer_present=outer_present):
                constants = _correlated_constants(
                    database,
                    (1, 2),
                    ((1, 7), (2, 8)),
                    outer_present=outer_present,
                )
                self.assertEqual(
                    _ground(family.outcomes[0].error, constants),
                    expected_error,
                )

    def test_per_invocation_relational_alternatives_fail_closed(self):
        snapshot = parse_snapshot(_correlated_scalar_snapshot())
        script = smt.Script()
        database = Database(snapshot, 2, script)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script))
        closed = evaluator.node("inner_scan")
        evaluator.cache["inner_scan"] = RelationFamily(
            (closed.outcomes[0], closed.outcomes[0])
        )
        with self.assertRaisesRegex(
            RelationError,
            "closed input has per-invocation relational choices",
        ):
            evaluator.root()

    def test_correlated_sum_matches_grouped_left_join_lowering(self):
        initial_raw = _correlated_scalar_snapshot("sum")
        final_raw = {
            "format": initial_raw["format"],
            "version": initial_raw["version"],
            "schema": copy.deepcopy(initial_raw["schema"]),
            "plan": {
                "nodes": [
                    copy.deepcopy(initial_raw["plan"]["nodes"][0]),
                    copy.deepcopy(initial_raw["plan"]["nodes"][2]),
                    {
                        "id": "grouped",
                        "op": "aggregate",
                        "input": "inner_scan",
                        "keys": ["inner.k"],
                        "aggregates": [
                            {
                                "input": "inner.v",
                                "function": "sum",
                                "output": "grouped.value",
                                "type": "Int64",
                                "nullable": True,
                                "distinct": False,
                                "unwrap": False,
                            }
                        ],
                        "phase": "undefined",
                        "distinct_all": False,
                    },
                    {
                        "id": "decorrelated",
                        "op": "join",
                        "left": "outer_scan",
                        "right": "grouped",
                        "kind": "left",
                        "predicate": _equality("outer.k", "inner.k"),
                    },
                    {
                        "id": "result",
                        "op": "project",
                        "input": "decorrelated",
                        "ordered": False,
                        "columns": [
                            {
                                "output": "outer.k",
                                "expression": {
                                    "kind": "column",
                                    "column": "outer.k",
                                },
                            },
                            {
                                "output": "result",
                                "expression": {
                                    "kind": "column",
                                    "column": "grouped.value",
                                },
                            },
                        ],
                    },
                ],
                "root": "result",
                "output": ["outer.k", "result"],
                "subplans": [],
            },
            "stage_graph": None,
        }

        initial = parse_snapshot(initial_raw)
        final = parse_snapshot(final_raw)
        script = smt.Script()
        database = Database(initial, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(
                initial,
                database,
                scalar,
                choice_scope="initial",
            ).root(),
            Evaluator(
                final,
                database,
                scalar,
                choice_scope="final",
            ).root(),
            scalar,
        )
        for outer, inner in (
            ((1, 2), ((1, 5), (2, 7))),
            ((1, 3), ((1, 5), (1, 7))),
            ((None, 1), ((None, 5), (2, 7))),
        ):
            with self.subTest(outer=outer, inner=inner):
                self.assertTrue(
                    _ground(
                        equality,
                        _correlated_constants(database, outer, inner),
                    )
                )

    def test_correlated_pair_construction_is_bounded(self):
        with self.assertRaisesRegex(
            RelationError,
            "correlated scalar evaluation requires 16641 candidate-row pairs",
        ):
            self._evaluate(_correlated_scalar_snapshot(), row_bound=129)

    def test_correlated_pair_budget_is_cumulative_across_outer_outcomes(self):
        snapshot = parse_snapshot(_correlated_scalar_snapshot())
        script = smt.Script()
        database = Database(snapshot, 91, script)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script))
        outer = evaluator.node("outer_scan").outcomes[0]
        absent_outer = replace(
            outer,
            relation=replace(
                outer.relation,
                rows=tuple(
                    replace(row, present=smt.FALSE)
                    for row in outer.relation.rows
                ),
            ),
        )
        evaluator.cache["outer_scan"] = RelationFamily(
            (absent_outer, absent_outer)
        )
        with self.assertRaisesRegex(
            RelationError,
            "correlated scalar evaluation requires 16562 candidate-row pairs",
        ):
            evaluator.root()

    def test_correlated_invocations_share_one_validated_plan_context(self):
        snapshot = parse_snapshot(_correlated_scalar_snapshot())
        script = smt.Script()
        database = Database(snapshot, 2, script)
        with mock.patch.object(
            relation,
            "validate_snapshot",
            wraps=relation.validate_snapshot,
        ) as validate:
            Evaluator(snapshot, database, ScalarEncoder(script)).root()
        self.assertEqual(validate.call_count, 1)

    def test_correlated_invocation_scopes_distinguish_outer_outcomes_and_rows(self):
        snapshot = parse_snapshot(_correlated_scalar_snapshot())
        script = smt.Script()
        database = Database(snapshot, 2, script)
        events = []
        evaluator = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
            choice_scope="before:logical",
            node_observer=lambda *event: events.append(event),
        )
        outer = evaluator.node("outer_scan")
        disabled = Outcome(
            smt.FALSE,
            outer.outcomes[0].relation,
            outer.outcomes[0].error,
            outer.outcomes[0].decisions,
            outer.outcomes[0].choices,
        )
        evaluator.cache["outer_scan"] = RelationFamily(
            (outer.outcomes[0], disabled)
        )
        evaluator.root()
        invocations = [
            (scope, family)
            for scope, node, family in events
            if node == "outer_bind"
        ]
        self.assertEqual(
            [scope for scope, _family in invocations],
            [
                f"before:logical:correlated_scalar:"
                f"{BINDING}:outcome:0:row:0",
                f"before:logical:correlated_scalar:"
                f"{BINDING}:outcome:0:row:1",
                f"before:logical:correlated_scalar:"
                f"{BINDING}:outcome:1:row:0",
                f"before:logical:correlated_scalar:"
                f"{BINDING}:outcome:1:row:1",
            ],
        )
        self.assertTrue(
            all(
                outcome.enabled == smt.FALSE
                for _scope, family in invocations[2:]
                for outcome in family.outcomes
            )
        )


class CorrelatedScalarSubplanValidationTest(unittest.TestCase):
    def test_real_project_aggregate_project_filter_shape_is_admitted(self):
        # The exporter renders an untouched Map input as an identity projection.
        # That is passive dataflow, not an optimizer expression use.
        parse_snapshot(_correlated_scalar_snapshot())

    def test_dependency_outer_bind_and_consumer_contracts_are_exact(self):
        raw = _correlated_scalar_snapshot()
        raw["plan"]["subplans"][0]["dependencies"] = []
        with self.assertRaisesRegex(SnapshotError, "uncorrelated scalar root"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        raw["plan"]["subplans"][0]["dependencies"] = ["outer.other"]
        with self.assertRaisesRegex(SnapshotError, "disagrees with outer_bind"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        raw["plan"]["nodes"][3]["nullable"] = False
        with self.assertRaisesRegex(SnapshotError, "consumer input"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "second_outer_bind",
                "op": "outer_bind",
                "input": "inner_scan",
                "dependency": "outer.other",
                "type": "Int64",
                "nullable": True,
            }
        )
        raw["plan"]["nodes"][3]["input"] = "second_outer_bind"
        with self.assertRaisesRegex(SnapshotError, "exactly one outer_bind"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "second_consumer",
                "op": "project",
                "input": "main_project",
                "ordered": False,
                "columns": [
                    {
                        "output": "result2",
                        "expression": {
                            "kind": "column",
                            "column": BINDING,
                        },
                    }
                ],
            }
        )
        raw["plan"]["root"] = "second_consumer"
        raw["plan"]["output"] = ["result2"]
        raw["plan"]["subplans"][0]["consumers"].append("second_consumer")
        with self.assertRaisesRegex(SnapshotError, "exactly one consumer"):
            parse_snapshot(raw)

    def test_dependency_may_only_feed_the_correlation_filter(self):
        raw = _correlated_scalar_snapshot()
        aggregate = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "scalar_aggregate"
        )
        aggregate["aggregates"][0]["input"] = "outer.k"
        with self.assertRaisesRegex(
            SnapshotError,
            "may not aggregate its outer dependency",
        ):
            parse_snapshot(raw)

        for output, expression in (
            (
                "renamed.dependency",
                {"kind": "column", "column": "outer.k"},
            ),
            (
                "computed.dependency",
                {
                    "kind": "add",
                    "left": {"kind": "column", "column": "outer.k"},
                    "right": _literal("Int64", 0),
                    "type": "Int64",
                    "nullable": True,
                },
            ),
        ):
            with self.subTest(output=output):
                raw = _correlated_scalar_snapshot()
                project = next(
                    node for node in raw["plan"]["nodes"]
                    if node["id"] == "inner_project"
                )
                project["columns"].append(
                    {
                        "output": output,
                        "expression": expression,
                    }
                )
                with self.assertRaisesRegex(
                    SnapshotError,
                    "only in the correlation Filter",
                ):
                    parse_snapshot(raw)

    def test_outer_bind_may_not_fan_out_into_the_main_plan(self):
        raw = _correlated_scalar_snapshot()
        main_project = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "main_project"
        )
        raw["plan"]["nodes"].append(
            {
                "id": "main_union",
                "op": "union_all",
                "inputs": [
                    {"node": "outer_scan", "columns": ["outer.k"]},
                    {
                        "node": "correlation_filter",
                        "columns": ["outer.k"],
                    },
                ],
                "output": ["outer.k"],
                "ordered": False,
            }
        )
        main_project["input"] = "main_union"
        with self.assertRaisesRegex(
            SnapshotError,
            "outer_bind may not be reachable from the main plan",
        ):
            parse_snapshot(raw)

    def test_aggregate_path_and_row_selection_contracts_are_exact(self):
        raw = _correlated_scalar_snapshot()
        aggregate = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "scalar_aggregate"
        )
        aggregate["keys"] = ["inner.k"]
        with self.assertRaisesRegex(SnapshotError, "must be ungrouped"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        aggregate = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "scalar_aggregate"
        )
        aggregate["phase"] = "final"
        with self.assertRaisesRegex(SnapshotError, "undefined"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        raw["plan"]["nodes"].insert(
            3,
            {
                "id": "inner_limit",
                "op": "limit",
                "input": "inner_scan",
                "count": _literal("Uint64", 1),
                "offset": None,
                "phase": "undefined",
            },
        )
        outer_bind = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "outer_bind"
        )
        outer_bind["input"] = "inner_limit"
        with self.assertRaisesRegex(SnapshotError, "row selection"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        inner_scan = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "inner_scan"
        )
        inner_scan["pushed_limit"] = _literal("Uint64", 1)
        with self.assertRaisesRegex(SnapshotError, "row selection"):
            parse_snapshot(raw)

    def test_correlation_requires_one_plain_outer_inner_equality(self):
        for predicate, message in (
            (
                _equality("outer.k", "inner.k", null_safe=True),
                "non-null-safe column equality",
            ),
            (
                {
                    "kind": "and",
                    "args": [
                        _equality("outer.k", "inner.k"),
                        _equality("outer.k", "inner.k"),
                    ],
                },
                "exactly one dependency-bearing conjunct",
            ),
            (
                {
                    "kind": "eq",
                    "left": {
                        "kind": "add",
                        "left": {
                            "kind": "column",
                            "column": "outer.k",
                        },
                        "right": _literal("Int64", 0),
                        "type": "Int64",
                        "nullable": True,
                    },
                    "right": {
                        "kind": "column",
                        "column": "inner.k",
                    },
                },
                "non-null-safe column equality",
            ),
        ):
            with self.subTest(message=message):
                raw = _correlated_scalar_snapshot()
                correlation_filter = next(
                    node for node in raw["plan"]["nodes"]
                    if node["id"] == "correlation_filter"
                )
                correlation_filter["predicate"] = predicate
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_outer_bind_object_is_strict(self):
        raw = _correlated_scalar_snapshot()
        outer_bind = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "outer_bind"
        )
        del outer_bind["type"]
        with self.assertRaisesRegex(SnapshotError, "missing fields: type"):
            parse_snapshot(raw)

        raw = _correlated_scalar_snapshot()
        outer_bind = next(
            node for node in raw["plan"]["nodes"]
            if node["id"] == "outer_bind"
        )
        outer_bind["surprise"] = True
        with self.assertRaisesRegex(SnapshotError, "unknown fields: surprise"):
            parse_snapshot(raw)


class ExistsSubplanEvaluationTest(unittest.TestCase):
    @staticmethod
    def _evaluate(raw, row_bound=2):
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, row_bound, script)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script))
        return evaluator, database, evaluator.root()

    @staticmethod
    def _present_values(relation, constants):
        return [
            _ground(row.values["outer.x"].value, constants)
            for row in relation.rows
            if _ground(row.present, constants)
        ]

    def test_correlated_exists_uses_sql_true_without_multiplying_outer_rows(self):
        evaluator, database, family = self._evaluate(_exists_snapshot())
        relation = family.certain()
        for outer, inner in (((None, 2), (2, 2)), ((1, 2), (None, 2))):
            with self.subTest(outer=outer, inner=inner):
                constants = _exists_constants(database, outer, inner)
                self.assertEqual(
                    self._present_values(relation, constants),
                    [2],
                )
        self.assertEqual(len(relation.rows), 2)
        self.assertEqual(set(evaluator.subplan_families), {"$exists"})

    def test_uncorrelated_empty_nonempty_not_and_repeated_binding(self):
        cases = (
            (_exists_snapshot(correlated=False), [1, 2]),
            (
                _exists_snapshot(
                    correlated=False,
                    inner_present=False,
                ),
                [],
            ),
            (_exists_snapshot(negate=True), [1]),
            (_exists_snapshot(repeat_binding=True), [2]),
        )
        for raw, expected in cases:
            with self.subTest(expected=expected):
                _, database, family = self._evaluate(raw)
                constants = _exists_constants(database, (1, 2), (2, None))
                self.assertEqual(
                    self._present_values(family.certain(), constants),
                    expected,
                )

    def test_exists_and_not_exists_match_semi_and_anti_join(self):
        def lowered(raw, join_kind):
            result = copy.deepcopy(raw)
            descriptor = result["plan"]["subplans"][0]
            consumer = next(
                node
                for node in result["plan"]["nodes"]
                if node["id"] == "main_filter"
            )
            consumer.clear()
            consumer.update(
                {
                    "id": "main_filter",
                    "op": "join",
                    "left": "outer_scan",
                    "right": descriptor["root"],
                    "kind": join_kind,
                    "predicate": descriptor["predicate"],
                }
            )
            result["plan"]["subplans"] = []
            return result

        for negate, join_kind in ((False, "left_semi"), (True, "left_anti")):
            with self.subTest(join_kind=join_kind):
                raw = _exists_snapshot(negate=negate)
                before = parse_snapshot(raw)
                after = parse_snapshot(lowered(raw, join_kind))
                script = smt.Script()
                database = Database(before, 2, script)
                scalar = ScalarEncoder(script)
                equality = family_equal(
                    Evaluator(before, database, scalar).root(),
                    Evaluator(after, database, scalar).root(),
                    scalar,
                )
                constants = _exists_constants(database, (1, 2), (2, 2))
                self.assertTrue(_ground(equality, constants))

    def test_complete_correlated_predicate_is_applied_per_inner_row(self):
        raw = _exists_snapshot()
        descriptor = raw["plan"]["subplans"][0]
        descriptor["predicate"] = {
            "kind": "and",
            "args": [
                descriptor["predicate"],
                {
                    "kind": "lt",
                    "left": _literal("Int64", 0),
                    "right": {"kind": "column", "column": "inner.x"},
                },
                {
                    "kind": "lt",
                    "left": {"kind": "column", "column": "inner.x"},
                    "right": _literal("Int64", 3),
                },
            ],
        }

        _, database, family = self._evaluate(raw)
        for outer, inner in (
            ((-1, 2), (-1, 2)),
            ((2, 4), (2, 4)),
            ((None, 2), (None, 2)),
        ):
            with self.subTest(outer=outer, inner=inner):
                constants = _exists_constants(database, outer, inner)
                self.assertEqual(
                    self._present_values(family.certain(), constants),
                    [2],
                )

    def test_scalar_and_correlated_exists_bindings_share_row_scope(self):
        raw = _exists_snapshot()
        scalar = _base_snapshot(_literal("Bool", True))
        raw["plan"]["nodes"].extend(scalar["plan"]["nodes"][2:])
        scalar_descriptor = scalar["plan"]["subplans"][0]
        scalar_descriptor["consumers"] = ["main_filter"]
        raw["plan"]["subplans"].append(scalar_descriptor)
        consumer = next(
            node
            for node in raw["plan"]["nodes"]
            if node["id"] == "main_filter"
        )
        consumer["predicate"] = {
            "kind": "and",
            "args": [
                consumer["predicate"],
                {"kind": "column", "column": BINDING},
            ],
        }

        _, database, family = self._evaluate(raw)
        constants = _exists_constants(database, (1, 2), (2, None))
        self.assertEqual(
            self._present_values(family.certain(), constants),
            [2],
        )

    def test_exists_row_pair_construction_is_bounded(self):
        with self.assertRaisesRegex(
            RelationError,
            "Boolean subplan evaluation requires 16641 candidate-row pairs",
        ):
            self._evaluate(_exists_snapshot(), row_bound=129)


class InSubplanEvaluationTest(unittest.TestCase):
    @staticmethod
    def _evaluate(raw, row_bound=3, observer=None):
        snapshot = parse_snapshot(raw)
        script = smt.Script()
        database = Database(snapshot, row_bound, script)
        evaluator = Evaluator(
            snapshot,
            database,
            ScalarEncoder(script),
            node_observer=observer,
        )
        return evaluator, database, evaluator.root()

    @staticmethod
    def _present_values(relation, constants):
        return [
            _ground(row.values["outer.k"].value, constants)
            for row in relation.rows
            if _ground(row.present, constants)
        ]

    def test_exact_membership_handles_duplicates_empty_not_and_repeated_use(self):
        domains = (
            ("Int32", (1, 2, 3), (2, 2, 9)),
            ("String", ("a", "b", "c"), ("b", "b", "z")),
        )
        cases = (
            (False, False, (True, True, True), (1,)),
            (False, False, (False, False, False), ()),
            (True, False, (True, True, True), (0, 2)),
            (False, True, (True, True, True), (1,)),
        )
        for scalar_type, outer, inner in domains:
            for negate, repeat_binding, inner_present, expected_indices in cases:
                with self.subTest(
                    scalar_type=scalar_type,
                    negate=negate,
                    repeat_binding=repeat_binding,
                    inner_present=inner_present,
                ):
                    observed = []
                    evaluator, database, family = self._evaluate(
                        _in_snapshot(
                            scalar_type=scalar_type,
                            negate=negate,
                            repeat_binding=repeat_binding,
                        ),
                        observer=lambda scope, node, value: observed.append(node),
                    )
                    if scalar_type == "String":
                        constants, rank_by_value = _string_in_constants(
                            evaluator,
                            database,
                            outer,
                            inner,
                            inner_present=inner_present,
                        )
                        expected = [
                            rank_by_value[outer[index]]
                            for index in expected_indices
                        ]
                    else:
                        constants = _in_constants(
                            database,
                            outer,
                            inner,
                            inner_present=inner_present,
                        )
                        expected = [
                            outer[index]
                            for index in expected_indices
                        ]
                    self.assertEqual(
                        self._present_values(family.certain(), constants),
                        expected,
                    )
                    self.assertEqual(set(evaluator.subplan_families), {IN_BINDING})
                    self.assertEqual(observed.count("inner_scan"), 1)

    def test_string_membership_matches_the_finite_reference_exhaustively(self):
        values = ("a", "b", "c")
        presence_vectors = tuple(product((False, True), repeat=2))
        for negate in (False, True):
            with self.subTest(negate=negate):
                evaluator, database, family = self._evaluate(
                    _in_snapshot(scalar_type="String", negate=negate),
                    row_bound=2,
                )
                script = evaluator.scalar.script
                for value in values:
                    script.string_atom(value)
                script.seal_string_order()
                representatives = script.string_literals
                rank_by_value = {
                    value: rank
                    for rank, value in representatives.items()
                }

                formula = script.render()
                upper = len(representatives)
                for table in ("Outer", "Inner"):
                    for row in database.witness[table]:
                        atom = row.cells["k"].value.atom
                        self.assertIn(
                            f"(assert (and (not (< {atom} 0)) (< {atom} {upper})))",
                            formula,
                        )

                for outer in product(values, repeat=2):
                    outer_ranks = tuple(rank_by_value[value] for value in outer)
                    for inner in product(values, repeat=2):
                        inner_ranks = tuple(rank_by_value[value] for value in inner)
                        for outer_present in presence_vectors:
                            for inner_present in presence_vectors:
                                constants = _in_constants(
                                    database,
                                    outer_ranks,
                                    inner_ranks,
                                    outer_present=outer_present,
                                    inner_present=inner_present,
                                )
                                actual = [
                                    representatives[rank]
                                    for rank in self._present_values(
                                        family.certain(),
                                        constants,
                                    )
                                ]
                                expected = [
                                    value
                                    for index, value in enumerate(outer)
                                    if outer_present[index]
                                    and (
                                        any(
                                            present and value == candidate
                                            for candidate, present in zip(
                                                inner,
                                                inner_present,
                                            )
                                        )
                                        != negate
                                    )
                                ]
                                self.assertEqual(actual, expected)

    def test_in_and_not_in_match_semi_and_anti_join(self):
        for scalar_type, outer, inner in (
            ("Int32", (1, 2, 3), (2, 2, 9)),
            ("String", ("a", "b", "c"), ("b", "b", "z")),
        ):
            for negate, join_kind in ((False, "left_semi"), (True, "left_anti")):
                with self.subTest(
                    scalar_type=scalar_type,
                    join_kind=join_kind,
                ):
                    raw = _in_snapshot(
                        scalar_type=scalar_type,
                        negate=negate,
                    )
                    before = parse_snapshot(raw)
                    after = parse_snapshot(_lower_in_snapshot(raw, join_kind))
                    script = smt.Script()
                    database = Database(before, 3, script)
                    scalar = ScalarEncoder(script)
                    before_evaluator = Evaluator(before, database, scalar)
                    equality = family_equal(
                        before_evaluator.root(),
                        Evaluator(after, database, scalar).root(),
                        scalar,
                    )
                    for inner_present in (
                        (False, False, False),
                        (True, False, True),
                        (True, True, True),
                    ):
                        if scalar_type == "String":
                            constants, _ = _string_in_constants(
                                before_evaluator,
                                database,
                                outer,
                                inner,
                                inner_present=inner_present,
                            )
                        else:
                            constants = _in_constants(
                                database,
                                outer,
                                inner,
                                inner_present=inner_present,
                            )
                        self.assertTrue(_ground(equality, constants))

    def test_declared_lookup_mapping_is_semantically_observable(self):
        raw = _in_snapshot()
        raw["schema"]["tables"][0]["columns"].append(
            {"name": "other", "type": "Int32", "nullable": False}
        )
        raw["plan"]["nodes"][0]["columns"].append(
            {"source": "other", "output": "outer.other"}
        )
        raw["plan"]["subplans"][0]["lookup"]["column"] = "outer.other"

        lowered = copy.deepcopy(raw)
        consumer = lowered["plan"]["nodes"][2]
        consumer.clear()
        consumer.update(
            {
                "id": "main_filter",
                "op": "join",
                "left": "outer_scan",
                "right": "inner_scan",
                "kind": "left_semi",
                "keys": [{"left": "outer.k", "right": "inner.k"}],
                "predicate": _literal("Bool", True),
            }
        )
        lowered["plan"]["subplans"] = []

        before = parse_snapshot(raw)
        after = parse_snapshot(lowered)
        script = smt.Script()
        database = Database(before, 1, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            Evaluator(before, database, scalar).root(),
            Evaluator(after, database, scalar).root(),
            scalar,
        )
        constants = _in_constants(database, (1,), (1,))
        constants[
            database.witness["Outer"][0].cells["other"].value.atom
        ] = 2
        self.assertFalse(_ground(equality, constants))

    def test_inner_errors_are_inherited_even_for_an_empty_outer_relation(self):
        snapshot = parse_snapshot(_in_snapshot())
        script = smt.Script()
        database = Database(snapshot, 0, script)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script))
        inner = evaluator.node("inner_scan")
        self.assertEqual(len(inner.outcomes), 1)
        evaluator.cache["inner_scan"] = RelationFamily(
            (replace(inner.outcomes[0], error=smt.TRUE),)
        )

        outcome = evaluator.root().outcomes[0]

        self.assertEqual(outcome.error, smt.TRUE)

    def test_in_row_pair_construction_is_bounded(self):
        with self.assertRaisesRegex(
            RelationError,
            "Boolean subplan evaluation requires 16641 candidate-row pairs",
        ):
            self._evaluate(_in_snapshot(), row_bound=129)

    def test_in_row_pair_construction_is_cumulative_across_outcomes(self):
        snapshot = parse_snapshot(_in_snapshot())
        script = smt.Script()
        database = Database(snapshot, 91, script)
        evaluator = Evaluator(snapshot, database, ScalarEncoder(script))
        inner = evaluator.node("inner_scan")
        evaluator.cache["inner_scan"] = RelationFamily(
            (inner.outcomes[0], inner.outcomes[0])
        )

        with self.assertRaisesRegex(
            RelationError,
            "Boolean subplan evaluation requires 16562 candidate-row pairs",
        ):
            evaluator.root()


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3 for solver tests")
class InSubplanSolverTest(unittest.TestCase):
    @staticmethod
    def _solve(before, after, row_bound=2):
        problem = build_logical_kernel_problem_for_tests(
            parse_snapshot(before),
            parse_snapshot(after),
            row_bound,
            10_000,
        )
        return solve(problem, SOLVER, row_bound, 10_000)

    def test_in_is_bounded_equivalent_to_left_semi(self):
        for scalar_type in ("Int32", "String"):
            with self.subTest(scalar_type=scalar_type):
                raw = _in_snapshot(scalar_type=scalar_type)

                result = self._solve(
                    raw,
                    _lower_in_snapshot(raw, "left_semi"),
                )

                self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_not_in_is_bounded_equivalent_to_left_anti(self):
        for scalar_type in ("Int32", "String"):
            with self.subTest(scalar_type=scalar_type):
                raw = _in_snapshot(
                    scalar_type=scalar_type,
                    negate=True,
                )

                result = self._solve(
                    raw,
                    _lower_in_snapshot(raw, "left_anti"),
                )

                self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_wrong_lookup_lowering_has_a_solver_counterexample(self):
        for scalar_type in ("Int32", "String"):
            with self.subTest(scalar_type=scalar_type):
                raw = _in_snapshot(scalar_type=scalar_type)
                raw["schema"]["tables"][0]["columns"].append(
                    {
                        "name": "other",
                        "type": scalar_type,
                        "nullable": False,
                    }
                )
                raw["plan"]["nodes"][0]["columns"].append(
                    {"source": "other", "output": "outer.other"}
                )
                raw["plan"]["subplans"][0]["lookup"]["column"] = "outer.other"
                lowered = _lower_in_snapshot(raw, "left_semi")
                lowered["plan"]["nodes"][2]["keys"][0]["left"] = "outer.k"

                result = self._solve(raw, lowered, row_bound=1)

                self.assertEqual(result.status, "COUNTEREXAMPLE")
                self.assertIsNotNone(result.witness)


class ScalarSubplanValidationTest(unittest.TestCase):
    def test_legacy_plan_without_subplans_defaults_to_empty(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = _literal(
            "Int64",
            7,
        )
        del raw["plan"]["nodes"][2:]
        del raw["plan"]["subplans"]

        snapshot = parse_snapshot(raw)

        self.assertEqual(snapshot.plan.subplans, ())

    def test_descriptor_and_output_objects_are_strict(self):
        for target in ("descriptor", "output"):
            with self.subTest(target=target):
                raw = _base_snapshot()
                obj = (
                    raw["plan"]["subplans"][0]
                    if target == "descriptor"
                    else raw["plan"]["subplans"][0]["output"]
                )
                obj["surprise"] = True
                with self.assertRaisesRegex(SnapshotError, "unknown fields: surprise"):
                    parse_snapshot(raw)

    def test_descriptor_type_and_nullability_are_exact(self):
        mutations = (
            ("type", "Uint64", "must exactly match"),
            ("nullable", False, "zero rows yield NULL"),
        )
        for field, value, message in mutations:
            with self.subTest(field=field):
                raw = _base_snapshot()
                raw["plan"]["subplans"][0][field] = value
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_scalar_dependencies_and_unknown_kinds_fail_closed(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["dependencies"] = ["outer.x"]
        with self.assertRaisesRegex(SnapshotError, "correlated scalar"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["kind"] = "tuple"
        with self.assertRaisesRegex(SnapshotError, "unsupported subplan kind"):
            parse_snapshot(raw)

    def test_consumers_are_exact_project_or_filter_references(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = []
        with self.assertRaisesRegex(SnapshotError, "binding is unused"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["expression"] = _literal("Int64", 0)
        with self.assertRaisesRegex(SnapshotError, "does not reference binding"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = ["main_source"]
        with self.assertRaisesRegex(SnapshotError, "Project or Filter"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = ["missing"]
        with self.assertRaisesRegex(SnapshotError, "unknown node"):
            parse_snapshot(raw)

    def test_undeclared_binding_reference_is_rejected(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"].insert(
            2,
            {
                "id": "extra_project",
                "op": "project",
                "input": "main_source",
                "ordered": False,
                "columns": [
                    {
                        "output": "extra",
                        "expression": {"kind": "column", "column": BINDING},
                    }
                ],
            },
        )
        raw["plan"]["nodes"].append(
            {
                "id": "main_union",
                "op": "union_all",
                "inputs": [
                    {"node": "main_project", "columns": ["result"]},
                    {"node": "extra_project", "columns": ["extra"]},
                ],
                "output": ["result"],
                "ordered": False,
            }
        )
        raw["plan"]["root"] = "main_union"
        with self.assertRaisesRegex(SnapshotError, "column '\\$scalar' is not available"):
            parse_snapshot(raw)

    def test_duplicate_and_input_colliding_bindings_are_rejected(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"].append(
            copy.deepcopy(raw["plan"]["subplans"][0])
        )
        with self.assertRaisesRegex(SnapshotError, "duplicate name"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"][0] = {
            "id": "main_source",
            "op": "project",
            "input": "main_unit",
            "ordered": False,
            "columns": [
                {
                    "output": BINDING,
                    "expression": _literal("Int64", 1),
                }
            ],
        }
        raw["plan"]["nodes"].append(
            {"id": "main_unit", "op": "empty_source"}
        )
        with self.assertRaisesRegex(SnapshotError, "collides with an input column"):
            parse_snapshot(raw)

    def test_binding_cannot_leak_into_main_relational_output(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][1]["columns"][0]["output"] = BINDING
        raw["plan"]["output"] = [BINDING]
        with self.assertRaisesRegex(SnapshotError, "must remain virtual"):
            parse_snapshot(raw)

    def test_output_schema_and_reachability_are_exact(self):
        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["output"]["column"] = "missing"
        with self.assertRaisesRegex(SnapshotError, "is not produced by its root"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["output"]["nullable"] = True
        with self.assertRaisesRegex(SnapshotError, "does not match its root"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["subplans"][0]["type"] = "Uint64"
        raw["plan"]["subplans"][0]["output"]["type"] = "Uint64"
        with self.assertRaisesRegex(SnapshotError, "does not match its root"):
            parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"].append(
            {"id": "orphan", "op": "empty_source"}
        )
        with self.assertRaisesRegex(SnapshotError, "nodes are not reachable"):
            parse_snapshot(raw)

    def test_scalar_root_may_retain_non_result_columns(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][2] = {
            "id": "sub_source",
            "op": "project",
            "input": "sub_unit",
            "ordered": False,
            "columns": [
                {
                    "output": "sub.auxiliary",
                    "expression": _literal("Int64", 9),
                }
            ],
        }
        raw["plan"]["nodes"].append(
            {"id": "sub_unit", "op": "empty_source"}
        )
        raw["plan"]["nodes"][3]["columns"].insert(
            0,
            {
                "output": "sub.auxiliary",
                "expression": {
                    "kind": "column",
                    "column": "sub.auxiliary",
                },
            },
        )

        evaluator, relation = _evaluate(raw)
        self.assertEqual(
            tuple(evaluator.schemas["sub_value"]),
            ("sub.auxiliary", "sub.value"),
        )
        result = relation.rows[0].values["result"]
        self.assertEqual(result.is_null, smt.FALSE)
        self.assertEqual(result.value, smt.int_value(7))

    def test_subplans_must_not_survive_into_stage_graph(self):
        raw = _base_snapshot()
        raw["stage_graph"] = {
            "root_stage": "ignored",
            "stages": [],
            "edges": [],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "fully eliminated"):
            parse_snapshot(raw)

    def test_nested_subplan_binding_reference_is_rejected(self):
        raw = _base_snapshot()
        raw["plan"]["nodes"][3]["columns"][0]["expression"] = {
            "kind": "column",
            "column": BINDING,
        }
        raw["plan"]["subplans"][0]["output"]["nullable"] = True
        raw["plan"]["subplans"][0]["consumers"].append("sub_value")
        with self.assertRaisesRegex(
            SnapshotError,
            "subplan expressions may not reference",
        ):
            parse_snapshot(raw)

    def test_general_scalar_shapes_pass_schema_validation(self):
        raw = _base_snapshot()
        raw["schema"]["tables"] = [
            {
                "name": "A",
                "columns": [
                    {"name": "x", "type": "Int64", "nullable": False}
                ],
                "unique_keys": [],
            }
        ]
        raw["plan"]["nodes"][2:] = [
            {
                "id": "sub_value",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "x", "output": "sub.value"}],
                "predicate": None,
                "pushed_limit": None,
            }
        ]
        parse_snapshot(raw)

        raw = _global_decimal_sum_snapshot()
        raw["plan"]["nodes"][-1]["phase"] = "intermediate"
        parse_snapshot(raw)

        raw = _base_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "sub_limit",
                "op": "limit",
                "input": "sub_value",
                "count": _literal("Uint64", 2),
                "offset": None,
                "phase": "undefined",
            }
        )
        raw["plan"]["subplans"][0]["root"] = "sub_limit"
        parse_snapshot(raw)

        for operation in ("join", "union_all"):
            with self.subTest(operation=operation):
                raw = _base_snapshot()
                if operation == "join":
                    raw["plan"]["nodes"][3] = {
                        "id": "sub_value",
                        "op": "join",
                        "left": "sub_left",
                        "right": "sub_right",
                        "kind": "left_semi",
                        "predicate": _literal("Bool", True),
                    }
                    raw["plan"]["nodes"].extend(
                        [
                            {
                                "id": "sub_left",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "sub.value",
                                        "expression": _literal("Int64", 1),
                                    }
                                ],
                            },
                            {
                                "id": "sub_right",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "right.value",
                                        "expression": _literal("Int64", 2),
                                    }
                                ],
                            },
                        ]
                    )
                else:
                    raw["plan"]["nodes"][3] = {
                        "id": "sub_value",
                        "op": "union_all",
                        "inputs": [
                            {"node": "sub_left", "columns": ["value"]},
                            {"node": "sub_right", "columns": ["value"]},
                        ],
                        "output": ["sub.value"],
                        "ordered": False,
                    }
                    raw["plan"]["nodes"].extend(
                        [
                            {
                                "id": "sub_left",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "value",
                                        "expression": _literal("Int64", 1),
                                    }
                                ],
                            },
                            {
                                "id": "sub_right",
                                "op": "project",
                                "input": "sub_source",
                                "ordered": False,
                                "columns": [
                                    {
                                        "output": "value",
                                        "expression": _literal("Int64", 2),
                                    }
                                ],
                            },
                        ]
                    )
                parse_snapshot(raw)

    def test_limit_one_shape_preserves_runtime_choices(self):
        raw = _base_snapshot()
        raw["schema"]["tables"] = [
            {
                "name": "A",
                "columns": [
                    {"name": "x", "type": "Int64", "nullable": False}
                ],
                "unique_keys": [],
            }
        ]
        raw["plan"]["nodes"][2:] = [
            {
                "id": "sub_scan",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "x", "output": "sub.value"}],
                "predicate": None,
                "pushed_limit": None,
            },
            {
                "id": "sub_value",
                "op": "limit",
                "input": "sub_scan",
                "count": _literal("Uint64", 1),
                "offset": None,
                "phase": "undefined",
            },
        ]
        deterministic_empty = copy.deepcopy(raw)
        deterministic_empty["plan"]["nodes"][-1]["count"] = _literal(
            "Uint64",
            0,
        )
        _, relation = _evaluate(deterministic_empty, row_bound=2)
        self.assertEqual(
            relation.rows[0].values["result"].is_null,
            smt.TRUE,
        )

        snapshot = parse_snapshot(raw)
        script = smt.Script()
        evaluator = Evaluator(
            snapshot,
            Database(snapshot, 2, script),
            ScalarEncoder(script),
        )
        family = evaluator.root()
        self.assertEqual(len(family.outcomes), 1)
        self.assertFalse(family.outcomes[0].decisions)
        self.assertEqual(len(family.outcomes[0].choices), 1)
        self.assertEqual(family.outcomes[0].choices[0].bound, 2)


class ExistsSubplanValidationTest(unittest.TestCase):
    def test_only_bool_nonnullable_descriptors_are_admitted(self):
        for field, value, message in (
            ("type", "Int64", "must have type 'Bool'"),
            ("nullable", True, "must be non-nullable"),
        ):
            with self.subTest(field=field):
                raw = _exists_snapshot()
                raw["plan"]["subplans"][0][field] = value
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

        raw = _exists_snapshot()
        raw["plan"]["subplans"][0]["output"] = {}
        with self.assertRaisesRegex(SnapshotError, "unknown fields: output"):
            parse_snapshot(raw)

    def test_dependency_and_predicate_contract_is_exact(self):
        for dependencies, predicate, message in (
            (["outer.x"], None, "exactly when EXISTS is correlated"),
            (
                [],
                _equality("outer.x", "inner.x"),
                "exactly when EXISTS is correlated",
            ),
            (
                ["outer.x", "outer.y"],
                _literal("Bool", True),
                "at most one outer dependency",
            ),
        ):
            with self.subTest(message=message):
                raw = _exists_snapshot()
                descriptor = raw["plan"]["subplans"][0]
                descriptor["dependencies"] = dependencies
                descriptor["predicate"] = predicate
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_correlation_requires_one_plain_outer_inner_column_equality(self):
        for predicate, message in (
            (_literal("Bool", True), "dependency-bearing conjunct"),
            (
                {
                    "kind": "and",
                    "args": [
                        _equality("outer.x", "inner.x"),
                        _equality("outer.x", "inner.x"),
                    ],
                },
                "exactly one dependency-bearing conjunct",
            ),
            (
                _equality("outer.x", "inner.x", null_safe=True),
                "non-null-safe column equality",
            ),
            (
                _equality("inner.x", "inner.x"),
                "dependency-bearing conjunct",
            ),
            (
                _equality("outer.x", "missing"),
                "is not produced by the subplan root",
            ),
        ):
            with self.subTest(message=message):
                raw = _exists_snapshot()
                raw["plan"]["subplans"][0]["predicate"] = predicate
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

        raw = _exists_snapshot()
        raw["schema"]["tables"][0]["columns"][1]["type"] = "Uint64"
        with self.assertRaisesRegex(SnapshotError, "types must match exactly"):
            parse_snapshot(raw)

    def test_consumer_and_error_outcome_shapes_fail_closed(self):
        raw = _exists_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = ["outer_scan"]
        with self.assertRaisesRegex(SnapshotError, "must be a Filter"):
            parse_snapshot(raw)

        raw = _exists_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "second_filter",
                "op": "filter",
                "input": "main_filter",
                "predicate": {"kind": "column", "column": "$exists"},
            }
        )
        raw["plan"]["root"] = "second_filter"
        raw["plan"]["subplans"][0]["consumers"].append("second_filter")
        with self.assertRaisesRegex(SnapshotError, "exactly one Filter consumer"):
            parse_snapshot(raw)

        raw = _exists_snapshot(limit_inner=True)
        limit = next(
            node for node in raw["plan"]["nodes"] if node["id"] == "inner_one"
        )
        limit["ensure_at_most_one"] = True
        with self.assertRaisesRegex(SnapshotError, "observable error outcomes"):
            parse_snapshot(raw)

    def test_correlated_row_selection_fails_closed(self):
        def with_sort(raw, *, limit):
            descriptor = raw["plan"]["subplans"][0]
            raw["plan"]["nodes"].insert(
                2,
                {
                    "id": "inner_sort",
                    "op": "sort",
                    "input": descriptor["root"],
                    "order": [
                        {
                            "column": "inner.x",
                            "ascending": True,
                            "nulls_first": True,
                        }
                    ],
                    "limit": None if limit is None else _literal("Uint64", limit),
                    "phase": "undefined",
                },
            )
            descriptor["root"] = "inner_sort"
            return raw

        with self.assertRaisesRegex(
            SnapshotError,
            "per-invocation row selection",
        ):
            parse_snapshot(_exists_snapshot(limit_inner=True))
        with self.assertRaisesRegex(
            SnapshotError,
            "per-invocation row selection",
        ):
            parse_snapshot(with_sort(_exists_snapshot(), limit=1))

        parse_snapshot(_exists_snapshot(correlated=False, limit_inner=True))
        parse_snapshot(
            with_sort(
                _exists_snapshot(correlated=False),
                limit=1,
            )
        )
        parse_snapshot(with_sort(_exists_snapshot(), limit=None))


class InSubplanValidationTest(unittest.TestCase):
    def test_descriptor_and_column_objects_are_strict_and_single_column(self):
        for target in ("descriptor", "lookup", "output"):
            with self.subTest(target=target):
                raw = _in_snapshot()
                descriptor = raw["plan"]["subplans"][0]
                obj = descriptor if target == "descriptor" else descriptor[target]
                obj["surprise"] = True
                with self.assertRaisesRegex(
                    SnapshotError,
                    "unknown fields: surprise",
                ):
                    parse_snapshot(raw)

        raw = _in_snapshot()
        raw["plan"]["subplans"][0]["lookup"] = [
            raw["plan"]["subplans"][0]["lookup"],
        ]
        with self.assertRaisesRegex(SnapshotError, "expected an object"):
            parse_snapshot(raw)

    def test_descriptor_is_bool_nonnullable_and_uncorrelated(self):
        for field, value, message in (
            ("type", "Int32", "IN binding must have type 'Bool'"),
            ("nullable", True, "IN binding must be non-nullable"),
            (
                "dependencies",
                ["outer.k"],
                "IN binding must be uncorrelated",
            ),
        ):
            with self.subTest(field=field):
                raw = _in_snapshot()
                raw["plan"]["subplans"][0][field] = value
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_lookup_and_output_require_the_same_supported_nonnullable_type(self):
        for scalar_type in ("Int32", "String"):
            with self.subTest(scalar_type=scalar_type, accepted=True):
                parse_snapshot(_in_snapshot(scalar_type=scalar_type))
            for target in ("lookup", "output"):
                with self.subTest(
                    scalar_type=scalar_type,
                    target=target,
                    nullable=True,
                ):
                    raw = _in_snapshot(scalar_type=scalar_type)
                    raw["plan"]["subplans"][0][target]["nullable"] = True
                    with self.assertRaisesRegex(SnapshotError, "requires non-null"):
                        parse_snapshot(raw)

        for lookup_type, output_type in (
            ("Int32", "Int64"),
            ("String", "Utf8"),
            ("String", "Int32"),
        ):
            with self.subTest(
                lookup_type=lookup_type,
                output_type=output_type,
            ):
                raw = _in_snapshot(scalar_type=lookup_type)
                raw["plan"]["subplans"][0]["output"]["type"] = output_type
                with self.assertRaisesRegex(
                    SnapshotError,
                    "types must match exactly",
                ):
                    parse_snapshot(raw)

        for scalar_type in ("Bool", "Utf8", "Date", "Decimal(7,2)"):
            with self.subTest(scalar_type=scalar_type):
                raw = _in_snapshot(scalar_type=scalar_type)
                with self.assertRaisesRegex(
                    SnapshotError,
                    "only fixed-width integral or String",
                ):
                    parse_snapshot(raw)

    def test_declared_columns_must_match_both_relation_scopes_exactly(self):
        mutations = (
            (
                lambda raw: raw["plan"]["subplans"][0]["lookup"].update(
                    column="missing"
                ),
                "lookup column is not available",
            ),
            (
                lambda raw: raw["schema"]["tables"][0]["columns"][0].update(
                    nullable=True
                ),
                "lookup schema does not match",
            ),
            (
                lambda raw: raw["plan"]["subplans"][0]["output"].update(
                    column="missing"
                ),
                "output column is not produced",
            ),
            (
                lambda raw: raw["schema"]["tables"][1]["columns"][0].update(
                    type="Int64"
                ),
                "output schema does not match",
            ),
        )
        for mutate, message in mutations:
            with self.subTest(message=message):
                raw = _in_snapshot()
                mutate(raw)
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(raw)

    def test_consumer_must_be_one_referencing_filter(self):
        raw = _in_snapshot()
        raw["plan"]["subplans"][0]["consumers"] = []
        with self.assertRaisesRegex(SnapshotError, "binding is unused"):
            parse_snapshot(raw)

        raw = _in_snapshot()
        raw["plan"]["nodes"][2]["predicate"] = _literal("Bool", True)
        with self.assertRaisesRegex(SnapshotError, "does not reference binding"):
            parse_snapshot(raw)

        raw = _in_snapshot()
        raw["plan"]["nodes"][2] = {
            "id": "main_filter",
            "op": "project",
            "input": "outer_scan",
            "ordered": False,
            "columns": [
                {
                    "output": "outer.k",
                    "expression": {
                        "kind": "column",
                        "column": "outer.k",
                    },
                },
                {
                    "output": "probe",
                    "expression": {
                        "kind": "column",
                        "column": IN_BINDING,
                    },
                },
            ],
        }
        with self.assertRaisesRegex(SnapshotError, "IN consumer must be a Filter"):
            parse_snapshot(raw)

        raw = _in_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "second_filter",
                "op": "filter",
                "input": "main_filter",
                "predicate": {
                    "kind": "column",
                    "column": IN_BINDING,
                },
            }
        )
        raw["plan"]["root"] = "second_filter"
        raw["plan"]["subplans"][0]["consumers"].append("second_filter")
        with self.assertRaisesRegex(
            SnapshotError,
            "exactly one Filter consumer",
        ):
            parse_snapshot(raw)

    def test_correlated_and_error_bearing_roots_fail_closed(self):
        raw = _in_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "inner_bind",
                "op": "outer_bind",
                "input": "inner_scan",
                "dependency": "outer.k",
                "type": "Int32",
                "nullable": False,
            }
        )
        raw["plan"]["subplans"][0]["root"] = "inner_bind"
        with self.assertRaisesRegex(
            SnapshotError,
            "uncorrelated IN root may not contain outer_bind",
        ):
            parse_snapshot(raw)

        raw = _in_snapshot()
        raw["plan"]["nodes"].append(
            {
                "id": "inner_checked",
                "op": "limit",
                "input": "inner_scan",
                "count": _literal("Uint64", 2),
                "offset": None,
                "phase": "undefined",
                "ensure_at_most_one": True,
            }
        )
        raw["plan"]["subplans"][0]["root"] = "inner_checked"
        with self.assertRaisesRegex(SnapshotError, "observable error outcomes"):
            parse_snapshot(raw)


if __name__ == "__main__":
    unittest.main()
