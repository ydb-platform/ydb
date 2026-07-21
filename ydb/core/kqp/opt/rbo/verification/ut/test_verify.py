import copy
import io
import os
import subprocess
import unittest
from contextlib import redirect_stderr, redirect_stdout
from itertools import product
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    SnapshotError,
    parse_snapshot,
    stage_task_counts,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import cli
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import verify as verifier
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    Problem,
    SchemaMismatch,
    SolverError,
    VerificationError,
    build_logical_kernel_problem_for_tests,
    build_problem,
    solve,
)


SOLVER = os.environ.get("RBO_Z3")


def schema():
    return {
        "tables": [
            {
                "name": "A",
                "columns": [
                    {"name": "k", "type": "Int64", "nullable": False},
                    {"name": "x", "type": "Int64", "nullable": False},
                ],
                "unique_keys": [],
            },
            {
                "name": "B",
                "columns": [
                    {"name": "k", "type": "Int64", "nullable": False},
                    {"name": "x", "type": "Int64", "nullable": False},
                ],
                "unique_keys": [],
            },
        ]
    }


SCAN_A = {
    "id": "a",
    "op": "scan",
    "table": "A",
    "columns": [
        {"source": "k", "output": "a.k"},
        {"source": "x", "output": "a.x"},
    ],
}
SCAN_B = {
    "id": "b",
    "op": "scan",
    "table": "B",
    "columns": [
        {"source": "k", "output": "b.k"},
        {"source": "x", "output": "b.x"},
    ],
}
KEY_EQUALITY = {
    "kind": "eq",
    "left": {"kind": "column", "column": "a.k"},
    "right": {"kind": "column", "column": "b.k"},
}
RESIDUAL = {
    "kind": "opaque",
    "fingerprint": "greater_than($0,$1)",
    "type": "Bool",
    "nullable": False,
    "args": [
        {"kind": "column", "column": "a.x"},
        {"kind": "column", "column": "b.x"},
    ],
}


def _stage_schema(*names):
    available = {table["name"]: table for table in schema()["tables"]}
    return {"tables": [copy.deepcopy(available[name]) for name in names]}


def _stage(
    stage_id,
    nodes,
    inputs,
    outputs,
    source_storage=None,
):
    return {
        "id": stage_id,
        "nodes": nodes,
        "inputs": inputs,
        "outputs": [
            {"index": index, "node": node}
            for index, node in enumerate(outputs)
        ],
        "source_storage": source_storage,
    }


def _edge(
    edge_id,
    producer,
    consumer,
    producer_output,
    consumer_input,
    kind,
    occurrence=0,
    **settings,
):
    return {
        "id": edge_id,
        "producer": producer,
        "consumer": consumer,
        "occurrence": occurrence,
        "producer_output": producer_output,
        "consumer_input": consumer_input,
        "kind": kind,
        **settings,
    }


def _snapshot_with_stage_graph(schema_value, nodes, root, output, stages=None, edges=None):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": schema_value,
        "plan": {"nodes": nodes, "root": root, "output": output},
        "stage_graph": None if stages is None else {
            "root_stage": stages[-1]["id"],
            "stages": stages,
            "edges": edges,
            "assumptions": [],
        },
    }


def _set_connection(edge, connection):
    common = {
        name: edge[name]
        for name in (
            "id",
            "producer",
            "consumer",
            "occurrence",
            "producer_output",
            "consumer_input",
        )
    }
    edge.clear()
    edge.update(common, **connection)


def passthrough_stage_snapshot(connection=None):
    project = {
        "id": "project",
        "op": "project",
        "input": "a",
        "columns": [
            {
                "output": "result",
                "expression": {"kind": "column", "column": "a.k"},
            }
        ],
    }
    nodes = [copy.deepcopy(SCAN_A), project]
    if connection is None:
        return parse_snapshot(
            _snapshot_with_stage_graph(_stage_schema("A"), nodes, "project", ["result"])
        )
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            nodes,
            "project",
            ["result"],
            [
                _stage("source", ["a"], [], ["a"], "row"),
                _stage("consumer", ["project"], ["a"], ["project"]),
            ],
            [_edge("edge", "source", "consumer", 0, 0, **connection)],
        )
    )


def local_join_stage_snapshot(left_connection=None, right_connection=None):
    join = {
        "id": "join",
        "op": "join",
        "left": "a",
        "right": "b",
        "kind": "inner",
        "predicate": copy.deepcopy(KEY_EQUALITY),
    }
    nodes = [copy.deepcopy(SCAN_A), copy.deepcopy(SCAN_B), join]
    if left_connection is None:
        return parse_snapshot(
            _snapshot_with_stage_graph(
                _stage_schema("A", "B"),
                nodes,
                "join",
                ["a.k", "b.k"],
            )
        )
    assert right_connection is not None
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A", "B"),
            nodes,
            "join",
            ["a.k", "b.k"],
            [
                _stage("left", ["a"], [], ["a"], "row"),
                _stage("right", ["b"], [], ["b"], "row"),
                _stage("join_stage", ["join"], ["a", "b"], ["join"]),
            ],
            [
                _edge("left_edge", "left", "join_stage", 0, 0, **left_connection),
                _edge("right_edge", "right", "join_stage", 0, 1, **right_connection),
            ],
        )
    )


def duplicate_edge_stage_value():
    union = {
        "id": "union",
        "op": "union_all",
        "inputs": [
            {"node": "a", "columns": ["a.k"]},
            {"node": "a", "columns": ["a.k"]},
        ],
        "output": ["result"],
    }
    return _snapshot_with_stage_graph(
        _stage_schema("A"),
        [copy.deepcopy(SCAN_A), union],
        "union",
        ["result"],
        [
            _stage("source", ["a"], [], ["a", "a"], "row"),
            _stage("consumer", ["union"], ["a", "a"], ["union"]),
        ],
        [
            _edge("first", "source", "consumer", 0, 0, "map", occurrence=0),
            _edge(
                "second",
                "source",
                "consumer",
                1,
                1,
                "union_all",
                occurrence=1,
                parallel=True,
            ),
        ],
    )


def serial_then_parallel_stage_snapshot(staged):
    gather = {
        "id": "gather",
        "op": "project",
        "input": "a",
        "columns": [
            {
                "output": "middle",
                "expression": {"kind": "column", "column": "a.k"},
            }
        ],
    }
    project = {
        "id": "project",
        "op": "project",
        "input": "gather",
        "columns": [
            {
                "output": "result",
                "expression": {"kind": "column", "column": "middle"},
            }
        ],
    }
    stages = None
    edges = None
    if staged:
        stages = [
            _stage("source", ["a"], [], ["a"], "row"),
            _stage("gather_stage", ["gather"], ["a"], ["gather"]),
            _stage("root", ["project"], ["gather"], ["project"]),
        ]
        edges = [
            _edge(
                "gather_edge",
                "source",
                "gather_stage",
                0,
                0,
                "union_all",
                parallel=False,
            ),
            _edge(
                "parallel_edge",
                "gather_stage",
                "root",
                0,
                0,
                "union_all",
                parallel=True,
            ),
        ]
    return parse_snapshot(
        _snapshot_with_stage_graph(
            _stage_schema("A"),
            [copy.deepcopy(SCAN_A), gather, project],
            "project",
            ["result"],
            stages,
            edges,
        )
    )


def right_join(predicate):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": schema(),
            "plan": {
                "nodes": [
                    SCAN_A,
                    SCAN_B,
                    {
                        "id": "join",
                        "op": "join",
                        "left": "a",
                        "right": "b",
                        "kind": "right",
                        "predicate": predicate,
                    },
                ],
                "root": "join",
                "output": ["a.x", "b.x"],
            },
            "stage_graph": None,
        }
    )


def union_snapshot(duplicate):
    if duplicate:
        nodes = [
            SCAN_A,
            {
                "id": "union",
                "op": "union_all",
                "inputs": [
                    {"node": "a", "columns": ["a.k"]},
                    {"node": "a", "columns": ["a.k"]},
                ],
                "output": ["u.k"],
            },
        ]
    else:
        nodes = [
            {
                "id": "a",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "k", "output": "u.k"}],
            }
        ]
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": schema(),
            "plan": {
                "nodes": nodes,
                "root": "union" if duplicate else "a",
                "output": ["u.k"],
            },
            "stage_graph": None,
        }
    )


def filtered_snapshot(predicate):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "T",
                        "columns": [{"name": "flag", "type": "Bool", "nullable": True}],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": [
                    {
                        "id": "scan",
                        "op": "scan",
                        "table": "T",
                        "columns": [{"source": "flag", "output": "t.flag"}],
                    },
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "scan",
                        "predicate": predicate,
                    },
                ],
                "root": "filter",
                "output": ["t.flag"],
            },
            "stage_graph": None,
        }
    )


def left_join_elimination_snapshot(with_join, right_key_is_unique):
    value = schema()
    value["tables"][1]["unique_keys"] = (
        [{"columns": ["k"], "nulls_distinct": False}] if right_key_is_unique else []
    )
    nodes = [SCAN_A]
    if with_join:
        nodes.extend(
            [
                SCAN_B,
                {
                    "id": "join",
                    "op": "join",
                    "left": "a",
                    "right": "b",
                    "kind": "left",
                    "predicate": KEY_EQUALITY,
                },
            ]
        )
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": value,
            "plan": {
                "nodes": nodes,
                "root": "join" if with_join else "a",
                "output": ["a.x"],
            },
            "stage_graph": None,
        }
    )


def constant_snapshot(value, output="result", scalar_type=None):
    scalar_type = scalar_type or ("String" if isinstance(value, str) else "Int64")
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {"tables": []},
            "plan": {
                "nodes": [
                    {"id": "source", "op": "empty_source"},
                    {
                        "id": "project",
                        "op": "project",
                        "input": "source",
                        "columns": [
                            {
                                "output": output,
                                "expression": {"kind": "literal", "type": scalar_type, "value": value},
                            }
                        ],
                    },
                ],
                "root": "project",
                "output": [output],
            },
            "stage_graph": None,
        }
    )


class SolverProtocolTest(unittest.TestCase):
    def test_solver_error_cannot_be_reported_as_verified(self):
        process = subprocess.CompletedProcess(
            args=["z3"],
            returncode=1,
            stdout="unsat\n",
            stderr="parser error\n",
        )
        with mock.patch.object(verifier, "_run_solver", return_value=process):
            with self.assertRaisesRegex(SolverError, "parser error"):
                solve(Problem(smt.Script(), {}), "z3", 0)

    def test_extra_solver_responses_cannot_be_reported_as_verified(self):
        for output in (
            "unsat\nsat\n",
            "unsat\nsuccess\n",
            'unsat\n(error\n"late")\n',
        ):
            process = subprocess.CompletedProcess(
                args=["z3"],
                returncode=0,
                stdout=output,
                stderr="",
            )
            with self.subTest(output=output):
                with mock.patch.object(verifier, "_run_solver", return_value=process):
                    with self.assertRaisesRegex(SolverError, "exactly one solver status"):
                        solve(Problem(smt.Script(), {}), "z3", 0)

    def test_extra_witness_response_is_rejected(self):
        with self.assertRaisesRegex(SolverError, "SAT and one get-value response"):
            verifier._get_values("sat\n((v_0 true))\nsat\n")

    def test_schema_mismatch_is_a_correctness_verdict(self):
        output = io.StringIO()
        with (
            mock.patch.object(cli, "load_snapshot", return_value=mock.sentinel.snapshot),
            mock.patch.object(cli, "build_problem", side_effect=SchemaMismatch("changed root")),
            redirect_stdout(output),
        ):
            exit_code = cli.main(["before.json", "after.json", "--emit-smt", "formula.smt2"])
        self.assertEqual(exit_code, 1)
        self.assertIn('"status": "SCHEMA_MISMATCH"', output.getvalue())
        self.assertIn('"task_bound": 2', output.getvalue())

    def test_root_nullability_is_part_of_the_contract(self):
        nullable = parse_snapshot(
            {
                "format": "ydb-rbo-semantic-snapshot",
                "version": 1,
                "schema": {"tables": []},
                "plan": {
                    "nodes": [
                        {"id": "source", "op": "empty_source"},
                        {
                            "id": "project",
                            "op": "project",
                            "input": "source",
                            "columns": [
                                {
                                    "output": "result",
                                    "expression": {
                                        "kind": "opaque",
                                        "fingerprint": "nullable_constant",
                                        "type": "Int64",
                                        "nullable": True,
                                        "args": [],
                                    },
                                }
                            ],
                        },
                    ],
                    "root": "project",
                    "output": ["result"],
                },
                "stage_graph": None,
            }
        )
        with self.assertRaisesRegex(SchemaMismatch, "root output nullability differs"):
            build_logical_kernel_problem_for_tests(constant_snapshot(1), nullable, 0)

    def test_root_output_name_is_part_of_the_contract(self):
        with self.assertRaisesRegex(SchemaMismatch, "root output names or order differ"):
            build_logical_kernel_problem_for_tests(
                constant_snapshot(1, "before_name"),
                constant_snapshot(1, "after_name"),
                0,
            )

    def test_exact_integer_width_is_part_of_the_contract(self):
        with self.assertRaisesRegex(SchemaMismatch, "root output type differs"):
            build_logical_kernel_problem_for_tests(
                constant_snapshot(1, scalar_type="Int32"),
                constant_snapshot(1, scalar_type="Int64"),
                0,
            )


class BoundaryContractTest(unittest.TestCase):
    def test_initial_boundary_must_be_logical(self):
        staged = passthrough_stage_snapshot({"kind": "map"})
        logical = passthrough_stage_snapshot()
        with self.assertRaisesRegex(VerificationError, "initial snapshot.*stage_graph:null"):
            build_problem(staged, logical, 1)

    def test_final_boundary_must_have_a_stage_graph(self):
        logical = passthrough_stage_snapshot()
        with self.assertRaisesRegex(VerificationError, "final snapshot.*non-null stage_graph"):
            build_problem(logical, logical, 1)

    def test_logical_kernel_escape_hatch_rejects_staged_snapshots(self):
        logical = passthrough_stage_snapshot()
        staged = passthrough_stage_snapshot({"kind": "map"})
        with self.assertRaisesRegex(VerificationError, "test comparisons require stage_graph:null"):
            build_logical_kernel_problem_for_tests(logical, staged, 1)

    def test_cli_enforces_boundary_roles(self):
        logical = passthrough_stage_snapshot()
        errors = io.StringIO()
        with (
            mock.patch.object(cli, "load_snapshot", side_effect=[logical, logical]),
            redirect_stderr(errors),
        ):
            exit_code = cli.main(
                ["initial.json", "final.json", "--emit-smt", "unused.smt2"]
            )
        self.assertEqual(exit_code, 2)
        self.assertIn('"status": "UNSUPPORTED"', errors.getvalue())
        self.assertIn("final snapshot", errors.getvalue())


class _MissingFunction(Exception):
    def __init__(self, key, sort):
        super().__init__(key)
        self.key = key
        self.sort = sort


def _restricted_domain_has_model(script):
    """Exhaust a tiny data domain and all Boolean/UF choices.

    This is deliberately test-only and independent of SMT-LIB rendering. It
    produces valid witnesses when true. False only means that this restricted
    domain has no witness; solver-backed tests establish actual UNSAT results.
    """

    root = smt.and_(*script.assertions)
    symbols = {}
    integer_literals = {0, 1}

    def collect(term):
        if term.operation == "symbol":
            symbols[term.atom] = term.sort
        elif term.operation == "int":
            integer_literals.add(term.atom)
        for argument in term.arguments:
            collect(argument)

    collect(root)
    names = tuple(sorted(symbols))
    domains = {smt.BOOL: (False, True), smt.INT: tuple(sorted(integer_literals))}

    def evaluate(term, constants, functions):
        if term.operation == "symbol":
            return constants[term.atom]
        if term.operation in {"bool", "int"}:
            return term.atom
        if term.operation == "not":
            return not evaluate(term.arguments[0], constants, functions)
        if term.operation == "and":
            return all(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation == "or":
            return any(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation == "=":
            return evaluate(term.arguments[0], constants, functions) == evaluate(
                term.arguments[1], constants, functions
            )
        if term.operation == "ite":
            branch = term.arguments[1] if evaluate(term.arguments[0], constants, functions) else term.arguments[2]
            return evaluate(branch, constants, functions)
        if term.operation == "+":
            return sum(evaluate(argument, constants, functions) for argument in term.arguments)
        if term.operation.startswith("f_"):
            key = (
                term.operation,
                tuple(evaluate(argument, constants, functions) for argument in term.arguments),
            )
            if key not in functions:
                raise _MissingFunction(key, term.sort)
            return functions[key]
        raise AssertionError(f"unsupported test SMT operation {term.operation!r}")

    def choose_functions(constants, functions):
        try:
            return evaluate(root, constants, functions) is True
        except _MissingFunction as missing:
            for value in domains[missing.sort]:
                functions[missing.key] = value
                if choose_functions(constants, functions):
                    return True
            del functions[missing.key]
            return False

    for values in product(*(domains[symbols[name]] for name in names)):
        if choose_functions(dict(zip(names, values)), {}):
            return True
    return False


class RestrictedModelSmokeTest(unittest.TestCase):
    def test_identical_join_has_no_restricted_model(self):
        snapshot = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        self.assertFalse(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(snapshot, snapshot, 1).script
            )
        )

    def test_dropped_right_join_filter_has_a_restricted_model(self):
        before = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        self.assertTrue(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(
                    before, right_join(KEY_EQUALITY), 1
                ).script
            )
        )

    def test_union_multiplicity_mutation_has_a_restricted_model(self):
        self.assertTrue(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(
                    union_snapshot(True), union_snapshot(False), 1
                ).script
            )
        )

    def test_null_and_false_filters_have_no_restricted_model(self):
        before = filtered_snapshot({"kind": "null", "type": "Bool"})
        after = filtered_snapshot({"kind": "literal", "type": "Bool", "value": False})
        self.assertFalse(
            _restricted_domain_has_model(
                build_logical_kernel_problem_for_tests(before, after, 1).script
            )
        )

    def test_catalog_key_controls_left_join_elimination(self):
        with_key = build_logical_kernel_problem_for_tests(
            left_join_elimination_snapshot(True, True),
            left_join_elimination_snapshot(False, True),
            2,
        )
        without_key = build_logical_kernel_problem_for_tests(
            left_join_elimination_snapshot(True, False),
            left_join_elimination_snapshot(False, False),
            2,
        )
        self.assertFalse(_restricted_domain_has_model(with_key.script))
        self.assertTrue(_restricted_domain_has_model(without_key.script))


class StageGraphRestrictedModelTest(unittest.TestCase):
    def test_two_task_map_and_hash_shuffle_preserve_rows(self):
        logical = passthrough_stage_snapshot()
        connections = [
            {"kind": "map"},
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
        ]
        for connection in connections:
            with self.subTest(kind=connection["kind"]):
                staged = passthrough_stage_snapshot(connection)
                self.assertFalse(
                    _restricted_domain_has_model(build_problem(logical, staged, 2).script)
                )

    def test_serial_and_parallel_union_all_connections_preserve_rows(self):
        logical = passthrough_stage_snapshot()
        for parallel in (False, True):
            with self.subTest(parallel=parallel):
                staged = passthrough_stage_snapshot(
                    {"kind": "union_all", "parallel": parallel}
                )
                self.assertFalse(
                    _restricted_domain_has_model(build_problem(logical, staged, 2).script)
                )

    def test_merge_order_survives_parse_but_evaluation_is_unsupported(self):
        order = [
            {"column": "a.k", "ascending": True, "nulls_first": False},
            {"column": "a.x", "ascending": False, "nulls_first": True},
        ]
        logical = passthrough_stage_snapshot()
        staged = passthrough_stage_snapshot({"kind": "merge", "order": order})

        edge = staged.stage_graph.edges[0]
        self.assertEqual(
            [
                (item.column, item.ascending, item.nulls_first)
                for item in edge.order
            ],
            [
                ("a.k", True, False),
                ("a.x", False, True),
            ],
        )
        with self.assertRaisesRegex(VerificationError, "Merge ordering.*not modeled"):
            build_problem(logical, staged, 2)

    def test_serial_gather_then_parallel_union_stays_single_task(self):
        logical = serial_then_parallel_stage_snapshot(False)
        staged = serial_then_parallel_stage_snapshot(True)

        self.assertEqual(
            stage_task_counts(staged),
            {"source": 2, "gather_stage": 1, "root": 1},
        )
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 2).script)
        )

    def test_matching_shuffle_key_and_hash_make_local_join_complete(self):
        logical = local_join_stage_snapshot()
        staged = local_join_stage_snapshot(
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {
                "kind": "hash_shuffle",
                "keys": ["b.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
        )
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 1).script)
        )

    def test_wrong_shuffle_key_or_hash_has_a_routing_witness(self):
        logical = local_join_stage_snapshot()
        left = {
            "kind": "hash_shuffle",
            "keys": ["a.k"],
            "hash_function": "HashV1",
            "use_spilling": False,
        }
        mutations = [
            {
                "kind": "hash_shuffle",
                "keys": ["b.x"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {
                "kind": "hash_shuffle",
                "keys": ["b.k"],
                "hash_function": "HashV2",
                "use_spilling": False,
            },
        ]
        for right in mutations:
            with self.subTest(right=right):
                mutated = local_join_stage_snapshot(left, right)
                self.assertTrue(
                    _restricted_domain_has_model(build_problem(logical, mutated, 1).script)
                )

    def test_broadcast_makes_cross_task_local_join_complete(self):
        logical = local_join_stage_snapshot()
        staged = local_join_stage_snapshot(
            {"kind": "map"},
            {"kind": "broadcast"},
        )
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 2).script)
        )

    def test_replacing_broadcast_with_map_has_a_witness(self):
        logical = local_join_stage_snapshot()
        mutated = local_join_stage_snapshot(
            {"kind": "map"},
            {"kind": "union_all", "parallel": True},
        )
        self.assertTrue(
            _restricted_domain_has_model(build_problem(logical, mutated, 2).script)
        )

    def test_duplicate_edges_use_distinct_occurrences_and_input_ordinals(self):
        staged_value = duplicate_edge_stage_value()
        staged = parse_snapshot(staged_value)
        logical_value = copy.deepcopy(staged_value)
        logical_value["stage_graph"] = None
        logical = parse_snapshot(logical_value)
        self.assertFalse(
            _restricted_domain_has_model(build_problem(logical, staged, 1).script)
        )

        duplicate_occurrence = copy.deepcopy(staged_value)
        duplicate_occurrence["stage_graph"]["edges"][1]["occurrence"] = 0
        with self.assertRaises(SnapshotError):
            parse_snapshot(duplicate_occurrence)

        duplicate_input = copy.deepcopy(staged_value)
        duplicate_input["stage_graph"]["edges"][1]["consumer_input"] = 0
        with self.assertRaises(SnapshotError):
            parse_snapshot(duplicate_input)

        reused_output = copy.deepcopy(staged_value)
        reused_output["stage_graph"]["edges"][1]["producer_output"] = 0
        with self.assertRaises(SnapshotError):
            parse_snapshot(reused_output)

        unused_output = copy.deepcopy(staged_value)
        unused_output["stage_graph"]["stages"][0]["outputs"].append(
            {"index": 2, "node": "a"}
        )
        with self.assertRaises(SnapshotError):
            parse_snapshot(unused_output)

    def test_connection_variants_reject_unknown_and_missing_fields(self):
        def value_with_variant(variant):
            value = duplicate_edge_stage_value()
            edges = value["stage_graph"]["edges"]
            _set_connection(edges[0], variant)
            if variant["kind"] in {"union_all", "merge"}:
                _set_connection(edges[1], variant)
            return value

        valid_variants = [
            {"kind": "map"},
            {"kind": "broadcast"},
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {"kind": "union_all", "parallel": False},
            {
                "kind": "merge",
                "order": [
                    {"column": "a.k", "ascending": True, "nulls_first": True}
                ],
            },
        ]
        required_settings = {
            "hash_shuffle": ("keys", "hash_function", "use_spilling"),
            "union_all": ("parallel",),
            "merge": ("order",),
        }
        for variant in valid_variants:
            with self.subTest(kind=variant["kind"], case="valid"):
                parse_snapshot(value_with_variant(variant))

            with self.subTest(kind=variant["kind"], case="unknown"):
                invalid = dict(variant, unmodelled=True)
                with self.assertRaisesRegex(SnapshotError, "unknown fields: unmodelled"):
                    parse_snapshot(value_with_variant(invalid))

            for field in required_settings.get(variant["kind"], ()):
                with self.subTest(kind=variant["kind"], case="missing", field=field):
                    invalid = dict(variant)
                    del invalid[field]
                    with self.assertRaisesRegex(SnapshotError, f"missing fields:.*{field}"):
                        parse_snapshot(value_with_variant(invalid))

    def test_stage_output_mapping_must_match_its_node_and_edge_index(self):
        mutations = []

        non_contiguous = duplicate_edge_stage_value()
        non_contiguous["stage_graph"]["stages"][0]["outputs"][0]["index"] = 1
        mutations.append(non_contiguous)

        foreign_node = duplicate_edge_stage_value()
        foreign_node["stage_graph"]["stages"][0]["outputs"][0]["node"] = "union"
        mutations.append(foreign_node)

        missing_output = duplicate_edge_stage_value()
        missing_output["stage_graph"]["edges"][0]["producer_output"] = 1
        mutations.append(missing_output)

        for value in mutations:
            with self.subTest(value=value):
                with self.assertRaises(SnapshotError):
                    parse_snapshot(value)

    def test_consumer_task_counts_match_executor_and_channel_constraints(self):
        broadcast_only = duplicate_edge_stage_value()
        for edge in broadcast_only["stage_graph"]["edges"]:
            _set_connection(edge, {"kind": "broadcast"})
        broadcast_snapshot = parse_snapshot(broadcast_only)
        self.assertEqual(stage_task_counts(broadcast_snapshot)["consumer"], 1)

        serial_with_map = duplicate_edge_stage_value()
        _set_connection(
            serial_with_map["stage_graph"]["edges"][1],
            {"kind": "union_all", "parallel": False},
        )

        two_maps = duplicate_edge_stage_value()
        for edge in two_maps["stage_graph"]["edges"]:
            _set_connection(edge, {"kind": "map"})

        for case, value, message in (
            ("serial_with_map", serial_with_map, "serial UnionAll"),
            ("two_maps", two_maps, "only one Map"),
        ):
            with self.subTest(case=case):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(value)


@unittest.skipUnless(SOLVER, "set RBO_Z3 to run solver integration tests")
class VerificationTest(unittest.TestCase):
    def test_matching_stage_shuffles_are_bounded_equivalent(self):
        logical = local_join_stage_snapshot()
        staged = local_join_stage_snapshot(
            {
                "kind": "hash_shuffle",
                "keys": ["a.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
            {
                "kind": "hash_shuffle",
                "keys": ["b.k"],
                "hash_function": "HashV1",
                "use_spilling": False,
            },
        )
        result = solve(build_problem(logical, staged, 1, 10_000), SOLVER, 1, 10_000)
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_wrong_stage_routing_has_a_solver_counterexample(self):
        logical = local_join_stage_snapshot()
        mutations = [
            (
                1,
                local_join_stage_snapshot(
                    {
                        "kind": "hash_shuffle",
                        "keys": ["a.k"],
                        "hash_function": "HashV1",
                        "use_spilling": False,
                    },
                    {
                        "kind": "hash_shuffle",
                        "keys": ["b.k"],
                        "hash_function": "HashV2",
                        "use_spilling": False,
                    },
                ),
            ),
            (
                2,
                local_join_stage_snapshot(
                    {"kind": "map"},
                    {"kind": "union_all", "parallel": True},
                ),
            ),
        ]
        for row_bound, staged in mutations:
            with self.subTest(row_bound=row_bound, staged=staged):
                result = solve(
                    build_problem(logical, staged, row_bound, 10_000),
                    SOLVER,
                    row_bound,
                    10_000,
                )
                self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_identical_plans_are_bounded_equivalent(self):
        snapshot = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        result = solve(
            build_logical_kernel_problem_for_tests(snapshot, snapshot, 1, 10_000),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_dropped_right_join_filter_has_a_witness(self):
        before = right_join({"kind": "and", "args": [KEY_EQUALITY, RESIDUAL]})
        after = right_join(KEY_EQUALITY)
        result = solve(
            build_logical_kernel_problem_for_tests(before, after, 1, 10_000),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)
        self.assertEqual(len(result.witness["B"]), 1)
        self.assertEqual(result.witness["A"][0]["k"], result.witness["B"][0]["k"])

    def test_dropping_union_all_branch_changes_multiplicity(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                union_snapshot(True), union_snapshot(False), 1, 10_000
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["A"]), 1)

    def test_null_and_false_filters_are_equivalent(self):
        null_filter = filtered_snapshot({"kind": "null", "type": "Bool"})
        false_filter = filtered_snapshot({"kind": "literal", "type": "Bool", "value": False})
        result = solve(
            build_logical_kernel_problem_for_tests(
                null_filter, false_filter, 1, 10_000
            ),
            SOLVER,
            1,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_unique_key_justifies_left_join_elimination(self):
        before = left_join_elimination_snapshot(True, True)
        after = left_join_elimination_snapshot(False, True)
        result = solve(
            build_logical_kernel_problem_for_tests(before, after, 2, 10_000),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(result.status, "VERIFIED_BOUNDED")

    def test_left_join_elimination_without_unique_key_has_a_witness(self):
        before = left_join_elimination_snapshot(True, False)
        after = left_join_elimination_snapshot(False, False)
        result = solve(
            build_logical_kernel_problem_for_tests(before, after, 2, 10_000),
            SOLVER,
            2,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(len(result.witness["B"]), 2)

    def test_constant_query_uses_one_empty_source_row(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                constant_snapshot(1), constant_snapshot(2), 0, 10_000
            ),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")
        self.assertEqual(result.witness, {})

    def test_string_escape_forms_remain_distinct(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                constant_snapshot("\\n"), constant_snapshot("\n"), 0, 10_000
            ),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")

    def test_unicode_literal_round_trips_through_z3(self):
        result = solve(
            build_logical_kernel_problem_for_tests(
                constant_snapshot("é"), constant_snapshot("u{e9}"), 0, 10_000
            ),
            SOLVER,
            0,
            10_000,
        )
        self.assertEqual(result.status, "COUNTEREXAMPLE")


if __name__ == "__main__":
    unittest.main()
