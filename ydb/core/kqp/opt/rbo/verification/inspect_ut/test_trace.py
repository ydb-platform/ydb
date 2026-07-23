import os
import unittest
from unittest import mock

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.inspector.trace import (
    Probes,
    _add_family,
    _family_json,
    prepare,
)
from ydb.core.kqp.opt.rbo.verification.inspector.plan import InspectionError
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Column, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    OrdinalChoice,
    Outcome,
    Relation,
    RelationFamily,
    Row,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    DecimalAverageState,
    Value,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
)


def _schema(scalar_type="Int64", nullable=False):
    return {
        "tables": [{
            "name": "A",
            "columns": [{"name": "x", "type": scalar_type, "nullable": nullable}],
            "unique_keys": [],
        }]
    }


def _scan(node="scan"):
    return {
        "id": node,
        "op": "scan",
        "table": "A",
        "columns": [{"source": "x", "output": "x"}],
        "predicate": None,
        "pushed_limit": None,
    }


def _project(expression, node="project"):
    return {
        "id": node,
        "op": "project",
        "input": "scan",
        "ordered": False,
        "columns": [{"output": "x", "expression": expression}],
    }


def _logical(expression=None, scalar_type="Int64", nullable=False):
    nodes = [_scan()]
    root = "scan"
    if expression is not None:
        nodes.append(_project(expression))
        root = "project"
    return parse_snapshot({
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": _schema(scalar_type, nullable),
        "plan": {"nodes": nodes, "root": root, "output": ["x"], "subplans": []},
        "stage_graph": None,
    })


def _staged(
    expression,
    split=False,
    connection="map",
    scalar_type="Int64",
    nullable=False,
):
    nodes = [_scan(), _project(expression)]
    if split:
        stages = [
            {
                "id": "source",
                "nodes": ["scan"],
                "inputs": [],
                "outputs": [{"index": 0, "node": "scan"}],
                "source_storage": "row",
            },
            {
                "id": "root",
                "nodes": ["project"],
                "inputs": ["scan"],
                "outputs": [{"index": 0, "node": "project"}],
                "source_storage": None,
            },
        ]
        edge = {
            "id": connection,
            "producer": "source",
            "consumer": "root",
            "occurrence": 0,
            "producer_output": 0,
            "consumer_input": 0,
            "kind": connection,
        }
        if connection == "hash_shuffle":
            edge.update(keys=["x"], hash_function="HashV1", use_spilling=False)
        edges = [edge]
    else:
        stages = [{
            "id": "root",
            "nodes": ["scan", "project"],
            "inputs": [],
            "outputs": [{"index": 0, "node": "project"}],
            "source_storage": "column",
        }]
        edges = []
    return parse_snapshot({
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": _schema(scalar_type, nullable),
        "plan": {
            "nodes": nodes,
            "root": "project",
            "output": ["x"],
            "subplans": [],
        },
        "stage_graph": {
            "root_stage": "root",
            "stages": stages,
            "edges": edges,
            "assumptions": [],
        },
    })


def _staged_limit():
    return parse_snapshot({
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": _schema(),
        "plan": {
            "nodes": [
                _scan(),
                _project(ZERO),
                {
                    "id": "limit",
                    "op": "limit",
                    "input": "project",
                    "count": {"kind": "literal", "type": "Uint64", "value": 1},
                    "offset": None,
                    "phase": "final",
                },
            ],
            "root": "limit",
            "output": ["x"],
            "subplans": [],
        },
        "stage_graph": {
            "root_stage": "root",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan", "project"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "project"}],
                    "source_storage": "column",
                },
                {
                    "id": "root",
                    "nodes": ["limit"],
                    "inputs": ["project"],
                    "outputs": [{"index": 0, "node": "limit"}],
                    "source_storage": None,
                },
            ],
            "edges": [{
                "id": "gather",
                "producer": "source",
                "consumer": "root",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "union_all",
                "parallel": False,
            }],
            "assumptions": [],
        },
    })


def _constant(value, staged, include_table):
    graph = None
    if staged:
        graph = {
            "root_stage": "root",
            "stages": [{
                "id": "root",
                "nodes": ["source", "project"],
                "inputs": [],
                "outputs": [{"index": 0, "node": "project"}],
                "source_storage": None,
            }],
            "edges": [],
            "assumptions": [],
        }
    return parse_snapshot({
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": _schema() if include_table else {"tables": []},
        "plan": {
            "nodes": [
                {"id": "source", "op": "empty_source"},
                {
                    "id": "project",
                    "op": "project",
                    "input": "source",
                    "ordered": False,
                    "columns": [{
                        "output": "x",
                        "expression": {
                            "kind": "literal",
                            "type": "Int64",
                            "value": value,
                        },
                    }],
                },
            ],
            "root": "project",
            "output": ["x"],
            "subplans": [],
        },
        "stage_graph": graph,
    })


COLUMN = {"kind": "column", "column": "x"}
ZERO = {"kind": "literal", "type": "Int64", "value": 0}
OPAQUE_STRING = {
    "kind": "opaque",
    "fingerprint": "nullable_string($0)",
    "type": "String",
    "nullable": True,
    "args": [COLUMN],
}
NULL_STRING = {"kind": "null", "type": "String"}


def _average_family(
    state: DecimalAverageState,
    is_null: smt.Term,
) -> RelationFamily:
    return RelationFamily((
        Outcome(
            smt.TRUE,
            Relation(
                (Column("average", "Decimal(12,2)", True),),
                (
                    Row(
                        smt.TRUE,
                        {
                            "average": Value(
                                "Decimal(12,2)",
                                is_null,
                                smt.ONE,
                                decimal_average_state=state,
                            )
                        },
                    ),
                ),
            ),
        ),
    ))


class ObserverTest(unittest.TestCase):
    def test_probe_interning_is_iterative_exact_and_bounded(self):
        left = smt.symbol("same_probe", smt.INT)
        right = smt.symbol("same_probe", smt.INT)
        for _ in range(2000):
            left = smt.Term(smt.INT, "deep_probe", (left, left))
            right = smt.Term(smt.INT, "deep_probe", (right, right))

        probes = Probes(smt.Script())
        probes.add(left)
        probes.add(right)
        self.assertEqual(len(probes._terms), 2)
        self.assertEqual(len(probes.requested), 1)
        self.assertIs(probes._bound[id(left)], probes._bound[id(right)])
        alias = probes._bound[id(left)]
        assert isinstance(alias.atom, str)
        self.assertEqual(probes.value(left, {alias.atom: 7}), 7)
        self.assertEqual(probes.value(right, {alias.atom: 7}), 7)
        with self.assertRaisesRegex(InspectionError, "after sealing"):
            probes.add(smt.ZERO)

        bounded = Probes(smt.Script())
        bounded.add(smt.symbol("first_probe", smt.INT))
        bounded.add(smt.symbol("second_probe", smt.INT))
        with (
            mock.patch(
                "ydb.core.kqp.opt.rbo.verification.inspector.trace.MAX_TRACE_PROBES",
                1,
            ),
            self.assertRaisesRegex(
                InspectionError,
                "1 unique-probe audit bound",
            ),
        ):
            bounded.seal()

    def test_read_only_observers_do_not_change_the_obligation(self):
        before = _logical(COLUMN)
        after = _staged(COLUMN, split=True)
        plain = build_problem(before, after, 1, 1_000).formula()
        nodes = []
        edges = []
        boundaries = []
        comparisons = []
        observed = build_problem(
            before,
            after,
            1,
            1_000,
            before_node_observer=lambda *event: nodes.append(event),
            after_node_observer=lambda *event: nodes.append(event),
            after_edge_observer=lambda *event: edges.append(event),
            boundary_observer=lambda *event: boundaries.append(event),
            comparison_observer=comparisons.append,
        ).formula()
        self.assertEqual(observed, plain)
        self.assertTrue(nodes)
        self.assertEqual(len(edges), 2)
        self.assertEqual(len(boundaries), 2)
        self.assertEqual(len(comparisons), 1)

    def test_source_scan_override_is_observed_once_per_task(self):
        prepared = prepare(_logical(COLUMN), _staged(COLUMN), 1, 1_000)
        scans = [
            event for event in prepared.observation.after.nodes
            if event.node == "scan"
        ]
        self.assertEqual(
            [event.scope for event in scans],
            ["stage:root:task:0", "stage:root:task:1"],
        )

    def test_connected_partitions_are_observed_per_consumer_task(self):
        for connection, tasks in (("map", (0, 1)), ("hash_shuffle", (0, 1)), ("broadcast", (0,))):
            with self.subTest(connection=connection):
                prepared = prepare(
                    _logical(COLUMN),
                    _staged(COLUMN, split=True, connection=connection),
                    1,
                    1_000,
                )
                self.assertEqual(
                    [
                        (event.edge.id, event.consumer_task)
                        for event in prepared.observation.after.edges
                    ],
                    [(connection, task) for task in tasks],
                )

    def test_every_simultaneously_enabled_outcome_is_retained(self):
        relation = Relation((Column("x", "Int64", False),), ())
        result = RelationFamily((
            Outcome(smt.TRUE, relation, (("tie", 0),)),
            Outcome(smt.TRUE, relation, (("tie", 1),)),
        ))
        probes = Probes(smt.Script())
        for outcome in result.outcomes:
            probes.add(outcome.enabled)
        rendered = _family_json(result, probes, {}, {})
        self.assertEqual(
            [outcome["index"] for outcome in rendered["outcomes"]],
            [0, 1],
        )

    def test_symbolic_ordinals_are_probed_and_rendered_in_model_order(self):
        first_ordinal = smt.symbol("first_ordinal", smt.INT)
        second_ordinal = smt.symbol("second_ordinal", smt.INT)
        relation = Relation(
            (Column("x", "Int64", False),),
            (
                Row(
                    smt.TRUE,
                    {"x": Value("Int64", smt.FALSE, smt.int_value(10))},
                ),
                Row(
                    smt.TRUE,
                    {"x": Value("Int64", smt.FALSE, smt.int_value(20))},
                ),
            ),
            sequence=True,
            ordinals=(first_ordinal, second_ordinal),
        )
        family = RelationFamily((
            Outcome(
                smt.TRUE,
                relation,
                choices=(
                    OrdinalChoice(first_ordinal, 2),
                    OrdinalChoice(second_ordinal, 2),
                ),
            ),
            Outcome(
                smt.TRUE,
                relation,
                decisions=(("alternative", 1),),
                choices=(
                    OrdinalChoice(first_ordinal, 2),
                    OrdinalChoice(second_ordinal, 2),
                ),
            ),
        ))
        probes = Probes(smt.Script())
        _add_family(probes, family)

        self.assertIn(first_ordinal, probes.requested)
        self.assertIn(second_ordinal, probes.requested)
        rendered = _family_json(
            family,
            probes,
            {"first_ordinal": 1, "second_ordinal": 0},
            {},
        )
        self.assertEqual(
            [outcome["index"] for outcome in rendered["outcomes"]],
            [0, 1],
        )
        self.assertEqual(
            [
                row["values"][0]["value"]
                for row in rendered["outcomes"][0]["rows"]
            ],
            [20, 10],
        )

    def test_decimal_average_state_is_probed_and_rendered_as_optional_tuple(self):
        state_sum = smt.symbol("average_state_sum", smt.INT)
        state_count = smt.symbol("average_state_count", smt.INT)
        state = DecimalAverageState(
            sum_type="Decimal(35,2)",
            sum=state_sum,
            count=state_count,
            finite_abs_bound=75,
            count_bound=2,
        )

        for is_null, expected_value, model in (
            (
                smt.FALSE,
                {"sum": 75, "count": 2},
                {"average_state_sum": 75, "average_state_count": 2},
            ),
            (smt.TRUE, None, {}),
        ):
            with self.subTest(is_null=is_null):
                family = _average_family(state, is_null)
                probes = Probes(smt.Script())
                _add_family(probes, family)

                self.assertIn(state_sum, probes.requested)
                self.assertIn(state_count, probes.requested)
                cell = _family_json(family, probes, model, {})[
                    "outcomes"
                ][0]["rows"][0]["values"][0]
                self.assertEqual(
                    cell["average_state"],
                    {
                        "sum_type": "Decimal(35,2)",
                        "count_type": "Uint64",
                        "value": expected_value,
                        "proof_bounds": {
                            "finite_sum_abs": 75,
                            "count": 2,
                        },
                    },
                )

    def test_decimal_average_state_cannot_be_silently_partially_rendered(self):
        state = DecimalAverageState(
            sum_type="Decimal(35,2)",
            sum=smt.symbol("unregistered_state_sum", smt.INT),
            count=smt.symbol("unregistered_state_count", smt.INT),
            finite_abs_bound=1,
            count_bound=1,
        )
        family = _average_family(state, smt.FALSE)
        probes = Probes(smt.Script())
        probes.add(smt.TRUE)
        probes.add(smt.FALSE)
        probes.add(smt.ONE)

        with self.assertRaisesRegex(InspectionError, "unregistered trace term"):
            _family_json(family, probes, {}, {})


@unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3 for solver tests")
class ConcreteTraceTest(unittest.TestCase):
    def test_counterexample_uses_one_model_for_witness_and_operator_trace(self):
        result = prepare(_logical(), _staged(ZERO), 1, 10_000).solve(SOLVER, 10_000)
        self.assertEqual(result["status"], "COUNTEREXAMPLE")
        self.assertTrue(result["mismatches"])
        self.assertEqual(len(result["witness"]["A"]), 1)

        after = result["trace"]["after"]
        scopes = {
            (item["scope"]["stage"], item["scope"]["task"])
            for item in after["operators"]
        }
        self.assertEqual(scopes, {("root", 0), ("root", 1)})
        absent = [
            row
            for item in after["operators"]
            for outcome in item["result"]["outcomes"]
            for row in outcome["rows"]
            if not row["present"]
        ]
        self.assertTrue(absent)
        self.assertTrue(all("values" not in row for row in absent))

        before_values = _present_values(result["trace"]["before"]["boundary"])
        after_values = _present_values(after["boundary"])
        self.assertNotEqual(before_values, after_values)

    def test_saved_verifier_database_is_fixed_while_trace_choices_are_resolved(self):
        witness = {"A": [{"x": 7}]}
        result = prepare(
            _logical(),
            _staged(ZERO),
            1,
            10_000,
            fixed_witness=witness,
        ).solve(SOLVER, 10_000)
        self.assertEqual(result["status"], "COUNTEREXAMPLE")
        self.assertEqual(result["witness"], witness)
        self.assertEqual(
            _present_values(result["trace"]["before"]["boundary"]),
            [(7,)],
        )

    def test_saved_database_without_the_mismatch_is_not_a_global_proof(self):
        result = prepare(
            _logical(),
            _staged(ZERO),
            1,
            10_000,
            fixed_witness={"A": [{"x": 0}]},
        ).solve(SOLVER, 10_000)
        self.assertEqual(result["status"], "WITNESS_NOT_REPRODUCED")
        self.assertNotIn("trace", result)

    def test_nullable_opaque_string_is_decoded_from_the_trace_model(self):
        result = prepare(
            _logical(OPAQUE_STRING, "String", True),
            _staged(NULL_STRING, scalar_type="String", nullable=True),
            1,
            10_000,
        ).solve(SOLVER, 10_000)
        self.assertEqual(result["status"], "COUNTEREXAMPLE")
        self.assertEqual(len(result["witness"]["A"]), 1)
        before = _present_values(result["trace"]["before"]["boundary"])
        after = _present_values(result["trace"]["after"]["boundary"])
        self.assertEqual(len(before), 1)
        self.assertIsInstance(before[0][0], str)
        self.assertEqual(after, [(None,)])

    def test_all_enabled_unordered_limit_choices_are_retained(self):
        result = prepare(_logical(ZERO), _staged_limit(), 2, 10_000).solve(
            SOLVER, 10_000
        )
        self.assertEqual(result["status"], "COUNTEREXAMPLE")
        self.assertEqual(len(result["witness"]["A"]), 2)
        outcomes = result["trace"]["after"]["boundary"]["outcomes"]
        self.assertEqual(len(outcomes), 2)
        self.assertTrue(all(outcome["decisions"] for outcome in outcomes))
        self.assertEqual(_present_values(result["trace"]["after"]["boundary"]), [(0,), (0,)])

    def test_constant_trace_needs_no_model_values(self):
        for include_table, witness in ((False, {}), (True, {"A": []})):
            with self.subTest(include_table=include_table):
                result = prepare(
                    _constant(1, False, include_table),
                    _constant(2, True, include_table),
                    0,
                    10_000,
                ).solve(SOLVER, 10_000)
                self.assertEqual(result["status"], "COUNTEREXAMPLE")
                self.assertEqual(result["witness"], witness)
                self.assertEqual(
                    _present_values(result["trace"]["before"]["boundary"]),
                    [(1,)],
                )
                self.assertEqual(
                    _present_values(result["trace"]["after"]["boundary"]),
                    [(2,)],
                )

    def test_equivalent_pair_has_no_concrete_trace(self):
        result = prepare(_logical(COLUMN), _staged(COLUMN), 1, 10_000).solve(
            SOLVER, 10_000
        )
        self.assertEqual(result["status"], "VERIFIED_BOUNDED")
        self.assertNotIn("trace", result)


def _present_values(result):
    return [
        tuple(cell["value"] for cell in row["values"])
        for outcome in result["outcomes"]
        for row in outcome["rows"]
        if row["present"]
    ]


if __name__ == "__main__":
    unittest.main()
