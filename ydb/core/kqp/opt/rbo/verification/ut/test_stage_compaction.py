import unittest
from itertools import product
from unittest.mock import patch

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import (
    decimal,
    relation as relation_model,
    smt,
    stages,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column,
    SortOrder,
    StageEdge,
    parse_snapshot,
    stage_task_counts,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator as RelationEvaluator,
    Occurrence,
    PartitionFact,
    Relation,
    Row,
    single,
    sort_family,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    DecimalSumState,
    Encoder as ScalarEncoder,
    IntegralAverageState,
    Value,
)


ABSENT = object()


def _uint64(value):
    return {"kind": "literal", "type": "Uint64", "value": value}


def _order_item(column, ascending=True, nulls_first=False):
    return {
        "column": column,
        "ascending": ascending,
        "nulls_first": nulls_first,
    }


def _unique_order_stage_snapshot(
    *,
    shuffle_keys=("a.k1",),
    final_aggregate=True,
    complete_alias=True,
):
    scan = {
        "id": "scan",
        "op": "scan",
        "table": "A",
        "columns": [
            {"source": "k1", "output": "a.k1"},
            {"source": "k2", "output": "a.k2"},
            {"source": "payload", "output": "a.payload"},
        ],
        "pushed_limit": None,
    }
    partial = {
        "id": "partial",
        "op": "aggregate",
        "input": "scan",
        "keys": ["a.k1", "a.k2"],
        "aggregates": [
            {
                "input": "a.payload",
                "function": "count",
                "output": "_state",
                "type": "Uint64",
                "nullable": False,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "intermediate",
        "distinct_all": False,
    }
    final = {
        "id": "final",
        "op": "aggregate",
        "input": "partial",
        "keys": ["a.k1", "a.k2"],
        "aggregates": [
            {
                "input": "_state",
                "function": "sum",
                "output": "row_count",
                "type": "Uint64",
                "nullable": False,
                "distinct": False,
                "unwrap": False,
            }
        ],
        "phase": "final",
        "distinct_all": False,
    }
    aggregate_output = "row_count" if final_aggregate else "_state"
    filter_node = {
        "id": "filter",
        "op": "filter",
        "input": "final" if final_aggregate else "partial",
        "predicate": {"kind": "literal", "type": "Bool", "value": True},
    }
    aliases = [
        {
            "output": "out.k1",
            "expression": {"kind": "column", "column": "a.k1"},
        },
    ]
    if complete_alias:
        aliases.append(
            {
                "output": "out.k2",
                "expression": {"kind": "column", "column": "a.k2"},
            }
        )
    aliases.append(
        {
            "output": "out.count",
            "expression": {"kind": "column", "column": aggregate_output},
        }
    )
    project = {
        "id": "alias",
        "op": "project",
        "input": "filter",
        "ordered": False,
        "columns": aliases,
    }
    order = [_order_item("out.count", ascending=False)]
    order.append(_order_item("out.k1", nulls_first=True))
    if complete_alias:
        order.append(_order_item("out.k2", nulls_first=True))
    top = {
        "id": "top",
        "op": "sort",
        "input": "alias",
        "order": [dict(item) for item in order],
        "limit": _uint64(2),
        "phase": "intermediate",
    }
    finish = {
        "id": "finish",
        "op": "limit",
        "input": "top",
        "count": _uint64(2),
        "offset": None,
        "phase": "final",
    }
    middle_nodes = []
    nodes = [scan, partial]
    if final_aggregate:
        middle_nodes.append("final")
        nodes.append(final)
    middle_nodes.extend(("filter", "alias", "top"))
    nodes.extend((filter_node, project, top, finish))
    output = [column["output"] for column in aliases]
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {
                "tables": [
                    {
                        "name": "A",
                        "columns": [
                            {"name": "k1", "type": "Int64", "nullable": True},
                            {"name": "k2", "type": "Int64", "nullable": True},
                            {
                                "name": "payload",
                                "type": "Int64",
                                "nullable": False,
                            },
                        ],
                        "unique_keys": [],
                    }
                ]
            },
            "plan": {
                "nodes": nodes,
                "root": "finish",
                "output": output,
                "subplans": [],
            },
            "stage_graph": {
                "root_stage": "root",
                "stages": [
                    {
                        "id": "source",
                        "nodes": ["scan", "partial"],
                        "inputs": [],
                        "outputs": [{"index": 0, "node": "partial"}],
                        "source_storage": "column",
                    },
                    {
                        "id": "grouped",
                        "nodes": middle_nodes,
                        "inputs": ["partial"],
                        "outputs": [{"index": 0, "node": "top"}],
                        "source_storage": None,
                    },
                    {
                        "id": "root",
                        "nodes": ["finish"],
                        "inputs": ["top"],
                        "outputs": [{"index": 0, "node": "finish"}],
                        "source_storage": None,
                    },
                ],
                "edges": [
                    {
                        "id": "shuffle",
                        "producer": "source",
                        "consumer": "grouped",
                        "occurrence": 0,
                        "producer_output": 0,
                        "consumer_input": 0,
                        "kind": "hash_shuffle",
                        "keys": list(shuffle_keys),
                        "hash_function": "HashV1",
                        "use_spilling": False,
                    },
                    {
                        "id": "merge",
                        "producer": "grouped",
                        "consumer": "root",
                        "occurrence": 0,
                        "producer_output": 0,
                        "consumer_input": 0,
                        "kind": "merge",
                        "order": [dict(item) for item in order],
                    },
                ],
                "assumptions": [],
            },
        }
    )


def _ground(term, constants, function_value=None):
    if term.operation == "symbol":
        return constants[term.atom]
    if term.operation in {"bool", "int"}:
        return term.atom
    if term.operation == "not":
        return not _ground(term.arguments[0], constants, function_value)
    if term.operation == "and":
        return all(_ground(item, constants, function_value) for item in term.arguments)
    if term.operation == "or":
        return any(_ground(item, constants, function_value) for item in term.arguments)
    if term.operation == "=":
        return _ground(term.arguments[0], constants, function_value) == _ground(
            term.arguments[1], constants, function_value
        )
    if term.operation == "distinct":
        values = tuple(
            _ground(item, constants, function_value) for item in term.arguments
        )
        return len(values) == len(set(values))
    if term.operation == "<":
        return _ground(term.arguments[0], constants, function_value) < _ground(
            term.arguments[1], constants, function_value
        )
    if term.operation == "ite":
        branch = term.arguments[
            1 if _ground(term.arguments[0], constants, function_value) else 2
        ]
        return _ground(branch, constants, function_value)
    if term.operation in {"+", "-", "*", "mod"}:
        values = tuple(
            _ground(item, constants, function_value) for item in term.arguments
        )
        if term.operation == "+":
            return sum(values)
        if term.operation == "-":
            return values[0] - values[1]
        if term.operation == "*":
            return values[0] * values[1]
        return values[0] % values[1]
    if term.operation.startswith("f_") and function_value is not None:
        return function_value(
            term.operation,
            tuple(
                _ground(item, constants, function_value)
                for item in term.arguments
            ),
        )
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _family_sequences(family, constants, function_value):
    sequences = set()
    for outcome in family.outcomes:
        if not _ground(outcome.enabled, constants, function_value):
            continue
        relation = outcome.relation
        indices = [
            index
            for index, row in enumerate(relation.rows)
            if _ground(row.present, constants, function_value)
        ]
        if relation.ordinals is not None:
            indices.sort(
                key=lambda index: _ground(
                    relation.ordinals[index], constants, function_value
                )
            )
        sequences.add(
            tuple(
                tuple(
                    None
                    if _ground(
                        relation.rows[index].values[column.name].is_null,
                        constants,
                        function_value,
                    )
                    else _ground(
                        relation.rows[index].values[column.name].value,
                        constants,
                        function_value,
                    )
                    for column in relation.columns
                )
                for index in indices
            )
        )
    return sequences


class StageCompactionTest(unittest.TestCase):
    COLUMN = Column("x", "Int64", True)
    COLUMNS = (COLUMN,)
    OCCURRENCE = Occurrence("table", "A", 0)

    @staticmethod
    def _row(present, value, is_null, occurrence, *facts):
        return Row(
            present,
            {"x": Value("Int64", is_null, smt.int_value(value))},
            occurrence,
            frozenset(facts),
        )

    def test_exclusive_task_copies_select_the_active_value_exactly(self):
        route = smt.symbol("route", smt.BOOL)
        base = smt.symbol("base_present", smt.BOOL)
        left_present = smt.and_(base, smt.not_(route))
        right_present = smt.and_(base, route)
        rows = (
            self._row(
                left_present,
                10,
                smt.TRUE,
                self.OCCURRENCE,
                PartitionFact(route, False),
            ),
            self._row(
                right_present,
                20,
                smt.FALSE,
                self.OCCURRENCE,
                PartitionFact(route, True),
            ),
        )

        compacted = stages._compact_exclusive_rows(rows, self.COLUMNS)

        self.assertEqual(len(compacted), 1)
        row = compacted[0]
        self.assertEqual(row.present, smt.or_(left_present, right_present))
        self.assertEqual(
            row.values["x"].is_null,
            smt.ite(left_present, smt.TRUE, smt.FALSE),
        )
        self.assertEqual(
            row.values["x"].value,
            smt.ite(left_present, smt.int_value(10), smt.int_value(20)),
        )
        self.assertEqual(row.partition_facts, frozenset())

        explicit = stages._compact_exclusive_rows(
            rows,
            self.COLUMNS,
            merge_conditional_values=False,
        )
        self.assertEqual(explicit, rows)

    def test_exclusive_compaction_keeps_integral_state(self):
        columns = (
            Column("k", "Int64", False),
            Column("state", "Double", True),
        )
        route = smt.symbol("route", smt.BOOL)
        payloads = (
            (10, 1, -4, 8),
            (20, 2, -7, 9),
        )
        rows = tuple(
            Row(
                route if task else smt.not_(route),
                {
                    "k": Value("Int64", smt.FALSE, smt.int_value(carrier)),
                    "state": Value(
                        "Double",
                        smt.FALSE,
                        smt.int_value(carrier),
                        average_metadata=IntegralAverageState(
                            smt.int_value(count),
                            smt.int_value(minimum),
                            smt.int_value(maximum),
                            count,
                        ),
                    ),
                },
                self.OCCURRENCE,
                frozenset((PartitionFact(route, task),)),
            )
            for task, (
                carrier,
                count,
                minimum,
                maximum,
            ) in enumerate(payloads)
        )

        compacted = stages._compact_exclusive_rows(rows, columns)

        self.assertEqual(len(compacted), 1)
        selected = compacted[0]
        state_value = selected.values["state"]
        state = state_value.average_metadata
        self.assertIsInstance(state, IntegralAverageState)
        assert isinstance(state, IntegralAverageState)
        self.assertEqual(state.count_bound, 2)
        left_present = smt.not_(route)
        self.assertEqual(
            (
                state_value.value,
                state.count,
                state.minimum,
                state.maximum,
            ),
            tuple(
                smt.ite(
                    left_present,
                    smt.int_value(left),
                    smt.int_value(right),
                )
                for left, right in zip(payloads[0], payloads[1])
            ),
        )

    def test_exclusive_compaction_selects_a_complete_decimal_sum_state(self):
        column = Column("state", "Decimal(35,0)", True)
        route = smt.symbol("decimal_sum_route", smt.BOOL)
        payloads = (
            (True, True, True, True, 10, 10),
            (False, False, False, False, 0, 20),
        )

        def row(task, payload, state_mutation=None):
            (
                any_non_null,
                has_nan,
                has_pos_inf,
                has_neg_inf,
                finite_total,
                bound,
            ) = payload
            state = DecimalSumState(
                sum_type="Decimal(35,0)",
                any_non_null=smt.bool_value(any_non_null),
                has_nan=smt.bool_value(has_nan),
                has_pos_inf=smt.bool_value(has_pos_inf),
                has_neg_inf=smt.bool_value(has_neg_inf),
                finite_total=smt.int_value(finite_total),
                finite_abs_bound=bound,
            )
            if state_mutation is not None:
                state = state_mutation(state)
            present = route if task else smt.not_(route)
            return Row(
                present,
                {
                    "state": Value(
                        "Decimal(35,0)",
                        smt.not_(state.any_non_null),
                        decimal.finish_sum_state(state),
                        bound,
                        decimal_sum_state=state,
                    )
                },
                self.OCCURRENCE,
                frozenset((PartitionFact(route, task),)),
            )

        rows = tuple(row(bool(task), payload) for task, payload in enumerate(payloads))
        compacted = stages._compact_exclusive_rows(rows, (column,))

        self.assertEqual(len(compacted), 1)
        selected = compacted[0].values["state"]
        state = selected.decimal_sum_state
        self.assertIsInstance(state, DecimalSumState)
        assert isinstance(state, DecimalSumState)
        left_present = smt.not_(route)
        expected_lanes = tuple(
            smt.ite(
                left_present,
                smt.bool_value(left),
                smt.bool_value(right),
            )
            for left, right in zip(payloads[0][:4], payloads[1][:4])
        ) + (
            smt.ite(
                left_present,
                smt.int_value(payloads[0][4]),
                smt.int_value(payloads[1][4]),
            ),
        )
        self.assertEqual(
            (
                state.any_non_null,
                state.has_nan,
                state.has_pos_inf,
                state.has_neg_inf,
                state.finite_total,
            ),
            expected_lanes,
        )
        self.assertEqual(state.finite_abs_bound, 20)
        self.assertEqual(selected.decimal_finite_abs_bound, 20)
        self.assertEqual(selected.is_null, smt.not_(state.any_non_null))
        self.assertEqual(
            selected.value,
            decimal.finish_sum_state(state),
        )

        malformed_rows = (
            (
                (
                    rows[0],
                    Row(
                        rows[1].present,
                        {
                            "state": Value(
                                "Decimal(35,0)",
                                rows[1].values["state"].is_null,
                                rows[1].values["state"].value,
                                rows[1]
                                .values["state"]
                                .decimal_finite_abs_bound,
                            )
                        },
                        rows[1].occurrence,
                        rows[1].partition_facts,
                    ),
                ),
                20,
            ),
            (
                (
                    rows[0],
                    row(
                        True,
                        payloads[1],
                        lambda state: DecimalSumState(
                            sum_type="Decimal(35,1)",
                            any_non_null=state.any_non_null,
                            has_nan=state.has_nan,
                            has_pos_inf=state.has_pos_inf,
                            has_neg_inf=state.has_neg_inf,
                            finite_total=state.finite_total,
                            finite_abs_bound=state.finite_abs_bound,
                        ),
                    ),
                ),
                20,
            ),
            (
                (
                    rows[0],
                    Row(
                        rows[1].present,
                        {
                            "state": Value(
                                "Decimal(35,0)",
                                rows[1].values["state"].is_null,
                                rows[1].values["state"].value,
                                19,
                                decimal_sum_state=rows[1]
                                .values["state"]
                                .decimal_sum_state,
                            )
                        },
                        rows[1].occurrence,
                        rows[1].partition_facts,
                    ),
                ),
                19,
            ),
        )
        for kind, (alternatives, expected_bound) in enumerate(malformed_rows):
            with self.subTest(kind=kind):
                fallback = stages._compact_exclusive_rows(
                    alternatives,
                    (column,),
                )[0].values["state"]
                self.assertIsNone(fallback.decimal_sum_state)
                self.assertEqual(
                    fallback.decimal_finite_abs_bound,
                    expected_bound,
                )

    def test_overlapping_broadcast_copies_retain_bag_multiplicity(self):
        present = smt.symbol("present", smt.BOOL)
        rows = (
            self._row(present, 1, smt.FALSE, self.OCCURRENCE),
            self._row(present, 1, smt.FALSE, self.OCCURRENCE),
        )

        compacted = stages._compact_exclusive_rows(rows, self.COLUMNS)

        self.assertEqual(compacted, rows)

    def test_two_overlapping_copies_per_task_compact_to_multiplicity_two(self):
        route = smt.symbol("route", smt.BOOL)
        left = PartitionFact(route, False)
        right = PartitionFact(route, True)
        rows = (
            self._row(smt.not_(route), 1, smt.FALSE, self.OCCURRENCE, left),
            self._row(smt.not_(route), 1, smt.FALSE, self.OCCURRENCE, left),
            self._row(route, 1, smt.FALSE, self.OCCURRENCE, right),
            self._row(route, 1, smt.FALSE, self.OCCURRENCE, right),
        )

        compacted = stages._compact_exclusive_rows(rows, self.COLUMNS)

        self.assertEqual(len(compacted), 2)
        self.assertTrue(
            all(
                row.present == smt.or_(smt.not_(route), route)
                for row in compacted
            )
        )

    def test_opposite_routes_do_not_merge_distinct_occurrences(self):
        route = smt.symbol("route", smt.BOOL)
        rows = (
            self._row(
                smt.not_(route),
                1,
                smt.FALSE,
                Occurrence("table", "A", 0),
                PartitionFact(route, False),
            ),
            self._row(
                route,
                1,
                smt.FALSE,
                Occurrence("table", "A", 1),
                PartitionFact(route, True),
            ),
        )

        compacted = stages._compact_exclusive_rows(rows, self.COLUMNS)

        self.assertEqual(compacted, rows)

    def test_unknown_occurrences_fail_closed_to_explicit_rows(self):
        route = smt.symbol("route", smt.BOOL)
        rows = (
            self._row(
                smt.not_(route),
                1,
                smt.FALSE,
                None,
                PartitionFact(route, False),
            ),
            self._row(
                route,
                1,
                smt.FALSE,
                None,
                PartitionFact(route, True),
            ),
        )

        self.assertEqual(
            stages._compact_exclusive_rows(rows, self.COLUMNS),
            rows,
        )

    def test_compaction_retains_only_facts_common_to_every_alternative(self):
        route = smt.symbol("route", smt.BOOL)
        upstream = smt.symbol("upstream", smt.BOOL)
        common = PartitionFact(upstream, True)
        rows = (
            self._row(
                smt.not_(route),
                1,
                smt.FALSE,
                self.OCCURRENCE,
                common,
                PartitionFact(route, False),
            ),
            self._row(
                route,
                1,
                smt.FALSE,
                self.OCCURRENCE,
                common,
                PartitionFact(route, True),
            ),
        )

        compacted = stages._compact_exclusive_rows(rows, self.COLUMNS)

        self.assertEqual(len(compacted), 1)
        self.assertEqual(compacted[0].partition_facts, frozenset((common,)))

    def test_gather_uses_the_audited_conditional_value_threshold(self):
        def gathered(origin_count, *, compact_exclusive_task_copies=False):
            left = []
            right = []
            for index in range(origin_count):
                route = smt.symbol(f"route_{index}", smt.BOOL)
                occurrence = Occurrence("table", "A", index)
                left.append(self._row(
                    smt.not_(route),
                    index,
                    smt.FALSE,
                    occurrence,
                    PartitionFact(route, False),
                ))
                right.append(self._row(
                    route,
                    index + 100,
                    smt.FALSE,
                    occurrence,
                    PartitionFact(route, True),
                ))
            return stages._gather(
                (
                    single(Relation(self.COLUMNS, tuple(left))),
                    single(Relation(self.COLUMNS, tuple(right))),
                ),
                compact_exclusive_task_copies=compact_exclusive_task_copies,
            ).certain()

        self.assertEqual(len(gathered(4).rows), 8)
        self.assertEqual(len(gathered(5).rows), 5)
        self.assertEqual(
            len(gathered(1, compact_exclusive_task_copies=True).rows),
            1,
        )

    def test_routing_connections_merge_small_exclusive_task_values(self):
        route = smt.symbol("source_route", smt.BOOL)
        columns = tuple(
            Column(name, "Int64", True)
            for name in ("x", "y", "z", "w", "v")
        )

        def row(source_columns, present, offset, fact):
            return Row(
                present,
                {
                    column.name: Value(
                        "Int64",
                        smt.FALSE,
                        smt.int_value(offset + index),
                    )
                    for index, column in enumerate(source_columns)
                },
                self.OCCURRENCE,
                frozenset((fact,)),
            )

        def source_for(source_columns):
            return stages.Partitions((
                single(Relation(source_columns, (
                    row(
                        source_columns,
                        smt.not_(route),
                        10,
                        PartitionFact(route, False),
                    ),
                ))),
                single(Relation(source_columns, (
                    row(
                        source_columns,
                        route,
                        20,
                        PartitionFact(route, True),
                    ),
                ))),
            ))

        source = source_for(columns)
        evaluator = object.__new__(stages.Evaluator)
        evaluator.router = stages.Router(smt.Script())

        self.assertEqual(
            len(stages._gather(source.relations).certain().rows),
            2,
        )

        def edge(kind, **kwargs):
            return StageEdge(
                id=f"{kind}_edge",
                producer="producer",
                consumer="consumer",
                occurrence=0,
                producer_output=0,
                consumer_input=0,
                kind=kind,
                **kwargs,
            )

        broadcast = evaluator._connect(
            edge("broadcast"),
            source,
            stages.TASKS,
            0,
        )
        self.assertEqual(
            tuple(len(family.certain().rows) for family in broadcast.relations),
            (1, 1),
        )
        self.assertEqual(
            broadcast.relations[0].certain().rows[0].values,
            broadcast.relations[1].certain().rows[0].values,
        )

        hash_edge = edge(
            "hash_shuffle",
            keys=("x",),
            hash_function="HashV1",
            use_spilling=False,
        )
        boundary = evaluator._connect(
            hash_edge,
            source_for(columns[:4]),
            stages.TASKS,
            0,
        )
        self.assertEqual(
            tuple(len(family.certain().rows) for family in boundary.relations),
            (2, 2),
        )

        shuffled = evaluator._connect(
            hash_edge,
            source,
            stages.TASKS,
            0,
        )
        self.assertEqual(
            tuple(len(family.certain().rows) for family in shuffled.relations),
            (1, 1),
        )
        left = shuffled.relations[0].certain().rows[0]
        right = shuffled.relations[1].certain().rows[0]
        self.assertEqual(
            left.values["x"].value,
            smt.ite(
                smt.not_(route),
                smt.int_value(10),
                smt.int_value(20),
            ),
        )
        self.assertEqual(len(left.partition_facts), 1)
        self.assertEqual(len(right.partition_facts), 1)
        left_fact = next(iter(left.partition_facts))
        right_fact = next(iter(right.partition_facts))
        self.assertEqual(left_fact.term, right_fact.term)
        self.assertFalse(left_fact.value)
        self.assertTrue(right_fact.value)
        gathered_present = smt.or_(smt.not_(route), route)
        self.assertEqual(
            left.present,
            smt.and_(gathered_present, smt.not_(left_fact.term)),
        )
        self.assertEqual(
            right.present,
            smt.and_(gathered_present, right_fact.term),
        )

    def test_decimal_bounds_merge_conservatively(self):
        column = Column("d", "Decimal(3,0)", False)
        route = smt.symbol("route", smt.BOOL)

        def row(task, bound):
            present = route if task else smt.not_(route)
            return Row(
                present,
                {"d": Value("Decimal(3,0)", smt.FALSE, smt.ZERO, bound)},
                self.OCCURRENCE,
                frozenset((PartitionFact(route, task),)),
            )

        known = stages._compact_exclusive_rows(
            (row(False, 10), row(True, 20)),
            (column,),
        )
        unknown = stages._compact_exclusive_rows(
            (row(False, 10), row(True, None)),
            (column,),
        )

        self.assertEqual(known[0].values["d"].decimal_finite_abs_bound, 20)
        self.assertIsNone(unknown[0].values["d"].decimal_finite_abs_bound)


class DerivedUniqueStageGraphTest(unittest.TestCase):
    INPUT_KEY = frozenset(("a.k1", "a.k2"))
    OUTPUT_KEY = frozenset(("out.k1", "out.k2"))

    @staticmethod
    def _evaluate(snapshot):
        script = smt.Script()
        database = Database(snapshot, 2, script)
        scalar = ScalarEncoder(script)
        router = stages.Router(script)
        observed_nodes = {}
        observed_edges = {}

        def observe_node(_scope, node, family):
            observed_nodes.setdefault(node, []).append(family)

        def observe_edge(edge, task, family):
            observed_edges.setdefault(edge.id, []).append((task, family))

        staged = stages.Evaluator(
            snapshot,
            database,
            scalar,
            router,
            node_observer=observe_node,
            edge_observer=observe_edge,
        ).root()
        return (
            script,
            database,
            scalar,
            router,
            staged,
            observed_nodes,
            observed_edges,
        )

    @staticmethod
    def _relations(families):
        return tuple(
            outcome.relation
            for family in families
            for outcome in family.outcomes
        )

    def assert_choice_free(self, families):
        for family in families:
            self.assertEqual(len(family.outcomes), 1)
            self.assertEqual(family.outcomes[0].decisions, ())
            self.assertEqual(family.outcomes[0].choices, ())

    @staticmethod
    def _constants(database, router, rows, tasks):
        constants = {}
        for witness, state in zip(database.witness["A"], rows):
            present = state is not ABSENT
            constants[witness.present.atom] = present
            k1, k2, payload = (0, 0, 0) if not present else state
            for name, value in (("k1", k1), ("k2", k2), ("payload", payload)):
                cell = witness.cells[name]
                if cell.is_null.operation == "symbol":
                    constants[cell.is_null.atom] = value is None
                constants[cell.value.atom] = 0 if value is None else value
        for slot, task in enumerate(tasks):
            constants[router.source_task("A", slot).atom] = task
        return constants

    @staticmethod
    def _hash_value(_function, arguments):
        value = 0
        for is_null, payload in zip(arguments[::2], arguments[1::2]):
            value = value * 131 + (17 if is_null else payload)
        return bool(value % 2)

    def test_nullable_group_key_pipeline_is_equivalent_and_choice_free(self):
        snapshot = _unique_order_stage_snapshot()
        self.assertEqual(
            stage_task_counts(snapshot),
            {"source": 2, "grouped": 2, "root": 1},
        )
        (
            script,
            database,
            scalar,
            router,
            staged,
            nodes,
            edges,
        ) = self._evaluate(snapshot)
        logical = RelationEvaluator(
            snapshot,
            database,
            scalar,
            choice_scope="logical",
        ).root()

        shuffled = [family for _task, family in edges["shuffle"]]
        self.assertTrue(all(
            relation.null_safe_unique_key is None
            and relation.task_partition_key == frozenset(("a.k1",))
            for relation in self._relations(shuffled)
        ))
        for node_id in ("final", "filter"):
            self.assertTrue(all(
                relation.null_safe_unique_key == self.INPUT_KEY
                and relation.task_partition_key == frozenset(("a.k1",))
                for relation in self._relations(nodes[node_id])
            ))
        for node_id in ("alias", "top"):
            self.assertTrue(all(
                relation.null_safe_unique_key == self.OUTPUT_KEY
                and relation.task_partition_key == frozenset(("out.k1",))
                for relation in self._relations(nodes[node_id])
            ))

        merged = [family for _task, family in edges["merge"]]
        self.assertTrue(all(
            relation.null_safe_unique_key == self.OUTPUT_KEY
            and relation.task_partition_key is None
            for relation in self._relations(merged)
        ))
        self.assertTrue(all(
            outcome.relation.null_safe_unique_key == self.OUTPUT_KEY
            and outcome.relation.task_partition_key is None
            for outcome in staged.outcomes
        ))
        self.assert_choice_free(nodes["top"] + merged + [staged])
        self.assert_choice_free([logical])

        states = (
            ABSENT,
            (None, None, 7),
            (None, None, 8),
            (None, 1, 9),
            (0, None, 10),
            (0, 1, 11),
        )
        for rows, tasks in product(
            product(states, repeat=2),
            product((False, True), repeat=2),
        ):
            constants = self._constants(database, router, rows, tasks)
            self.assertEqual(
                _family_sequences(staged, constants, self._hash_value),
                _family_sequences(logical, constants, self._hash_value),
                (rows, tasks),
            )

        nullable_duplicate = self._constants(
            database,
            router,
            ((None, None, 7), (None, None, 8)),
            (False, True),
        )
        self.assertEqual(
            _family_sequences(staged, nullable_duplicate, self._hash_value),
            {((None, None, 2),)},
        )

    def test_broken_stage_certificates_retain_ordering_alternatives(self):
        cases = (
            (
                "partition_not_subset",
                _unique_order_stage_snapshot(
                    shuffle_keys=("a.k1", "a.k2", "_state")
                ),
            ),
            (
                "no_final_aggregate",
                _unique_order_stage_snapshot(final_aggregate=False),
            ),
            (
                "incomplete_alias",
                _unique_order_stage_snapshot(complete_alias=False),
            ),
        )
        for name, snapshot in cases:
            with self.subTest(name=name):
                (*_unused, staged, nodes, edges) = self._evaluate(snapshot)
                shuffled = [family for _task, family in edges["shuffle"]]
                merged = [family for _task, family in edges["merge"]]

                if name == "partition_not_subset":
                    self.assertTrue(all(
                        relation.task_partition_key
                        == frozenset(("a.k1", "a.k2", "_state"))
                        for relation in self._relations(shuffled)
                    ))
                    self.assertTrue(all(
                        relation.null_safe_unique_key == self.INPUT_KEY
                        and relation.task_partition_key is None
                        for relation in self._relations(nodes["final"])
                    ))
                elif name == "no_final_aggregate":
                    self.assertTrue(all(
                        relation.null_safe_unique_key is None
                        for relation in self._relations(nodes["filter"])
                    ))
                else:
                    self.assertTrue(all(
                        relation.null_safe_unique_key == self.INPUT_KEY
                        for relation in self._relations(nodes["final"])
                    ))
                    self.assertTrue(all(
                        relation.null_safe_unique_key is None
                        for relation in self._relations(nodes["alias"])
                    ))

                self.assertTrue(any(
                    outcome.decisions or outcome.choices
                    for family in merged
                    for outcome in family.outcomes
                ))
                if name != "partition_not_subset":
                    self.assertTrue(any(
                        outcome.decisions or outcome.choices
                        for family in nodes["top"]
                        for outcome in family.outcomes
                    ))
                self.assertTrue(all(
                    relation.null_safe_unique_key is None
                    for relation in self._relations(merged + [staged])
                ))

    def test_broadcast_cannot_promote_replicated_local_keys_at_merge(self):
        script = smt.Script()
        scalar = ScalarEncoder(script)
        evaluator = object.__new__(stages.Evaluator)
        evaluator.scalar = scalar
        columns = (Column("k", "Int64", True),)
        key = frozenset(("k",))

        def task_relation(value, occurrence):
            return single(Relation(
                columns,
                (
                    Row(
                        smt.TRUE,
                        {"k": Value("Int64", smt.FALSE, smt.int_value(value))},
                        Occurrence("table", "A", occurrence),
                    ),
                ),
                null_safe_unique_key=key,
                task_partition_key=key,
            ))

        source = stages.Partitions((task_relation(0, 0), task_relation(1, 1)))
        broadcast = evaluator._connect(
            StageEdge(
                "broadcast",
                "source",
                "sorted",
                0,
                0,
                0,
                "broadcast",
            ),
            source,
            stages.TASKS,
            0,
        )
        self.assertTrue(all(
            family.certain().null_safe_unique_key == key
            and family.certain().task_partition_key is None
            for family in broadcast.relations
        ))

        order = (SortOrder("k", True, True),)
        sorted_partitions = stages.Partitions(tuple(
            sort_family(family, order, script, f"sort:{task}")
            for task, family in enumerate(broadcast.relations)
        ))
        self.assert_choice_free(list(sorted_partitions.relations))
        merged = evaluator._connect(
            StageEdge(
                "merge",
                "sorted",
                "root",
                0,
                0,
                0,
                "merge",
                order=order,
            ),
            sorted_partitions,
            1,
            0,
        ).relations[0]

        self.assertTrue(all(
            outcome.relation.null_safe_unique_key is None
            for outcome in merged.outcomes
        ))
        self.assertTrue(any(
            outcome.decisions or outcome.choices
            for outcome in merged.outcomes
        ))

    def test_gather_drops_task_local_key_when_cross_task_duplicates_are_possible(self):
        columns = (Column("k", "Int64", True),)
        key = frozenset(("k",))

        def local(task):
            return single(Relation(
                columns,
                (
                    Row(
                        smt.TRUE,
                        {"k": Value("Int64", smt.FALSE, smt.int_value(1))},
                        Occurrence("table", "A", task),
                    ),
                ),
                null_safe_unique_key=key,
            ))

        gathered = stages._gather((local(0), local(1))).certain()

        self.assertEqual(len(gathered.rows), 2)
        self.assertIsNone(gathered.null_safe_unique_key)
        self.assertIsNone(gathered.task_partition_key)

    def test_global_key_forces_choice_free_merge_network_under_pair_cap(self):
        script = smt.Script()
        evaluator = object.__new__(stages.Evaluator)
        evaluator.scalar = ScalarEncoder(script)
        columns = (
            Column("k", "Int64", False),
            Column("payload", "Int64", False),
        )
        key = frozenset(("k",))
        order = (SortOrder("k", True, False),)
        edge = StageEdge(
            "merge",
            "source",
            "root",
            0,
            0,
            0,
            "merge",
            order=order,
        )

        def producer(items, partition=True):
            rows = tuple(
                Row(
                    smt.TRUE,
                    {
                        "k": Value("Int64", smt.FALSE, smt.int_value(k)),
                        "payload": Value(
                            "Int64", smt.FALSE, smt.int_value(payload)
                        ),
                    },
                )
                for k, payload in items
            )
            return single(Relation(
                columns,
                rows,
                sequence=True,
                order=order,
                ordinals=tuple(smt.int_value(index) for index in range(len(rows))),
                null_safe_unique_key=key,
                task_partition_key=key if partition else None,
            ))

        def merge(partitions):
            with patch.object(relation_model, "MAX_RELATION_ROW_PAIRS", 5):
                return evaluator._connect(
                    edge,
                    stages.Partitions(partitions),
                    1,
                    0,
                ).relations[0]

        left = producer(((1, 10), (4, 40)))
        right = producer(((2, 20), (3, 30)))
        certified = merge((left, right))
        outcome = certified.outcomes[0]

        self.assertEqual(len(certified.outcomes), 1)
        self.assertEqual(outcome.decisions, ())
        self.assertEqual(outcome.choices, ())
        self.assertTrue(outcome.relation.present_prefix)
        self.assertIsNone(outcome.relation.ordinals)
        self.assertEqual(outcome.relation.null_safe_unique_key, key)
        self.assertIsNone(outcome.relation.task_partition_key)
        self.assertEqual(len(outcome.relation.rows), 4)

        near_miss = merge((left, producer(((2, 20), (3, 30)), False)))
        near_outcome = near_miss.outcomes[0]
        self.assertTrue(near_outcome.relation.present_prefix)
        self.assertIsNone(near_outcome.relation.null_safe_unique_key)
        self.assertEqual(near_outcome.decisions, ())
        self.assertEqual(len(near_outcome.choices), 4)


if __name__ == "__main__":
    unittest.main()
