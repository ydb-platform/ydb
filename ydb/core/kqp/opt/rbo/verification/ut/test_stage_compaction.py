import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt, stages
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Column, StageEdge
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Occurrence,
    PartitionFact,
    Relation,
    Row,
    single,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    IntegralAverageState,
    Value,
)


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


if __name__ == "__main__":
    unittest.main()
