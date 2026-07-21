import copy
import unittest
from itertools import permutations, product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column,
    SnapshotError,
    parse_snapshot,
    stage_task_counts,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator as RelationEvaluator,
    Relation,
    RelationError,
    Row,
    family_equal,
    single,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    Encoder as ScalarEncoder,
    Value,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stages import (
    Evaluator as StageEvaluator,
    Router,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    VerificationError,
    build_logical_kernel_problem_for_tests,
    build_problem,
)


ABSENT = object()
UINT64_MAX = (1 << 64) - 1
COLUMNS = ("a.k1", "a.k2", "a.payload")
COLUMN_INDEX = {name: index for index, name in enumerate(COLUMNS)}


def uint64(value):
    return {"kind": "literal", "type": "Uint64", "value": value}


def order_item(column, ascending=True, nulls_first=False):
    return {
        "column": column,
        "ascending": ascending,
        "nulls_first": nulls_first,
    }


def scan():
    return {
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


def sort_node(
    node_id,
    input_id,
    order,
    limit=None,
    phase="undefined",
):
    return {
        "id": node_id,
        "op": "sort",
        "input": input_id,
        "order": copy.deepcopy(order),
        "limit": None if limit is None else uint64(limit),
        "phase": phase,
    }


def project_node(node_id, input_id, ordered):
    return {
        "id": node_id,
        "op": "project",
        "input": input_id,
        "ordered": ordered,
        "columns": [
            {
                "output": column,
                "expression": {"kind": "column", "column": column},
            }
            for column in COLUMNS
        ],
    }


def limit_node(
    node_id,
    input_id,
    count,
    offset=None,
    phase="undefined",
):
    return {
        "id": node_id,
        "op": "limit",
        "input": input_id,
        "count": uint64(count),
        "offset": None if offset is None else uint64(offset),
        "phase": phase,
    }


def union_node(node_id, left, right):
    return {
        "id": node_id,
        "op": "union_all",
        "inputs": [
            {"node": left, "columns": list(COLUMNS)},
            {"node": right, "columns": list(COLUMNS)},
        ],
        "output": list(COLUMNS),
    }


def snapshot(nodes, root, stage_graph=None):
    return {
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
            "root": root,
            "output": list(COLUMNS),
        },
        "stage_graph": stage_graph,
    }


def logical_sort(order, top_limit=None, phase="undefined"):
    return parse_snapshot(
        snapshot(
            [scan(), sort_node("sort", "scan", order, top_limit, phase)],
            "sort",
        )
    )


def logical_ordered_limit(
    order,
    count,
    offset=None,
    sort_limit=None,
    sort_phase="undefined",
    limit_phase="undefined",
):
    return parse_snapshot(
        snapshot(
            [
                scan(),
                sort_node("sort", "scan", order, sort_limit, sort_phase),
                limit_node("limit", "sort", count, offset, limit_phase),
            ],
            "limit",
        )
    )


def staged_top_sort_merge(
    order,
    partial_limit=2,
    final_count=1,
    final_offset=1,
    partial_order=None,
    merge_order=None,
    partial_phase="intermediate",
    final_phase="final",
):
    partial_order = order if partial_order is None else partial_order
    merge_order = partial_order if merge_order is None else merge_order
    nodes = [
        scan(),
        sort_node(
            "partial",
            "scan",
            partial_order,
            partial_limit,
            partial_phase,
        ),
        limit_node(
            "final",
            "partial",
            final_count,
            final_offset,
            final_phase,
        ),
    ]
    graph = {
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
                "id": "root",
                "nodes": ["final"],
                "inputs": ["partial"],
                "outputs": [{"index": 0, "node": "final"}],
                "source_storage": None,
            },
        ],
        "edges": [
            {
                "id": "merge",
                "producer": "source",
                "consumer": "root",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "merge",
                "order": copy.deepcopy(merge_order),
            }
        ],
        "assumptions": [],
    }
    return parse_snapshot(snapshot(nodes, "final", graph))


def staged_parallel_sort_merge(order):
    nodes = [
        scan(),
        sort_node("partial", "scan", order),
        project_node("middle", "partial", False),
        project_node("root_project", "middle", False),
    ]
    graph = {
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
                "id": "middle_stage",
                "nodes": ["middle"],
                "inputs": ["partial"],
                "outputs": [{"index": 0, "node": "middle"}],
                "source_storage": None,
            },
            {
                "id": "root",
                "nodes": ["root_project"],
                "inputs": ["middle"],
                "outputs": [{"index": 0, "node": "root_project"}],
                "source_storage": None,
            },
        ],
        "edges": [
            {
                "id": "parallel",
                "producer": "source",
                "consumer": "middle_stage",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "union_all",
                "parallel": True,
            },
            {
                "id": "merge",
                "producer": "middle_stage",
                "consumer": "root",
                "occurrence": 0,
                "producer_output": 0,
                "consumer_input": 0,
                "kind": "merge",
                "order": copy.deepcopy(order),
            },
        ],
        "assumptions": [],
    }
    return parse_snapshot(snapshot(nodes, "root_project", graph))


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
            term.arguments[1], constants
        )
    if term.operation == "<":
        return _ground(term.arguments[0], constants) < _ground(
            term.arguments[1], constants
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
    if term.operation == "mod":
        return _ground(term.arguments[0], constants) % _ground(
            term.arguments[1], constants
        )
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _witness_constants(witness, rows):
    constants = {}
    for witness_row, concrete in zip(witness["A"], rows):
        present = concrete is not ABSENT
        constants[witness_row.present.atom] = present
        concrete_values = (0, 0, 0) if not present else concrete
        for name, value in zip(("k1", "k2", "payload"), concrete_values):
            cell = witness_row.cells[name]
            if cell.is_null.operation == "symbol":
                constants[cell.is_null.atom] = value is None
            constants[cell.value.atom] = 0 if value is None else value
    return constants


def _database_constants(database, rows, router=None, tasks=None):
    constants = _witness_constants(database.witness, rows)
    if router is not None:
        assert tasks is not None
        for slot, task in enumerate(tasks):
            constants[router.source_task("A", slot).atom] = task
    return constants


def _cell(value, constants):
    return (
        None
        if _ground(value.is_null, constants)
        else _ground(value.value, constants)
    )


def _sequences(family, constants):
    assert family.sequence
    result = set()
    for outcome in family.outcomes:
        if not _ground(outcome.enabled, constants):
            continue
        names = tuple(column.name for column in outcome.relation.columns)
        rows = tuple(
            tuple(_cell(row.values[name], constants) for name in names)
            for row in outcome.relation.rows
            if _ground(row.present, constants)
        )
        result.add(rows)
    return result


def _compare_cells(left, right, ascending, nulls_first):
    if left is None or right is None:
        if left is right:
            return 0
        if left is None:
            return -1 if nulls_first else 1
        return 1 if nulls_first else -1
    if left == right:
        return 0
    before = left < right if ascending else left > right
    return -1 if before else 1


def _compare_rows(left, right, order):
    for item in order:
        index = COLUMN_INDEX[item["column"]]
        comparison = _compare_cells(
            left[index],
            right[index],
            item["ascending"],
            item["nulls_first"],
        )
        if comparison:
            return comparison
    return 0


def _reference_sequences(rows, order):
    present = tuple(row for row in rows if row is not ABSENT)
    return {
        permutation
        for permutation in permutations(present)
        if all(
            _compare_rows(permutation[index], permutation[index + 1], order) <= 0
            for index in range(len(permutation) - 1)
        )
    }


def _logical_family(parsed, row_bound):
    script = smt.Script()
    database = Database(parsed, row_bound, script)
    family = RelationEvaluator(parsed, database, ScalarEncoder(script)).root()
    return database, family


def _counterexample_formula_holds(problem, rows):
    constants = _witness_constants(problem.witness, rows)
    return all(_ground(assertion, constants) for assertion in problem.script.assertions)


class SortIrTest(unittest.TestCase):
    def test_order_limit_and_every_phase_are_strictly_decoded(self):
        order = [
            order_item("a.k1", True, False),
            order_item("a.k2", False, True),
        ]
        for phase, top_limit in product(
            ("undefined", "intermediate", "final"),
            (None, 0, UINT64_MAX),
        ):
            with self.subTest(phase=phase, top_limit=top_limit):
                parsed = logical_sort(order, top_limit, phase).plan.nodes[-1]
                self.assertEqual(
                    [
                        (item.column, item.ascending, item.nulls_first)
                        for item in parsed.order
                    ],
                    [
                        ("a.k1", True, False),
                        ("a.k2", False, True),
                    ],
                )
                self.assertEqual(
                    None if parsed.limit is None else parsed.limit.value,
                    top_limit,
                )
                self.assertEqual(parsed.phase, phase)

    def test_sort_shape_and_order_items_fail_closed(self):
        base = snapshot(
            [scan(), sort_node("sort", "scan", [order_item("a.k1")])],
            "sort",
        )
        mutations = []

        for field in ("input", "order", "limit", "phase"):
            value = copy.deepcopy(base)
            del value["plan"]["nodes"][1][field]
            mutations.append((f"missing {field}", value, "missing fields"))

        unknown = copy.deepcopy(base)
        unknown["plan"]["nodes"][1]["stable"] = True
        mutations.append(("unknown sort field", unknown, "unknown fields: stable"))

        empty = copy.deepcopy(base)
        empty["plan"]["nodes"][1]["order"] = []
        mutations.append(("empty order", empty, "must not be empty"))

        unavailable = copy.deepcopy(base)
        unavailable["plan"]["nodes"][1]["order"][0]["column"] = "a.missing"
        mutations.append(("unavailable column", unavailable, "is not available"))

        bad_phase = copy.deepcopy(base)
        bad_phase["plan"]["nodes"][1]["phase"] = "partial"
        mutations.append(("bad phase", bad_phase, "unsupported sort phase"))

        for field in ("column", "ascending", "nulls_first"):
            value = copy.deepcopy(base)
            del value["plan"]["nodes"][1]["order"][0][field]
            mutations.append((f"missing order {field}", value, "missing fields"))

        extra_item = copy.deepcopy(base)
        extra_item["plan"]["nodes"][1]["order"][0]["collation"] = "binary"
        mutations.append(("unknown order field", extra_item, "unknown fields: collation"))

        bad_column = copy.deepcopy(base)
        bad_column["plan"]["nodes"][1]["order"][0]["column"] = 7
        mutations.append(("non-string column", bad_column, "non-empty string"))

        for field in ("ascending", "nulls_first"):
            value = copy.deepcopy(base)
            value["plan"]["nodes"][1]["order"][0][field] = 1
            mutations.append((f"non-Boolean {field}", value, "expected a Boolean"))

        for name, value, message in mutations:
            with self.subTest(name=name):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(value)

    def test_top_sort_limit_is_an_exact_uint64_literal(self):
        base = snapshot(
            [
                scan(),
                sort_node("sort", "scan", [order_item("a.k1")], 1),
            ],
            "sort",
        )
        invalid = (
            {"kind": "null", "type": "Uint64"},
            uint64(-1),
            uint64(1 << 64),
            {"kind": "literal", "type": "Int64", "value": 1},
            {
                "kind": "opaque",
                "fingerprint": "parameter",
                "type": "Uint64",
                "nullable": False,
                "args": [],
            },
        )
        for expression in invalid:
            value = copy.deepcopy(base)
            value["plan"]["nodes"][1]["limit"] = expression
            with self.subTest(expression=expression):
                with self.assertRaisesRegex(SnapshotError, "Uint64 literal"):
                    parse_snapshot(value)

    def test_string_and_utf8_ordering_fail_closed(self):
        for scalar_type in ("String", "Utf8"):
            value = snapshot(
                [scan(), sort_node("sort", "scan", [order_item("a.k1")])],
                "sort",
            )
            value["schema"]["tables"][0]["columns"][0]["type"] = scalar_type
            with self.subTest(scalar_type=scalar_type):
                with self.assertRaisesRegex(
                    SnapshotError,
                    "String and Utf8 ordering is not modeled",
                ):
                    parse_snapshot(value)

    def test_project_order_preservation_flag_is_required_and_boolean(self):
        base = snapshot(
            [scan(), project_node("project", "scan", True)],
            "project",
        )
        self.assertTrue(parse_snapshot(base).plan.nodes[-1].ordered)

        missing = copy.deepcopy(base)
        del missing["plan"]["nodes"][1]["ordered"]
        with self.assertRaisesRegex(SnapshotError, "missing fields: ordered"):
            parse_snapshot(missing)

        invalid = copy.deepcopy(base)
        invalid["plan"]["nodes"][1]["ordered"] = 1
        with self.assertRaisesRegex(SnapshotError, "expected a Boolean"):
            parse_snapshot(invalid)


class SortConcreteDifferentialTest(unittest.TestCase):
    def test_single_key_matches_exhaustive_tiny_reference(self):
        for ascending, nulls_first in product((False, True), repeat=2):
            order = [order_item("a.k1", ascending, nulls_first)]
            parsed = logical_sort(order)
            database, family = _logical_family(parsed, 3)
            slot_states = tuple(
                (ABSENT,) + tuple((key, 0, slot) for key in (None, -1, 1))
                for slot in range(3)
            )
            for rows in product(*slot_states):
                with self.subTest(
                    ascending=ascending,
                    nulls_first=nulls_first,
                    rows=rows,
                ):
                    constants = _database_constants(database, rows)
                    self.assertEqual(
                        _sequences(family, constants),
                        _reference_sequences(rows, order),
                    )

    def test_multikey_matches_exhaustive_tiny_reference(self):
        order = [
            order_item("a.k1", True, False),
            order_item("a.k2", False, True),
        ]
        parsed = logical_sort(order)
        database, family = _logical_family(parsed, 3)
        key_pairs = tuple(product((None, 0, 1), repeat=2))
        slot_states = tuple(
            (ABSENT,) + tuple((first, second, slot) for first, second in key_pairs)
            for slot in range(3)
        )
        for rows in product(*slot_states):
            constants = _database_constants(database, rows)
            self.assertEqual(
                _sequences(family, constants),
                _reference_sequences(rows, order),
                rows,
            )

    def test_ties_and_duplicate_rows_preserve_every_legal_occurrence_order(self):
        order = [order_item("a.k1"), order_item("a.k2")]
        parsed = logical_sort(order)
        database, family = _logical_family(parsed, 3)
        cases = (
            ((1, 2, 10), (1, 2, 11), (1, 2, 12)),
            ((1, 2, 7), (1, 2, 7), (1, 2, 8)),
        )
        for rows in cases:
            with self.subTest(rows=rows):
                constants = _database_constants(database, rows)
                self.assertEqual(
                    _sequences(family, constants),
                    _reference_sequences(rows, order),
                )


class OrderedLimitTest(unittest.TestCase):
    def test_count_and_offset_take_exact_sorted_prefix_slices(self):
        order = [order_item("a.k1", True, False)]
        slot_states = tuple(
            (ABSENT,) + tuple((key, 0, slot) for key in (None, 0, 1))
            for slot in range(3)
        )
        for count, offset in product(range(5), repeat=2):
            parsed = logical_ordered_limit(order, count, offset)
            database, family = _logical_family(parsed, 3)
            for rows in product(*slot_states):
                constants = _database_constants(database, rows)
                expected = {
                    sequence[offset : offset + count]
                    for sequence in _reference_sequences(rows, order)
                }
                self.assertEqual(
                    _sequences(family, constants),
                    expected,
                    (count, offset, rows),
                )

    def test_top_sort_is_sort_followed_by_an_ordered_limit(self):
        order = [order_item("a.k1", False, True)]
        top = logical_sort(order, top_limit=2)
        explicit = logical_ordered_limit(order, count=2)
        top_database, top_family = _logical_family(top, 3)
        explicit_script = smt.Script()
        explicit_family = RelationEvaluator(
            explicit,
            top_database,
            ScalarEncoder(explicit_script),
        ).root()
        slot_states = tuple(
            (ABSENT,) + tuple((key, 0, slot) for key in (None, 0, 1))
            for slot in range(3)
        )
        for rows in product(*slot_states):
            constants = _database_constants(top_database, rows)
            self.assertEqual(
                _sequences(top_family, constants),
                _sequences(explicit_family, constants),
                rows,
            )

    def test_sequence_equality_observes_order_while_bag_equality_does_not(self):
        columns = (Column("value", "Int64", False),)

        def relation(values, sequence):
            return single(
                Relation(
                    columns,
                    tuple(
                        Row(
                            smt.TRUE,
                            {"value": Value("Int64", smt.FALSE, smt.int_value(value))},
                        )
                        for value in values
                    ),
                    sequence=sequence,
                )
            )

        scalar = ScalarEncoder(smt.Script())
        self.assertFalse(
            _ground(
                family_equal(
                    relation((1, 2), True),
                    relation((2, 1), True),
                    scalar,
                ),
                {},
            )
        )
        self.assertTrue(
            _ground(
                family_equal(
                    relation((1, 2), False),
                    relation((2, 1), False),
                    scalar,
                ),
                {},
            )
        )

    def test_missing_order_is_expanded_without_spurious_empty_or_single_row_mismatch(self):
        columns = (Column("value", "Int64", False),)

        def relation(values, sequence):
            return single(
                Relation(
                    columns,
                    tuple(
                        Row(
                            smt.TRUE,
                            {"value": Value("Int64", smt.FALSE, smt.int_value(value))},
                        )
                        for value in values
                    ),
                    sequence=sequence,
                )
            )

        scalar = ScalarEncoder(smt.Script())
        for values in ((), (1,)):
            with self.subTest(values=values):
                self.assertTrue(
                    _ground(
                        family_equal(
                            relation(values, True),
                            relation(tuple(reversed(values)), False),
                            scalar,
                        ),
                        {},
                    )
                )
        self.assertFalse(
            _ground(
                family_equal(
                    relation((1, 2), True),
                    relation((2, 1), False),
                    scalar,
                ),
                {},
            )
        )


class SortMutationTest(unittest.TestCase):
    def test_direction_null_placement_key_order_and_top_limit_mutations_are_observable(self):
        ascending = [order_item("a.k1", True, False)]
        descending = [order_item("a.k1", False, False)]
        nulls_first = [order_item("a.k1", True, True)]
        first_then_second = [order_item("a.k1"), order_item("a.k2")]
        second_then_first = [order_item("a.k2"), order_item("a.k1")]
        mutations = (
            (
                "direction",
                logical_sort(ascending),
                logical_sort(descending),
                ((0, 0, 0), (1, 0, 1)),
            ),
            (
                "null placement",
                logical_sort(ascending),
                logical_sort(nulls_first),
                ((None, 0, 0), (0, 0, 1)),
            ),
            (
                "key order",
                logical_sort(first_then_second),
                logical_sort(second_then_first),
                ((0, 1, 0), (1, 0, 1)),
            ),
            (
                "top limit",
                logical_sort(ascending, 1),
                logical_sort(ascending, 2),
                ((0, 0, 0), (1, 0, 1)),
            ),
            (
                "dropped sort",
                logical_sort(ascending),
                parse_snapshot(snapshot([scan()], "scan")),
                ((1, 0, 0), (0, 0, 1)),
            ),
        )
        for name, before, after, rows in mutations:
            with self.subTest(name=name):
                problem = build_logical_kernel_problem_for_tests(before, after, 2)
                self.assertTrue(_counterexample_formula_holds(problem, rows))

    def test_sort_phase_does_not_change_runtime_semantics(self):
        order = [order_item("a.k1", True, False)]
        slot_states = tuple(
            (ABSENT,) + tuple((key, 0, slot) for key in (None, 0, 1))
            for slot in range(2)
        )
        for left_phase, right_phase in (
            ("undefined", "intermediate"),
            ("intermediate", "final"),
            ("final", "undefined"),
        ):
            problem = build_logical_kernel_problem_for_tests(
                logical_sort(order, 1, left_phase),
                logical_sort(order, 1, right_phase),
                2,
            )
            for rows in product(*slot_states):
                with self.subTest(
                    left_phase=left_phase,
                    right_phase=right_phase,
                    rows=rows,
                ):
                    self.assertFalse(_counterexample_formula_holds(problem, rows))

    def test_project_preserves_sequence_for_both_current_rbo_order_flags(self):
        order = [order_item("a.k1", True, False)]
        initial = logical_sort(order)

        def projected(ordered):
            return parse_snapshot(
                snapshot(
                    [
                        scan(),
                        sort_node("sort", "scan", order),
                        project_node("project", "sort", ordered),
                    ],
                    "project",
                )
            )

        rows = ((1, 0, 0), (0, 0, 1))
        for ordered in (False, True):
            with self.subTest(ordered=ordered):
                problem = build_logical_kernel_problem_for_tests(
                    initial,
                    projected(ordered),
                    2,
                )
                self.assertFalse(_counterexample_formula_holds(problem, rows))

    def test_shared_sort_is_not_silently_mutated_into_shared_top_sort(self):
        order = [order_item("a.k1", True, False)]

        def shared(sort_limit):
            return parse_snapshot(
                snapshot(
                    [
                        scan(),
                        sort_node("sort", "scan", order, sort_limit),
                        limit_node("limit", "sort", 1),
                        union_node("union", "limit", "sort"),
                    ],
                    "union",
                )
            )

        problem = build_logical_kernel_problem_for_tests(
            shared(None),
            shared(1),
            2,
        )
        self.assertTrue(
            _counterexample_formula_holds(
                problem,
                ((0, 0, 0), (1, 0, 1)),
            )
        )

    def test_sort_permutation_cap_fails_closed_at_six_slots(self):
        parsed = logical_sort([order_item("a.k1")])

        script = smt.Script()
        database = Database(parsed, 5, script)
        family = RelationEvaluator(parsed, database, ScalarEncoder(script)).root()
        self.assertEqual(len(family.outcomes), 120)

        script = smt.Script()
        database = Database(parsed, 6, script)
        with self.assertRaisesRegex(RelationError, "256 alternative audit bound"):
            RelationEvaluator(parsed, database, ScalarEncoder(script)).root()


class StageTopSortMergeTest(unittest.TestCase):
    ORDER = [order_item("a.k1", True, False)]

    @staticmethod
    def _families(staged):
        logical = logical_ordered_limit(
            StageTopSortMergeTest.ORDER,
            count=1,
            offset=1,
        )
        script = smt.Script()
        database = Database(logical, 2, script)
        scalar = ScalarEncoder(script)
        router = Router(script)
        logical_family = RelationEvaluator(
            logical,
            database,
            scalar,
            choice_scope="before",
        ).root()
        staged_family = StageEvaluator(staged, database, scalar, router).root()
        equality = family_equal(logical_family, staged_family, scalar)
        return database, router, equality

    def test_two_task_local_top_sort_merge_and_final_limit_are_equivalent(self):
        staged = staged_top_sort_merge(self.ORDER)
        self.assertEqual(stage_task_counts(staged), {"source": 2, "root": 1})
        database, router, equality = self._families(staged)
        slot_states = tuple(
            (ABSENT,) + tuple((key, 0, slot) for key in (None, 0, 1))
            for slot in range(2)
        )
        for rows, tasks in product(
            product(*slot_states),
            product((False, True), repeat=2),
        ):
            with self.subTest(rows=rows, tasks=tasks):
                constants = _database_constants(database, rows, router, tasks)
                self.assertTrue(_ground(equality, constants))

    def test_parallel_union_preserves_one_ordered_stream_per_target_for_merge(self):
        logical = logical_sort(self.ORDER)
        staged = staged_parallel_sort_merge(self.ORDER)
        self.assertEqual(
            stage_task_counts(staged),
            {"source": 2, "middle_stage": 2, "root": 1},
        )
        script = smt.Script()
        database = Database(logical, 2, script)
        scalar = ScalarEncoder(script)
        router = Router(script)
        staged_family = StageEvaluator(staged, database, scalar, router).root()
        slot_states = tuple(
            (ABSENT,) + tuple((key, 0, slot) for key in (None, 0, 1))
            for slot in range(2)
        )
        for rows, tasks in product(
            product(*slot_states),
            product((False, True), repeat=2),
        ):
            with self.subTest(rows=rows, tasks=tasks):
                constants = _database_constants(database, rows, router, tasks)
                self.assertEqual(
                    _sequences(staged_family, constants),
                    _reference_sequences(rows, self.ORDER),
                )

    def test_local_limit_merge_order_and_final_limit_mutations_are_observable(self):
        descending = [order_item("a.k1", False, False)]
        mutations = (
            (
                "local top limit too small",
                staged_top_sort_merge(self.ORDER, partial_limit=1),
                ((0, 0, 0), (1, 0, 1)),
                (False, False),
            ),
            (
                "wrong local and merge order",
                staged_top_sort_merge(
                    self.ORDER,
                    partial_order=descending,
                    merge_order=descending,
                ),
                ((0, 0, 0), (1, 0, 1)),
                (False, False),
            ),
            (
                "wrong final offset",
                staged_top_sort_merge(self.ORDER, final_offset=0),
                ((0, 0, 0), (1, 0, 1)),
                (False, True),
            ),
            (
                "wrong final count",
                staged_top_sort_merge(
                    self.ORDER,
                    final_count=2,
                    final_offset=0,
                ),
                ((0, 0, 0), (1, 0, 1)),
                (False, True),
            ),
        )
        for name, staged, rows, tasks in mutations:
            with self.subTest(name=name):
                database, router, equality = self._families(staged)
                constants = _database_constants(database, rows, router, tasks)
                self.assertFalse(_ground(equality, constants))

    def test_merge_order_must_match_every_local_sort(self):
        descending = [order_item("a.k1", False, False)]
        staged = staged_top_sort_merge(
            self.ORDER,
            partial_order=self.ORDER,
            merge_order=descending,
        )
        logical = logical_ordered_limit(self.ORDER, count=1, offset=1)
        with self.assertRaisesRegex(VerificationError, "merge edge .* input order differs"):
            build_problem(logical, staged, 2)


if __name__ == "__main__":
    unittest.main()
