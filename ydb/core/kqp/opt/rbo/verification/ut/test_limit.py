import copy
import unittest
from itertools import combinations, permutations, product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr,
    SnapshotError,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Column,
    Database,
    Evaluator as RelationEvaluator,
    Outcome,
    Relation,
    RelationError,
    RelationFamily,
    Row,
    Value,
    combine_families,
    family_equal,
    limit_family,
    map_family,
    single,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import Encoder as ScalarEncoder
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.stages import (
    Evaluator as StageEvaluator,
    Router,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import (
    VerificationError,
    build_logical_kernel_problem_for_tests,
    build_problem,
)


def uint64(value):
    return {"kind": "literal", "type": "Uint64", "value": value}


def scan(pushed_limit=None):
    return {
        "id": "scan",
        "op": "scan",
        "table": "A",
        "columns": [{"source": "value", "output": "a.value"}],
        "pushed_limit": pushed_limit,
    }


def limit(
    node_id,
    input_id,
    count,
    offset=None,
    phase="undefined",
    ensure_at_most_one=None,
):
    result = {
        "id": node_id,
        "op": "limit",
        "input": input_id,
        "count": uint64(count),
        "offset": None if offset is None else uint64(offset),
        "phase": phase,
    }
    if ensure_at_most_one is not None:
        result["ensure_at_most_one"] = ensure_at_most_one
    return result


def pass_project(node_id, output):
    return {
        "id": node_id,
        "op": "project",
        "input": "scan",
        "ordered": False,
        "columns": [
            {
                "output": output,
                "expression": {"kind": "column", "column": "a.value"},
            }
        ],
    }


def constant_project(node_id, input_id, output, value):
    return {
        "id": node_id,
        "op": "project",
        "input": input_id,
        "ordered": False,
        "columns": [
            {
                "output": output,
                "expression": {
                    "kind": "literal",
                    "type": "Int64",
                    "value": value,
                },
            }
        ],
    }


def false_filter(node_id, input_id):
    return {
        "id": node_id,
        "op": "filter",
        "input": input_id,
        "predicate": {"kind": "literal", "type": "Bool", "value": False},
    }


def union_all(node_id, left, left_column, right, right_column, *, ordered):
    return {
        "id": node_id,
        "op": "union_all",
        "inputs": [
            {"node": left, "columns": [left_column]},
            {"node": right, "columns": [right_column]},
        ],
        "output": ["a.value"],
        "ordered": ordered,
    }


def snapshot(nodes, root, stage_graph=None, nullable=False):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "value", "type": "Int64", "nullable": nullable}
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


def logical_limit(count, offset=None, phase="undefined"):
    return parse_snapshot(
        snapshot(
            [scan(), limit("limit", "scan", count, offset, phase)],
            "limit",
        )
    )


def column_scan_with_pushed_limit(count):
    return parse_snapshot(
        snapshot(
            [scan(uint64(count))],
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


def staged_limits(
    intermediate_count,
    final_count,
    *,
    intermediate_ensure=False,
    final_ensure=False,
):
    nodes = [scan()]
    source_root = "scan"
    source_nodes = ["scan"]
    if intermediate_count is not None:
        nodes.append(
            limit(
                "partial",
                "scan",
                intermediate_count,
                phase="intermediate",
                ensure_at_most_one=intermediate_ensure,
            )
        )
        source_root = "partial"
        source_nodes.append("partial")

    source_stage = {
        "id": "source",
        "nodes": source_nodes,
        "inputs": [],
        "outputs": [{"index": 0, "node": source_root}],
        "source_storage": "column",
    }
    if final_count is None:
        graph = {
            "root_stage": "source",
            "stages": [source_stage],
            "edges": [],
            "assumptions": [],
        }
        root = source_root
    else:
        nodes.append(
            limit(
                "final",
                source_root,
                final_count,
                phase="final",
                ensure_at_most_one=final_ensure,
            )
        )
        graph = {
            "root_stage": "root",
            "stages": [
                source_stage,
                {
                    "id": "root",
                    "nodes": ["final"],
                    "inputs": [source_root],
                    "outputs": [{"index": 0, "node": "final"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "gather",
                    "producer": "source",
                    "consumer": "root",
                    "occurrence": 0,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "union_all",
                    "parallel": False,
                }
            ],
            "assumptions": [],
        }
        root = "final"
    return parse_snapshot(snapshot(nodes, root, graph))


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
        branch = term.arguments[1] if _ground(term.arguments[0], constants) else term.arguments[2]
        return _ground(branch, constants)
    if term.operation == "+":
        return sum(_ground(argument, constants) for argument in term.arguments)
    if term.operation == "mod":
        return _ground(term.arguments[0], constants) % _ground(
            term.arguments[1], constants
        )
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _constants(database, present, values=None):
    constants = {}
    values = tuple(range(10, 10 + len(present))) if values is None else values
    for row, is_present, value in zip(database.witness["A"], present, values):
        constants[row.present.atom] = is_present
        cell = row.cells["value"]
        if cell.is_null.operation == "symbol":
            constants[cell.is_null.atom] = value is None
        constants[cell.value.atom] = 0 if value is None else value
    return constants


def _bags(family, constants):
    bags = set()
    for outcome in family.outcomes:
        if not _ground(outcome.enabled, constants):
            continue
        values = []
        for row in outcome.relation.rows:
            if _ground(row.present, constants):
                value = row.values["a.value"]
                values.append(
                    None
                    if _ground(value.is_null, constants)
                    else _ground(value.value, constants)
                )
        bags.add(_bag(values))
    return bags


def _bag(values):
    return tuple(sorted(values, key=lambda value: (value is not None, repr(value))))


def _reference_limit(present, count, offset, values=None):
    values = tuple(range(10, 10 + len(present))) if values is None else values
    rows = [value for value, active in zip(values, present) if active]
    size = min(count, max(len(rows) - offset, 0))
    return {_bag(choice) for choice in combinations(rows, size)}


class LimitIrTest(unittest.TestCase):
    def test_literal_count_offset_and_all_phases_are_strictly_decoded(self):
        for phase, count, offset in product(
            ("undefined", "intermediate", "final"),
            (7, (1 << 64) - 1),
            (3, (1 << 64) - 1),
        ):
            with self.subTest(phase=phase, count=count, offset=offset):
                parsed = logical_limit(count, offset, phase).plan.nodes[-1]
                self.assertEqual(parsed.count.value, count)
                self.assertEqual(parsed.offset.value, offset)
                self.assertEqual(parsed.phase, phase)
                self.assertFalse(parsed.ensure_at_most_one)

        checked = parse_snapshot(
            snapshot(
                [
                    scan(),
                    limit(
                        "limit",
                        "scan",
                        2,
                        ensure_at_most_one=True,
                    ),
                ],
                "limit",
            )
        ).plan.nodes[-1]
        self.assertTrue(checked.ensure_at_most_one)

    def test_limit_shape_and_literal_contract_fail_closed(self):
        base = snapshot([scan(), limit("limit", "scan", 1)], "limit")
        mutations = []

        missing = copy.deepcopy(base)
        del missing["plan"]["nodes"][1]["offset"]
        mutations.append((missing, "missing fields: offset"))

        unknown = copy.deepcopy(base)
        unknown["plan"]["nodes"][1]["ordered"] = False
        mutations.append((unknown, "unknown fields: ordered"))

        bad_phase = copy.deepcopy(base)
        bad_phase["plan"]["nodes"][1]["phase"] = "partial"
        mutations.append((bad_phase, "unsupported limit phase"))

        bad_counts = (
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
        for count in bad_counts:
            value = copy.deepcopy(base)
            value["plan"]["nodes"][1]["count"] = count
            mutations.append((value, "Uint64 literal"))

        bad_offset = copy.deepcopy(base)
        bad_offset["plan"]["nodes"][1]["offset"] = {
            "kind": "column",
            "column": "a.value",
        }
        mutations.append((bad_offset, "Uint64 literal"))

        for malformed in (0, 1, "true", None):
            bad_marker = copy.deepcopy(base)
            bad_marker["plan"]["nodes"][1]["ensure_at_most_one"] = malformed
            mutations.append((bad_marker, "expected a Boolean"))

        for value, message in mutations:
            with self.subTest(message=message, value=value):
                with self.assertRaisesRegex(SnapshotError, message):
                    parse_snapshot(value)

    def test_pushed_limit_is_only_valid_on_a_column_source(self):
        logical = snapshot([scan(uint64(1))], "scan")
        with self.assertRaisesRegex(SnapshotError, "column-storage source stage"):
            parse_snapshot(logical)

        invalid_literal = snapshot([scan(uint64(-1))], "scan")
        with self.assertRaisesRegex(SnapshotError, "outside"):
            parse_snapshot(invalid_literal)

        column = column_scan_with_pushed_limit(1)
        raw_row = snapshot(
            [scan(uint64(1))],
            "scan",
            {
                "root_stage": "source",
                "stages": [
                    {
                        "id": "source",
                        "nodes": ["scan"],
                        "inputs": [],
                        "outputs": [{"index": 0, "node": "scan"}],
                        "source_storage": "row",
                    }
                ],
                "edges": [],
                "assumptions": [],
            },
        )
        with self.assertRaisesRegex(SnapshotError, "requires column storage"):
            parse_snapshot(raw_row)
        self.assertIsNotNone(column.plan.nodes[0].pushed_limit)


class LimitOutcomeTest(unittest.TestCase):
    def test_query_error_is_distinct_from_every_successful_result(self):
        columns = (Column("a.value", "Int64", False),)
        empty = Relation(columns, ())
        row = Row(
            smt.TRUE,
            {"a.value": Value("Int64", smt.FALSE, smt.int_value(7))},
        )
        error = RelationFamily((Outcome(smt.TRUE, empty, smt.TRUE),))
        empty_success = single(empty)
        row_success = single(Relation(columns, (row,)))
        other_error_payload = RelationFamily((
            Outcome(smt.TRUE, Relation(columns, (row,)), smt.TRUE),
        ))
        scalar = ScalarEncoder(smt.Script())

        self.assertFalse(_ground(family_equal(error, empty_success, scalar), {}))
        self.assertFalse(_ground(family_equal(error, row_success, scalar), {}))
        self.assertTrue(
            _ground(family_equal(error, other_error_payload, scalar), {})
        )

    def test_family_combinators_preserve_and_join_query_errors(self):
        columns = (Column("a.value", "Int64", False),)
        relation = Relation(columns, ())
        left_error = smt.symbol("left_error", smt.BOOL)
        right_error = smt.symbol("right_error", smt.BOOL)
        left = RelationFamily((Outcome(smt.TRUE, relation, left_error),))
        right = RelationFamily((Outcome(smt.TRUE, relation, right_error),))

        mapped = map_family(left, lambda item: item)
        combined = combine_families((left, right), lambda items: items[0])

        self.assertIs(mapped.outcomes[0].error, left_error)
        for left_value, right_value in product((False, True), repeat=2):
            self.assertEqual(
                _ground(
                    combined.outcomes[0].error,
                    {
                        "left_error": left_value,
                        "right_error": right_value,
                    },
                ),
                left_value or right_value,
            )

        limited = limit_family(
            left,
            Expr(
                kind="literal",
                value=0,
                result_type="Uint64",
                nullable=False,
            ),
            None,
            "zero",
        )
        self.assertIs(limited.outcomes[0].error, left_error)

    def test_query_error_remains_correlated_with_successful_payloads(self):
        columns = (Column("a.value", "Int64", False),)

        def payload(value):
            return Relation(
                columns,
                (
                    Row(
                        smt.TRUE,
                        {
                            "a.value": Value(
                                "Int64",
                                smt.FALSE,
                                smt.int_value(value),
                            )
                        },
                    ),
                ),
            )

        left = RelationFamily((
            Outcome(smt.TRUE, payload(1), smt.TRUE, (("left", 0),)),
            Outcome(smt.TRUE, payload(2), smt.FALSE, (("left", 1),)),
        ))
        right = RelationFamily((
            Outcome(smt.TRUE, payload(2), smt.TRUE, (("right", 0),)),
            Outcome(smt.TRUE, payload(1), smt.FALSE, (("right", 1),)),
        ))
        scalar = ScalarEncoder(smt.Script())

        self.assertFalse(_ground(family_equal(left, right, scalar), {}))

    def test_ensure_at_most_one_checks_the_post_limit_relation(self):
        cases = (
            (1, None, (True, True), False),
            (2, None, (True, True), True),
            (2, 1, (True, True), False),
            (2, 1, (True, True, True), True),
        )
        for count, offset, present, expected_error in cases:
            parsed = parse_snapshot(
                snapshot(
                    [
                        scan(),
                        limit(
                            "checked",
                            "scan",
                            count,
                            offset,
                            ensure_at_most_one=True,
                        ),
                    ],
                    "checked",
                )
            )
            script = smt.Script()
            database = Database(parsed, len(present), script)
            family = RelationEvaluator(
                parsed,
                database,
                ScalarEncoder(script),
            ).root()
            constants = _constants(database, present)
            enabled_errors = {
                _ground(outcome.error, constants)
                for outcome in family.outcomes
                if _ground(outcome.enabled, constants)
            }
            with self.subTest(count=count, offset=offset, present=present):
                self.assertEqual(enabled_errors, {expected_error})

    def test_dropping_ensure_at_most_one_is_observable(self):
        checked = parse_snapshot(
            snapshot(
                [
                    scan(),
                    limit(
                        "limit",
                        "scan",
                        2,
                        ensure_at_most_one=True,
                    ),
                ],
                "limit",
            )
        )
        unchecked = parse_snapshot(
            snapshot([scan(), limit("limit", "scan", 2)], "limit")
        )
        script = smt.Script()
        database = Database(checked, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            RelationEvaluator(checked, database, scalar).root(),
            RelationEvaluator(unchecked, database, scalar).root(),
            scalar,
        )

        self.assertFalse(
            _ground(equality, _constants(database, (True, True)))
        )

    def test_ordered_union_limit_prefers_left_branch_and_uses_right_fallback(self):
        for omit_left, expected in ((False, 10), (True, 20)):
            nodes = [
                {"id": "left_unit", "op": "empty_source"},
                constant_project("left", "left_unit", "left.value", 10),
            ]
            left = "left"
            if omit_left:
                nodes.append(false_filter("no_left", left))
                left = "no_left"
            nodes.extend(
                [
                    {"id": "right_unit", "op": "empty_source"},
                    constant_project("right", "right_unit", "right.value", 20),
                    union_all(
                        "union",
                        left,
                        "left.value",
                        "right",
                        "right.value",
                        ordered=True,
                    ),
                    limit("first", "union", 1),
                ]
            )
            parsed = parse_snapshot(snapshot(nodes, "first"))
            script = smt.Script()
            family = RelationEvaluator(
                parsed,
                Database(parsed, 1, script),
                ScalarEncoder(script),
            ).root()
            with self.subTest(omit_left=omit_left):
                self.assertTrue(family.sequence)
                self.assertEqual(_bags(family, {}), {(expected,)})

    def test_ordered_union_branch_precedence_with_symbolic_input_ordinals(self):
        nodes = [
            scan(),
            {"id": "fallback_unit", "op": "empty_source"},
            constant_project(
                "fallback",
                "fallback_unit",
                "fallback.value",
                99,
            ),
            union_all(
                "union",
                "scan",
                "a.value",
                "fallback",
                "fallback.value",
                ordered=True,
            ),
            limit("first", "union", 1),
        ]
        parsed = parse_snapshot(snapshot(nodes, "first"))
        script = smt.Script()
        database = Database(parsed, 4, script)
        family = RelationEvaluator(
            parsed,
            database,
            ScalarEncoder(script),
        ).root()
        self.assertEqual(len(family.outcomes), 1)
        outcome = family.outcomes[0]
        self.assertEqual(len(outcome.choices), 4)
        self.assertIsNotNone(outcome.relation.ordinals)

        for present in product((False, True), repeat=4):
            present_indexes = tuple(
                index for index, is_present in enumerate(present) if is_present
            )
            for order in permutations(range(len(present_indexes))):
                input_ordinals = [0] * 4
                for index, ordinal in zip(present_indexes, order):
                    input_ordinals[index] = ordinal
                constants = _constants(database, present)
                constants.update(
                    {
                        choice.term.atom: ordinal
                        for choice, ordinal in zip(
                            outcome.choices,
                            input_ordinals,
                        )
                    }
                )
                self.assertTrue(_ground(outcome.enabled, constants))
                expected = (
                    99
                    if not present_indexes
                    else 10
                    + min(
                        present_indexes,
                        key=input_ordinals.__getitem__,
                    )
                )
                self.assertEqual(_bags(family, constants), {(expected,)})

    def test_ordered_union_gives_symbolic_branches_independent_ordinals(self):
        parsed = parse_snapshot(
            snapshot(
                [
                    scan(),
                    union_all(
                        "union",
                        "scan",
                        "a.value",
                        "scan",
                        "a.value",
                        ordered=True,
                    ),
                ],
                "union",
            )
        )
        script = smt.Script()
        family = RelationEvaluator(
            parsed,
            Database(parsed, 4, script),
            ScalarEncoder(script),
        ).root()
        choices = family.outcomes[0].choices
        self.assertEqual(len(choices), 8)
        self.assertEqual(
            len({choice.term.atom for choice in choices}),
            len(choices),
        )

    def test_unordered_limit_matches_an_independent_exhaustive_reference(self):
        for count, offset in product(range(5), range(5)):
            with self.subTest(count=count, offset=offset):
                parsed = logical_limit(count, None if offset == 0 else offset)
                script = smt.Script()
                database = Database(parsed, 3, script)
                family = RelationEvaluator(
                    parsed, database, ScalarEncoder(script)
                ).root()
                for present in product((False, True), repeat=3):
                    constants = _constants(database, present)
                    self.assertEqual(
                        _bags(family, constants),
                        _reference_limit(present, count, offset),
                        (count, offset, present),
                    )

    def test_exhaustive_reference_covers_duplicate_and_null_values(self):
        values = (7, 7, None)
        for count, offset in product(range(4), range(4)):
            raw = snapshot(
                [scan(), limit("limit", "scan", count, offset)],
                "limit",
                nullable=True,
            )
            parsed = parse_snapshot(raw)
            script = smt.Script()
            database = Database(parsed, 3, script)
            family = RelationEvaluator(
                parsed, database, ScalarEncoder(script)
            ).root()
            for present in product((False, True), repeat=3):
                with self.subTest(count=count, offset=offset, present=present):
                    constants = _constants(database, present, values)
                    self.assertEqual(
                        _bags(family, constants),
                        _reference_limit(present, count, offset, values),
                    )

    def test_shared_limit_node_uses_one_correlated_choice(self):
        parsed = parse_snapshot(
            snapshot(
                [
                    scan(),
                    limit("limit", "scan", 1),
                    {
                        "id": "union",
                        "op": "union_all",
                        "inputs": [
                            {"node": "limit", "columns": ["a.value"]},
                            {"node": "limit", "columns": ["a.value"]},
                        ],
                        "output": ["a.value"],
                        "ordered": False,
                    },
                ],
                "union",
            )
        )
        script = smt.Script()
        database = Database(parsed, 2, script)
        family = RelationEvaluator(parsed, database, ScalarEncoder(script)).root()
        self.assertEqual(
            _bags(family, _constants(database, (True, True))),
            {(10, 10), (11, 11)},
        )

    def test_distinct_limit_branches_over_one_stream_fail_closed(self):
        direct = parse_snapshot(
            snapshot(
                [
                    scan(),
                    limit("left", "scan", 1),
                    limit("right", "scan", 1),
                    {
                        "id": "union",
                        "op": "union_all",
                        "inputs": [
                            {"node": "left", "columns": ["a.value"]},
                            {"node": "right", "columns": ["a.value"]},
                        ],
                        "output": ["a.value"],
                        "ordered": False,
                    },
                ],
                "union",
            )
        )

        intervening = parse_snapshot(
            snapshot(
                [
                    scan(),
                    pass_project("left_project", "left.value"),
                    pass_project("right_project", "right.value"),
                    limit("left", "left_project", 1),
                    limit("right", "right_project", 1),
                    {
                        "id": "union",
                        "op": "union_all",
                        "inputs": [
                            {"node": "left", "columns": ["left.value"]},
                            {"node": "right", "columns": ["right.value"]},
                        ],
                        "output": ["a.value"],
                        "ordered": False,
                    },
                ],
                "union",
            )
        )
        for parsed in (direct, intervening):
            with self.subTest(parsed=parsed):
                with self.assertRaisesRegex(VerificationError, "correlated fan-out"):
                    build_problem(parsed, column_scan_with_pushed_limit(1), 2)

    def test_fanout_reconverged_before_sequential_limits_is_supported(self):
        reconverged = parse_snapshot(
            snapshot(
                [
                    scan(),
                    pass_project("left_project", "left.value"),
                    pass_project("right_project", "right.value"),
                    {
                        "id": "union",
                        "op": "union_all",
                        "inputs": [
                            {"node": "left_project", "columns": ["left.value"]},
                            {"node": "right_project", "columns": ["right.value"]},
                        ],
                        "output": ["a.value"],
                        "ordered": False,
                    },
                    limit("first", "union", 2),
                    limit("second", "first", 1),
                ],
                "second",
            )
        )
        script = smt.Script()
        database = Database(reconverged, 2, script)
        family = RelationEvaluator(
            reconverged, database, ScalarEncoder(script)
        ).root()
        self.assertTrue(family.outcomes)

    def test_mutations_and_fixed_prefix_are_observable(self):
        mutations = (
            (logical_limit(2), (True, True)),
            (logical_limit(1, 1), (True, False)),
        )
        for after, present in mutations:
            with self.subTest(after=after, present=present):
                problem = build_logical_kernel_problem_for_tests(
                    logical_limit(1), after, 2
                )
                constants = _constants_from_witness(problem.witness, present)
                self.assertTrue(
                    all(_ground(assertion, constants) for assertion in problem.script.assertions)
                )

        parsed = logical_limit(1)
        script = smt.Script()
        database = Database(parsed, 2, script)
        scalar = ScalarEncoder(script)
        unordered = RelationEvaluator(parsed, database, scalar).root()
        raw = database.relations["A"]
        first_only = single(
            Relation(
                raw.columns,
                (
                    raw.rows[0],
                    Row(smt.FALSE, raw.rows[1].values),
                ),
            )
        )
        constants = _constants(database, (True, True))
        self.assertFalse(_ground(family_equal(unordered, first_only, scalar), constants))

    def test_phase_does_not_change_runtime_semantics(self):
        problem = build_logical_kernel_problem_for_tests(
            logical_limit(1, phase="undefined"),
            logical_limit(1, phase="final"),
            2,
        )
        for present in product((False, True), repeat=2):
            constants = _constants_from_witness(problem.witness, present)
            self.assertFalse(
                all(_ground(assertion, constants) for assertion in problem.script.assertions)
            )

    def test_limit_crossing_filter_is_observable(self):
        predicate = {
            "kind": "eq",
            "left": {"kind": "column", "column": "a.value"},
            "right": {"kind": "literal", "type": "Int64", "value": 10},
        }
        before = parse_snapshot(
            snapshot(
                [
                    scan(),
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "scan",
                        "predicate": predicate,
                    },
                    limit("limit", "filter", 1),
                ],
                "limit",
            )
        )
        after = parse_snapshot(
            snapshot(
                [
                    scan(),
                    limit("limit", "scan", 1),
                    {
                        "id": "filter",
                        "op": "filter",
                        "input": "limit",
                        "predicate": predicate,
                    },
                ],
                "filter",
            )
        )
        script = smt.Script()
        database = Database(before, 2, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            RelationEvaluator(before, database, scalar, choice_scope="before").root(),
            RelationEvaluator(after, database, scalar, choice_scope="after").root(),
            scalar,
        )
        self.assertFalse(_ground(equality, _constants(database, (True, True))))

    def test_limit_commutes_with_deterministic_non_injective_project(self):
        project = {
            "id": "project",
            "op": "project",
            "input": "unused",
            "ordered": False,
            "columns": [
                {
                    "output": "a.value",
                    "expression": {
                        "kind": "literal",
                        "type": "Int64",
                        "value": 0,
                    },
                }
            ],
        }
        before_project = copy.deepcopy(project)
        before_project["input"] = "limit"
        before = parse_snapshot(
            snapshot(
                [scan(), limit("limit", "scan", 1), before_project],
                "project",
            )
        )
        after_project = copy.deepcopy(project)
        after_project["input"] = "scan"
        after = parse_snapshot(
            snapshot(
                [scan(), after_project, limit("limit", "project", 1)],
                "limit",
            )
        )
        script = smt.Script()
        database = Database(before, 3, script)
        scalar = ScalarEncoder(script)
        equality = family_equal(
            RelationEvaluator(before, database, scalar, choice_scope="before").root(),
            RelationEvaluator(after, database, scalar, choice_scope="after").root(),
            scalar,
        )
        for present in product((False, True), repeat=3):
            self.assertTrue(_ground(equality, _constants(database, present)), present)

    def test_alternative_cap_fails_closed(self):
        parsed = logical_limit(9)
        with self.assertRaisesRegex(VerificationError, "alternative audit bound"):
            build_logical_kernel_problem_for_tests(parsed, parsed, 9)

    def test_outcome_comparison_cap_fails_closed(self):
        parsed = logical_limit(4)
        with self.assertRaisesRegex(VerificationError, "4096 pair audit bound"):
            build_logical_kernel_problem_for_tests(parsed, parsed, 7)


class StageLimitOutcomeTest(unittest.TestCase):
    def test_ensure_at_most_one_is_checked_in_each_stage_task(self):
        def errors(parsed, tasks):
            script = smt.Script()
            database = Database(parsed, 2, script)
            router = Router(script)
            family = StageEvaluator(
                parsed,
                database,
                ScalarEncoder(script),
                router,
            ).root()
            constants = _constants(database, (True, True))
            for slot, task in enumerate(tasks):
                constants[router.source_task("A", slot).atom] = task
            return {
                _ground(outcome.error, constants)
                for outcome in family.outcomes
                if _ground(outcome.enabled, constants)
            }

        local = staged_limits(
            2,
            None,
            intermediate_ensure=True,
        )
        gathered = staged_limits(
            None,
            2,
            final_ensure=True,
        )

        self.assertEqual(errors(local, (False, True)), {False})
        self.assertEqual(errors(local, (False, False)), {True})
        self.assertEqual(errors(gathered, (False, True)), {True})

    def test_pushed_scan_limit_is_applied_independently_after_partitioning(self):
        parsed = column_scan_with_pushed_limit(1)
        script = smt.Script()
        database = Database(parsed, 3, script)
        router = Router(script)
        family = StageEvaluator(
            parsed, database, ScalarEncoder(script), router
        ).root()
        for present, tasks in product(
            product((False, True), repeat=3),
            product((False, True), repeat=3),
        ):
            constants = _constants(database, present)
            for slot, task in enumerate(tasks):
                constants[router.source_task("A", slot).atom] = task
            expected = {()}
            for task in (False, True):
                members = [
                    slot + 10
                    for slot, active in enumerate(present)
                    if active and tasks[slot] == task
                ]
                choices = [()] if not members else [(member,) for member in members]
                expected = {
                    tuple(sorted(left + right))
                    for left, right in product(expected, choices)
                }
            self.assertEqual(_bags(family, constants), expected, (present, tasks))

    def test_split_limit_phase_mutations_have_the_expected_semantics(self):
        logical = logical_limit(1)

        def compare(staged, present, tasks):
            script = smt.Script()
            database = Database(logical, 2, script)
            scalar = ScalarEncoder(script)
            router = Router(script)
            equality = family_equal(
                RelationEvaluator(logical, database, scalar).root(),
                StageEvaluator(staged, database, scalar, router).root(),
                scalar,
            )
            constants = _constants(database, present)
            for slot, task in enumerate(tasks):
                constants[router.source_task("A", slot).atom] = task
            return _ground(equality, constants)

        for name, staged in (
            ("split", staged_limits(1, 1)),
            ("dropped intermediate", staged_limits(None, 1)),
        ):
            for present, tasks in product(
                product((False, True), repeat=2),
                product((False, True), repeat=2),
            ):
                with self.subTest(name=name, present=present, tasks=tasks):
                    self.assertTrue(compare(staged, present, tasks))

        self.assertFalse(
            compare(staged_limits(1, None), (True, True), (False, True)),
            "dropping the final limit can retain one row from each source task",
        )
        self.assertFalse(
            compare(staged_limits(0, 1), (True, False), (False, False)),
            "an intermediate limit smaller than the final limit loses rows",
        )

    def test_pushed_limit_cannot_escape_the_stage_evaluator(self):
        parsed = column_scan_with_pushed_limit(1)
        script = smt.Script()
        database = Database(parsed, 1, script)
        with self.assertRaisesRegex(RelationError, "per column-source task"):
            RelationEvaluator(parsed, database, ScalarEncoder(script)).root()
        with self.assertRaisesRegex(VerificationError, "initial snapshot"):
            build_problem(parsed, parsed, 1)


def _constants_from_witness(witness, present):
    constants = {}
    for slot, (row, active) in enumerate(zip(witness["A"], present)):
        constants[row.present.atom] = active
        constants[row.cells["value"].value.atom] = slot + 10
    return constants


if __name__ == "__main__":
    unittest.main()
