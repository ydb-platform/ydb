import copy
import os
import subprocess
import unittest
from itertools import combinations, permutations, product

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Expr,
    SnapshotError,
    SortOrder,
    parse_snapshot,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Column,
    Database,
    Evaluator as RelationEvaluator,
    BoundedChoice,
    Outcome,
    PartitionFact,
    Relation,
    RelationError,
    RelationFamily,
    Row,
    Value,
    combine_families,
    compare_families,
    family_equal,
    limit_family,
    map_family,
    merge_family,
    single,
    sort_family,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    DecimalAverageState,
    Encoder as ScalarEncoder,
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


SOLVER = (
    yatest_common.binary_path("contrib/tools/z3/z3")
    if yatest_common is not None
    else os.environ.get("RBO_Z3")
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
    if term.operation in {"forall", "exists"}:
        variables = term.arguments[:-1]
        body = term.arguments[-1]
        domains = tuple(
            _quantifier_domain(variable, body)
            for variable in variables
        )
        values = (
            _ground(
                body,
                constants
                | dict(zip(
                    (variable.atom for variable in variables),
                    assignment,
                )),
            )
            for assignment in product(*domains)
        )
        return (
            all(values)
            if term.operation == "forall"
            else any(values)
        )
    raise AssertionError(f"unsupported ground SMT operation {term.operation!r}")


def _quantifier_domain(variable, body):
    if variable.sort == smt.BOOL:
        return (False, True)
    bounds = []

    def visit(term):
        if (
            term.operation == "<"
            and term.arguments[0] == variable
            and term.arguments[1].operation == "int"
        ):
            bounds.append(term.arguments[1].atom)
        for argument in term.arguments:
            visit(argument)

    visit(body)
    positive = [bound for bound in bounds if type(bound) is int and bound > 0]
    if not positive:
        raise AssertionError(f"cannot infer finite domain for {variable.atom}")
    return range(max(positive))


def _free_symbols(term, bound=frozenset()):
    if term.operation == "symbol":
        return set() if term.atom in bound else {term}
    if term.operation in {"forall", "exists"}:
        variables = term.arguments[:-1]
        nested = bound | frozenset(variable.atom for variable in variables)
        return _free_symbols(term.arguments[-1], nested)
    result = set()
    for argument in term.arguments:
        result.update(_free_symbols(argument, bound))
    return result


def _satisfiable(term, constants):
    free = sorted(
        (
            symbol
            for symbol in _free_symbols(term)
            if symbol.atom not in constants
        ),
        key=lambda symbol: symbol.atom,
    )
    domains = tuple(_quantifier_domain(symbol, term) for symbol in free)
    return any(
        _ground(
            term,
            constants
            | dict(zip(
                (symbol.atom for symbol in free),
                assignment,
            )),
        )
        for assignment in product(*domains)
    )


def _holds_for_all_choices(term, constants):
    return not _satisfiable(smt.not_(term), constants)


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
    for outcome, grounded in _enabled_outcomes(family, constants):
        values = []
        for row in outcome.relation.rows:
            if _ground(row.present, grounded):
                value = row.values["a.value"]
                values.append(
                    None
                    if _ground(value.is_null, grounded)
                    else _ground(value.value, grounded)
                )
        bags.add(_bag(values))
    return bags


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
            if not _ground(outcome.enabled, grounded):
                continue
            enabled.append((outcome, grounded))
    return tuple(enabled)


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
    @staticmethod
    def _two_outcome_family(script, scope, first, second):
        choice = script.fresh_constant(f"{scope} choice", smt.INT)
        error = script.fresh_constant(f"{scope} conditional error", smt.BOOL)
        columns = (Column("value", "Int64", False),)

        def payload(value):
            return Relation(
                columns,
                (
                    Row(
                        smt.TRUE,
                        {
                            "value": Value(
                                "Int64",
                                smt.FALSE,
                                smt.int_value(value),
                            )
                        },
                    ),
                ),
            )

        bounded = (BoundedChoice(choice, 2),)
        return RelationFamily((
            Outcome(
                smt.eq(choice, smt.ZERO),
                payload(first),
                error,
                choices=bounded,
            ),
            Outcome(
                smt.eq(choice, smt.ONE),
                payload(second),
                smt.not_(error),
                choices=bounded,
            ),
        ))

    def test_mismatch_branches_have_one_fixed_canonical_order(self):
        script = smt.Script()
        comparison = compare_families(
            self._two_outcome_family(script, "left", 1, 2),
            self._two_outcome_family(script, "right", 1, 3),
            ScalarEncoder(script),
        )

        self.assertEqual(
            tuple(branch.name for branch in comparison.mismatch.branches),
            (
                "left_language_empty",
                "right_language_empty",
                "left_outcome_0_unmatched",
                "left_outcome_1_unmatched",
                "right_outcome_0_unmatched",
                "right_outcome_1_unmatched",
            ),
        )

    @unittest.skipUnless(SOLVER, "run through ya or set RBO_Z3")
    def test_mismatch_branches_exactly_decompose_grouped_counterexample(self):
        script = smt.Script(timeout_ms=10_000)
        comparison = compare_families(
            self._two_outcome_family(script, "left", 1, 2),
            self._two_outcome_family(script, "right", 1, 3),
            ScalarEncoder(script),
        )
        distributed = smt.or_(
            *(branch.predicate for branch in comparison.mismatch.branches)
        )
        script.assert_term(
            smt.not_(
                smt.eq(comparison.mismatch.counterexample, distributed)
            )
        )

        solved = subprocess.run(
            (SOLVER, "-in"),
            input=script.render(),
            text=True,
            capture_output=True,
            check=False,
            timeout=15,
        )

        self.assertEqual(solved.returncode, 0, solved.stderr)
        self.assertEqual(solved.stdout.strip(), "unsat")

    def test_comparison_registers_and_range_guards_hand_built_choices(self):
        script = smt.Script()
        choice = script.fresh_constant("choice", smt.INT)
        family = RelationFamily((
            Outcome(
                smt.TRUE,
                Relation((), ()),
                smt.FALSE,
                choices=(BoundedChoice(choice, 2),),
            ),
        ))

        comparison = compare_families(
            family,
            single(Relation((), ())),
            ScalarEncoder(script),
        )

        self.assertEqual(script.quantified_choice_bound(choice), 2)
        enabled = comparison.left.outcomes[0].enabled
        self.assertFalse(_ground(enabled, {choice.atom: -1}))
        self.assertTrue(_ground(enabled, {choice.atom: 0}))
        self.assertFalse(_ground(enabled, {choice.atom: 2}))

    def test_comparison_rejects_untracked_choice_dependencies_in_every_outcome_field(self):
        def dependent_family(location, choice):
            predicate = smt.eq(choice, smt.ZERO)
            columns = (Column("value", "Int64", True),)
            value = Value("Int64", smt.FALSE, smt.ZERO)
            row = Row(smt.TRUE, {"value": value})
            enabled = smt.TRUE
            error = smt.FALSE
            ordinals = None
            if location == "enabled":
                enabled = predicate
            elif location == "error":
                error = predicate
            elif location == "present":
                row = Row(predicate, row.values)
            elif location == "value":
                row = Row(
                    smt.TRUE,
                    {"value": Value("Int64", predicate, choice)},
                )
            elif location == "ordinal":
                ordinals = (choice,)
            elif location == "partition":
                row = Row(
                    smt.TRUE,
                    row.values,
                    partition_facts=frozenset((
                        PartitionFact(predicate, True),
                    )),
                )
            elif location == "decimal state":
                columns = (Column("value", "Decimal(5,2)", False),)
                row = Row(
                    smt.TRUE,
                    {
                        "value": Value(
                            "Decimal(5,2)",
                            smt.FALSE,
                            smt.ZERO,
                            0,
                            DecimalAverageState(
                                "Decimal(35,2)",
                                choice,
                                choice,
                                1,
                                1,
                            ),
                        )
                    },
                )
            else:
                raise AssertionError(location)
            return RelationFamily((
                Outcome(
                    enabled,
                    Relation(
                        columns,
                        (row,),
                        sequence=ordinals is not None,
                        ordinals=ordinals,
                    ),
                    error,
                ),
            ))

        for location in (
            "enabled",
            "error",
            "present",
            "value",
            "ordinal",
            "partition",
            "decimal state",
        ):
            with self.subTest(location=location):
                script = smt.Script()
                choice = script.fresh_constant("choice", smt.INT)
                script.register_quantified_choice(choice, 2)
                family = dependent_family(location, choice)
                with self.assertRaisesRegex(
                    RelationError,
                    "without carrying",
                ):
                    compare_families(
                        family,
                        family,
                        ScalarEncoder(script),
                    )

    def test_comparison_rejects_a_carried_choice_bound_mismatch(self):
        script = smt.Script()
        choice = script.fresh_constant("choice", smt.INT)
        script.register_quantified_choice(choice, 2)
        family = RelationFamily((
            Outcome(
                smt.TRUE,
                Relation((), ()),
                smt.FALSE,
                choices=(BoundedChoice(choice, 3),),
            ),
        ))

        with self.assertRaisesRegex(smt.SmtError, "inconsistent bounds"):
            compare_families(
                family,
                single(Relation((), ())),
                ScalarEncoder(script),
            )

    def test_comparison_rejects_shared_choice_symbols_across_sides(self):
        script = smt.Script()
        choice = script.fresh_constant("choice", smt.INT)
        family = RelationFamily((
            Outcome(
                smt.TRUE,
                Relation((), ()),
                smt.FALSE,
                choices=(BoundedChoice(choice, 2),),
            ),
        ))

        with self.assertRaisesRegex(RelationError, "scopes must be disjoint"):
            compare_families(family, family, ScalarEncoder(script))

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
            smt.Script(),
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
                _ground(outcome.error, grounded)
                for outcome, grounded in _enabled_outcomes(family, constants)
            }
            with self.subTest(count=count, offset=offset, present=present):
                self.assertEqual(enabled_errors, {expected_error})

    def test_checked_limit_compression_preserves_exact_observations(self):
        columns = (Column("a.value", "Int64", False),)
        present_terms = tuple(
            smt.symbol(f"present_{index}", smt.BOOL)
            for index in range(4)
        )
        rows = tuple(
            Row(
                present,
                {
                    "a.value": Value(
                        "Int64",
                        smt.FALSE,
                        smt.int_value(10 + index),
                    )
                },
            )
            for index, present in enumerate(present_terms)
        )
        inherited_error = smt.symbol("inherited_error", smt.BOOL)
        source_enabled = smt.symbol("source_enabled", smt.BOOL)
        source_choice = BoundedChoice(
            smt.symbol("source_choice", smt.INT),
            2,
        )

        def literal(value):
            return Expr(
                kind="literal",
                value=value,
                result_type="Uint64",
                nullable=False,
            )

        for ordered, count, offset in product(
            (False, True),
            range(6),
            range(6),
        ):
            source = RelationFamily((
                Outcome(
                    source_enabled,
                    Relation(columns, rows, sequence=ordered),
                    inherited_error,
                    (("upstream", 7),),
                    (source_choice,),
                ),
            ))
            family = limit_family(
                source,
                literal(count),
                None if offset == 0 else literal(offset),
                smt.Script(),
                "checked",
                ensure_at_most_one=True,
            )
            self.assertTrue(all(
                source_choice in outcome.choices
                for outcome in family.outcomes
            ))
            self.assertTrue(
                all(
                    dict(outcome.decisions).get("upstream") == 7
                    for outcome in family.outcomes
                )
            )

            for present, source_error in product(
                product((False, True), repeat=4),
                (False, True),
            ):
                constants = {
                    term.atom: active
                    for term, active in zip(present_terms, present)
                }
                constants.update(
                    {
                        source_enabled.atom: True,
                        inherited_error.atom: source_error,
                        source_choice.term.atom: 0,
                    }
                )
                enabled = _enabled_outcomes(family, constants)
                retained = min(
                    count,
                    max(sum(present) - offset, 0),
                )
                context = (ordered, count, offset, present, source_error)
                self.assertTrue(enabled, context)
                expected_error = source_error or retained > 1
                self.assertTrue(
                    all(
                        _ground(outcome.error, grounded) == expected_error
                        for outcome, grounded in enabled
                    ),
                    context,
                )
                if expected_error:
                    continue
                if ordered:
                    active_values = tuple(
                        10 + index
                        for index, active in enumerate(present)
                        if active
                    )
                    expected = {
                        _bag(active_values[offset : offset + count])
                    }
                else:
                    expected = _reference_limit(
                        present,
                        count,
                        offset,
                    )
                self.assertEqual(_bags(family, constants), expected, context)

    def test_checked_limit_quotients_only_the_cardinality_error_region(self):
        checked_raw = snapshot(
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
        checked = parse_snapshot(checked_raw)
        script = smt.Script()
        family = RelationEvaluator(
            checked,
            Database(checked, 23, script),
            ScalarEncoder(script),
        ).root()

        self.assertEqual(len(family.outcomes), 1)
        build_logical_kernel_problem_for_tests(checked, checked, 23)

        unchecked_raw = copy.deepcopy(checked_raw)
        unchecked_raw["plan"]["nodes"][1]["ensure_at_most_one"] = False
        unchecked = parse_snapshot(unchecked_raw)
        with self.assertRaisesRegex(VerificationError, "alternative audit bound"):
            build_logical_kernel_problem_for_tests(unchecked, unchecked, 23)

    def test_checked_limit_with_offset_quotients_the_high_cardinality_region(self):
        checked_raw = snapshot(
            [
                scan(),
                limit(
                    "limit",
                    "scan",
                    3,
                    7,
                    ensure_at_most_one=True,
                ),
            ],
            "limit",
        )
        checked = parse_snapshot(checked_raw)
        script = smt.Script()
        family = RelationEvaluator(
            checked,
            Database(checked, 23, script),
            ScalarEncoder(script),
        ).root()

        error_outcomes = tuple(
            outcome
            for outcome in family.outcomes
            if outcome.error == smt.TRUE
        )
        self.assertEqual(len(error_outcomes), 1)
        self.assertEqual(len(family.outcomes), 25)
        build_logical_kernel_problem_for_tests(checked, checked, 23)

        unchecked_raw = copy.deepcopy(checked_raw)
        unchecked_raw["plan"]["nodes"][1]["ensure_at_most_one"] = False
        unchecked = parse_snapshot(unchecked_raw)
        with self.assertRaisesRegex(VerificationError, "alternative audit bound"):
            build_logical_kernel_problem_for_tests(unchecked, unchecked, 23)

    def test_repeated_limit_counts_only_live_candidate_slots(self):
        parsed = parse_snapshot(
            snapshot(
                [
                    scan(),
                    limit("first", "scan", 1),
                    limit("second", "first", 1),
                ],
                "second",
            )
        )
        script = smt.Script()
        family = RelationEvaluator(
            parsed,
            Database(parsed, 16, script),
            ScalarEncoder(script),
        ).root()

        self.assertEqual(len(family.outcomes), 1)
        self.assertEqual(
            {len(outcome.relation.rows) for outcome in family.outcomes},
            {1},
        )
        self.assertTrue(
            all(
                not outcome.decisions and len(outcome.choices) == 1
                for outcome in family.outcomes
            )
        )
        build_logical_kernel_problem_for_tests(parsed, parsed, 16)

    def test_limit_padding_does_not_consume_sort_or_merge_choices(self):
        parsed = parse_snapshot(
            snapshot(
                [scan(), limit("first", "scan", 1)],
                "first",
            )
        )
        script = smt.Script()
        limited = RelationEvaluator(
            parsed,
            Database(parsed, 16, script),
            ScalarEncoder(script),
        ).root()

        def inflate(relation):
            rows = tuple(
                Row(
                    row.present,
                    row.values,
                    row.occurrence,
                    row.partition_facts,
                )
                for row in relation.rows
                for _ in range(8)
            )
            rows += tuple(
                Row(smt.FALSE, relation.rows[0].values)
                for _ in range(8)
            )
            return Relation(relation.columns, rows)

        padded = map_family(limited, inflate)
        order = (SortOrder("a.value", True, True),)
        sorted_family = sort_family(padded, order, script, "sort")
        merged = merge_family(
            sorted_family,
            order,
            (tuple(range(8)), tuple(range(8, 16))),
            script,
            "merge",
        )

        self.assertEqual(
            {len(outcome.relation.rows) for outcome in sorted_family.outcomes},
            {16},
        )
        self.assertEqual(
            {len(outcome.choices) for outcome in sorted_family.outcomes},
            {9},
        )
        self.assertEqual(
            {len(outcome.choices) for outcome in merged.outcomes},
            {17},
        )
        for outcome in sorted_family.outcomes:
            live = {
                index
                for index, row in enumerate(outcome.relation.rows)
                if row.present != smt.FALSE
            }
            self.assertEqual(
                {
                    index
                    for index, ordinal in enumerate(outcome.relation.ordinals)
                    if ordinal != smt.ZERO
                },
                live,
            )

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

    def test_symbolic_singleton_preserves_metadata_and_hidden_decimal_state(self):
        script = smt.Script()
        present = (
            smt.symbol("left_present", smt.BOOL),
            smt.symbol("right_present", smt.BOOL),
        )
        common_route = smt.symbol("common_route", smt.BOOL)
        split_route = smt.symbol("split_route", smt.BOOL)
        common_fact = PartitionFact(common_route, True)
        states = (
            DecimalAverageState(
                "Decimal(35,2)",
                smt.int_value(110),
                smt.int_value(2),
                110,
                2,
            ),
            DecimalAverageState(
                "Decimal(35,2)",
                smt.int_value(270),
                smt.int_value(3),
                270,
                3,
            ),
        )
        rows = tuple(
            Row(
                guard,
                {
                    "a.value": Value(
                        "Decimal(35,2)",
                        smt.FALSE,
                        smt.int_value(value),
                        bound,
                        state,
                    )
                },
                partition_facts=frozenset((
                    common_fact,
                    PartitionFact(split_route, index == 1),
                )),
            )
            for index, (guard, value, bound, state) in enumerate(zip(
                present,
                (100, 250),
                (100, 250),
                states,
            ))
        )
        source_error = smt.symbol("source_error", smt.BOOL)
        source_enabled = smt.symbol("source_enabled", smt.BOOL)
        upstream = script.fresh_constant("upstream", smt.INT)
        script.register_quantified_choice(upstream, 2)
        upstream_choice = BoundedChoice(upstream, 2)
        family = limit_family(
            RelationFamily((
                Outcome(
                    source_enabled,
                    Relation(
                        (Column("a.value", "Decimal(35,2)", False),),
                        rows,
                    ),
                    source_error,
                    (("upstream", 7),),
                    (upstream_choice,),
                ),
            )),
            Expr(
                kind="literal",
                value=1,
                result_type="Uint64",
                nullable=False,
            ),
            None,
            script,
            "take",
        )

        self.assertEqual(len(family.outcomes), 1)
        outcome = family.outcomes[0]
        self.assertEqual(len(outcome.relation.rows), 1)
        self.assertEqual(len(outcome.choices), 2)
        self.assertIn(upstream_choice, outcome.choices)
        selector = next(
            choice
            for choice in outcome.choices
            if choice != upstream_choice
        )
        self.assertEqual(selector.bound, 2)
        self.assertIs(outcome.error, source_error)
        self.assertEqual(outcome.decisions, (("upstream", 7),))
        selected_row = outcome.relation.rows[0]
        self.assertIsNone(selected_row.occurrence)
        self.assertEqual(selected_row.partition_facts, frozenset((common_fact,)))
        selected_value = selected_row.values["a.value"]
        self.assertEqual(selected_value.decimal_finite_abs_bound, 250)
        self.assertIsNotNone(selected_value.decimal_average_state)
        selected_state = selected_value.decimal_average_state
        assert selected_state is not None
        self.assertEqual(selected_state.finite_abs_bound, 270)
        self.assertEqual(selected_state.count_bound, 3)

        for selected, value, state_sum, state_count in (
            (0, 100, 110, 2),
            (1, 250, 270, 3),
        ):
            constants = {
                present[0].atom: True,
                present[1].atom: True,
                source_enabled.atom: True,
                source_error.atom: False,
                upstream.atom: 0,
                selector.term.atom: selected,
            }
            self.assertTrue(_ground(outcome.enabled, constants))
            self.assertTrue(_ground(selected_row.present, constants))
            self.assertEqual(_ground(selected_value.value, constants), value)
            self.assertEqual(_ground(selected_state.sum, constants), state_sum)
            self.assertEqual(_ground(selected_state.count, constants), state_count)

        only_left = {
            present[0].atom: True,
            present[1].atom: False,
            source_enabled.atom: True,
            source_error.atom: False,
            upstream.atom: 0,
        }
        enabled = _enabled_outcomes(family, only_left)
        self.assertEqual(
            {grounded[selector.term.atom] for _, grounded in enabled},
            {0},
        )
        for outside in (-1, selector.bound):
            constants = {
                present[0].atom: True,
                present[1].atom: True,
                source_enabled.atom: True,
                source_error.atom: False,
                upstream.atom: 0,
                selector.term.atom: outside,
            }
            self.assertFalse(_ground(outcome.enabled, constants))

    def test_symbolic_singleton_rejects_mixed_decimal_average_state(self):
        script = smt.Script()
        state = DecimalAverageState(
            "Decimal(35,2)",
            smt.ONE,
            smt.ONE,
            1,
            1,
        )
        rows = (
            Row(
                smt.symbol("left_present", smt.BOOL),
                {
                    "a.value": Value(
                        "Decimal(35,2)",
                        smt.FALSE,
                        smt.ONE,
                        1,
                        state,
                    )
                },
            ),
            Row(
                smt.symbol("right_present", smt.BOOL),
                {
                    "a.value": Value(
                        "Decimal(35,2)",
                        smt.FALSE,
                        smt.int_value(2),
                        2,
                    )
                },
            ),
        )
        with self.assertRaisesRegex(
            RelationError,
            "mixed Decimal avg state and scalar values",
        ):
            limit_family(
                single(
                    Relation(
                        (Column("a.value", "Decimal(35,2)", False),),
                        rows,
                    )
                ),
                Expr(
                    kind="literal",
                    value=1,
                    result_type="Uint64",
                    nullable=False,
                ),
                None,
                script,
                "take",
            )

    def test_limit_at_candidate_bound_is_exact_noop_with_dead_padding(self):
        left = smt.symbol("left_present", smt.BOOL)
        right = smt.symbol("right_present", smt.BOOL)
        columns = (Column("a.value", "Int64", False),)
        live_rows = (
            Row(
                left,
                {"a.value": Value("Int64", smt.FALSE, smt.int_value(10))},
            ),
            Row(
                right,
                {"a.value": Value("Int64", smt.FALSE, smt.int_value(11))},
            ),
        )
        dead = tuple(
            Row(
                smt.FALSE,
                {"a.value": Value("Int64", smt.FALSE, smt.int_value(index))},
            )
            for index in range(300)
        )
        source = single(
            Relation(columns, (live_rows[0], *dead, live_rows[1]))
        )
        family = limit_family(
            source,
            Expr(
                kind="literal",
                value=2,
                result_type="Uint64",
                nullable=False,
            ),
            None,
            smt.Script(),
            "take",
        )

        self.assertIs(family, source)
        self.assertEqual(len(family.outcomes), 1)
        self.assertEqual(
            {len(outcome.relation.rows) for outcome in family.outcomes},
            {302},
        )
        for present in product((False, True), repeat=2):
            constants = {
                left.atom: present[0],
                right.atom: present[1],
            }
            self.assertEqual(
                _bags(family, constants),
                _reference_limit(present, 2, 0),
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
        self.assertEqual(len(family.outcomes), 1)
        self.assertEqual(len(family.outcomes[0].choices), 1)
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
                    _satisfiable(
                        smt.and_(*problem.script.assertions),
                        constants,
                    )
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
        self.assertFalse(
            _holds_for_all_choices(
                family_equal(unordered, first_only, scalar),
                constants,
            )
        )

    def test_phase_does_not_change_runtime_semantics(self):
        problem = build_logical_kernel_problem_for_tests(
            logical_limit(1, phase="undefined"),
            logical_limit(1, phase="final"),
            2,
        )
        for present in product((False, True), repeat=2):
            constants = _constants_from_witness(problem.witness, present)
            self.assertFalse(
                _satisfiable(
                    smt.and_(*problem.script.assertions),
                    constants,
                )
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
        self.assertFalse(
            _holds_for_all_choices(
                equality,
                _constants(database, (True, True)),
            )
        )

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
            self.assertTrue(
                _holds_for_all_choices(
                    equality,
                    _constants(database, present),
                ),
                present,
            )

    def test_alternative_cap_fails_closed(self):
        parsed = logical_limit(8)
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
            return _holds_for_all_choices(equality, constants)

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
