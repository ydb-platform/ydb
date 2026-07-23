import copy
import unittest
from itertools import permutations, product
from unittest.mock import patch

from ydb.core.kqp.opt.rbo.verification.rbo_verifier import decimal, relation, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import (
    Column,
    SnapshotError,
    SortOrder,
    parse_snapshot,
    stage_task_counts,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.relation import (
    Database,
    Evaluator as RelationEvaluator,
    Outcome,
    Relation,
    RelationError,
    RelationFamily,
    Row,
    compare_families,
    family_equal,
    merge_family,
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
    decode_witness,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.types import DATE, MAX_DATE


ABSENT = object()
UINT64_MAX = (1 << 64) - 1
REFERENCE_DECIMAL_INF = 100_000_000_000_000_000_000_000_000_000_000_000
REFERENCE_DECIMAL_NAN = REFERENCE_DECIMAL_INF + 1
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
        "ordered": False,
    }


def snapshot(nodes, root, stage_graph=None, key1_type="Int64"):
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "k1", "type": key1_type, "nullable": True},
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
            "subplans": [],
        },
        "stage_graph": stage_graph,
    }


def logical_sort(order, top_limit=None, phase="undefined", key1_type="Int64"):
    return parse_snapshot(
        snapshot(
            [scan(), sort_node("sort", "scan", order, top_limit, phase)],
            "sort",
            key1_type=key1_type,
        )
    )


def logical_ordered_limit(
    order,
    count,
    offset=None,
    sort_limit=None,
    sort_phase="undefined",
    limit_phase="undefined",
    key1_type="Int64",
):
    return parse_snapshot(
        snapshot(
            [
                scan(),
                sort_node("sort", "scan", order, sort_limit, sort_phase),
                limit_node("limit", "sort", count, offset, limit_phase),
            ],
            "limit",
            key1_type=key1_type,
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
    key1_type="Int64",
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
    return parse_snapshot(snapshot(nodes, "final", graph, key1_type=key1_type))


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
    if term.operation in {"forall", "exists"}:
        variables = term.arguments[:-1]
        body = term.arguments[-1]
        domains = tuple(_quantifier_domain(variable, body) for variable in variables)
        values = (
            _ground(body, constants | dict(zip((v.atom for v in variables), assignment)))
            for assignment in product(*domains)
        )
        return all(values) if term.operation == "forall" else any(values)
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
            constants | dict(zip((symbol.atom for symbol in free), assignment)),
        )
        for assignment in product(*domains)
    )


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
        domains = tuple(range(choice.bound) for choice in outcome.choices)
        for assignment in product(*domains):
            grounded = constants | {
                choice.term.atom: value
                for choice, value in zip(outcome.choices, assignment)
            }
            if not _ground(outcome.enabled, grounded):
                continue
            relation = outcome.relation
            indices = [
                index
                for index, row in enumerate(relation.rows)
                if _ground(row.present, grounded)
            ]
            if relation.ordinals is not None:
                indices.sort(
                    key=lambda index: _ground(relation.ordinals[index], grounded)
                )
            names = tuple(column.name for column in relation.columns)
            result.add(tuple(
                tuple(_cell(relation.rows[index].values[name], grounded) for name in names)
                for index in indices
            ))
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


def _decimal_order_key(value, precision):
    bound = 10**precision
    if value == -REFERENCE_DECIMAL_INF:
        return (0, 0)
    if -bound < value < bound:
        return (1, value)
    if value == REFERENCE_DECIMAL_INF:
        return (2, 0)
    if value == REFERENCE_DECIMAL_NAN:
        return (3, 0)
    raise AssertionError(f"illegal Decimal({precision},0) code {value}")


def _compare_decimal_cells(left, right, ascending, nulls_first, precision):
    if left is None or right is None:
        return _compare_cells(left, right, ascending, nulls_first)
    left_key = _decimal_order_key(left, precision)
    right_key = _decimal_order_key(right, precision)
    if left_key == right_key:
        return 0
    before = left_key < right_key if ascending else left_key > right_key
    return -1 if before else 1


def _decimal_reference_sequences(rows, order, precision):
    present = tuple(row for row in rows if row is not ABSENT)

    def compare_rows(left, right):
        for item in order:
            index = COLUMN_INDEX[item["column"]]
            comparison = (
                _compare_decimal_cells(
                    left[index],
                    right[index],
                    item["ascending"],
                    item["nulls_first"],
                    precision,
                )
                if index == COLUMN_INDEX["a.k1"]
                else _compare_cells(
                    left[index],
                    right[index],
                    item["ascending"],
                    item["nulls_first"],
                )
            )
            if comparison:
                return comparison
        return 0

    return {
        permutation
        for permutation in permutations(present)
        if all(
            compare_rows(permutation[index], permutation[index + 1]) <= 0
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
    return _satisfiable(smt.and_(*problem.script.assertions), constants)


class DateDomainTest(unittest.TestCase):
    def test_symbolic_source_days_have_the_exact_ydb_domain_and_integer_witness(self):
        parsed = logical_sort(
            [order_item("a.k1")],
            key1_type=DATE,
        )
        script = smt.Script()
        database = Database(parsed, 1, script)
        date_cell = database.witness["A"][0].cells["k1"]
        self.assertIn(
            smt.and_(
                smt.not_(smt.lt(date_cell.value, smt.ZERO)),
                smt.lt(date_cell.value, smt.int_value(MAX_DATE)),
            ),
            script.assertions,
        )

        for day in (0, MAX_DATE - 1):
            constants = _witness_constants(
                database.witness,
                ((day, 0, 7),),
            )
            witness = decode_witness(database.witness, constants, {})
            self.assertEqual(witness["A"][0]["k1"], day)
            self.assertIs(type(witness["A"][0]["k1"]), int)


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

    def test_decimal_and_text_ordering_are_admitted(self):
        parsed = logical_sort(
            [order_item("a.k1")],
            key1_type="Decimal(5,2)",
        )
        self.assertEqual(parsed.tables[0].columns[0].type, "Decimal(5,2)")

        for scalar_type in ("String", "Utf8"):
            value = snapshot(
                [scan(), sort_node("sort", "scan", [order_item("a.k1")])],
                "sort",
            )
            value["schema"]["tables"][0]["columns"][0]["type"] = scalar_type
            with self.subTest(scalar_type=scalar_type):
                parsed = parse_snapshot(value)
                self.assertEqual(parsed.tables[0].columns[0].type, scalar_type)

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
    def test_date_ordering_matches_reference_at_bounds_and_with_nulls(self):
        for ascending, nulls_first in product((False, True), repeat=2):
            order = [order_item("a.k1", ascending, nulls_first)]
            parsed = logical_sort(order, key1_type=DATE)
            database, family = _logical_family(parsed, 3)
            slot_states = tuple(
                (ABSENT,)
                + tuple(
                    (key, 0, slot)
                    for key in (None, 0, MAX_DATE - 1)
                )
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

    def test_default_symbolic_sort_matches_four_row_ties(self):
        order = [order_item("a.k1")]
        parsed = logical_sort(order)
        database, family = _logical_family(parsed, 4)
        rows = (
            (0, 0, 0),
            (0, 1, 1),
            (1, 0, 2),
            (1, 1, 3),
        )

        self.assertEqual(len(family.outcomes), 1)
        self.assertEqual(
            _sequences(family, _database_constants(database, rows)),
            _reference_sequences(rows, order),
        )

    def test_symbolic_sort_and_top_sort_match_tiny_reference(self):
        with patch.object(relation, "MAX_OUTCOME_ALTERNATIVES", 1):
            for ascending, nulls_first, top_limit in product(
                (False, True),
                (False, True),
                (None, 1),
            ):
                order = [order_item("a.k1", ascending, nulls_first)]
                parsed = logical_sort(order, top_limit)
                database, family = _logical_family(parsed, 2)
                slot_states = tuple(
                    (ABSENT,) + tuple((key, 0, slot) for key in (None, -1, 1))
                    for slot in range(2)
                )
                for rows in product(*slot_states):
                    expected = _reference_sequences(rows, order)
                    if top_limit is not None:
                        expected = {sequence[:top_limit] for sequence in expected}
                    self.assertEqual(
                        _sequences(family, _database_constants(database, rows)),
                        expected,
                        (ascending, nulls_first, top_limit, rows),
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


class DecimalSortConcreteDifferentialTest(unittest.TestCase):
    def test_total_code_order_matches_independent_reference_exhaustively(self):
        self.assertEqual(decimal.INF, REFERENCE_DECIMAL_INF)
        self.assertEqual(decimal.NAN, REFERENCE_DECIMAL_NAN)

        for precision in (1, 2):
            bound = 10**precision
            codes = tuple(range(-bound + 1, bound)) + (
                -REFERENCE_DECIMAL_INF,
                REFERENCE_DECIMAL_INF,
                REFERENCE_DECIMAL_NAN,
            )
            for left, right in product(codes, repeat=2):
                expected = _decimal_order_key(
                    left,
                    precision,
                ) < _decimal_order_key(right, precision)
                actual = _ground(
                    decimal.sort_less(
                        smt.int_value(left),
                        smt.int_value(right),
                    ),
                    {},
                )
                self.assertEqual(actual, expected, (precision, left, right))

    def test_sort_matches_special_null_and_tie_reference(self):
        keys = (None, -decimal.INF, -1, 0, 1, decimal.INF, decimal.NAN)
        for ascending, nulls_first in product((False, True), repeat=2):
            order = [order_item("a.k1", ascending, nulls_first)]
            parsed = logical_sort(order, key1_type="Decimal(2,0)")
            database, family = _logical_family(parsed, 2)
            slot_states = tuple(
                (ABSENT,) + tuple((key, 0, slot) for key in keys)
                for slot in range(2)
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
                        _decimal_reference_sequences(rows, order, 2),
                    )

    def test_nan_ties_allow_both_orders_and_next_key_breaks_ties(self):
        rows = (
            (decimal.NAN, 1, 10),
            (decimal.NAN, 0, 11),
        )
        one_key = [order_item("a.k1")]
        parsed = logical_sort(one_key, key1_type="Decimal(2,0)")
        database, family = _logical_family(parsed, 2)
        constants = _database_constants(database, rows)
        self.assertEqual(
            _sequences(family, constants),
            _decimal_reference_sequences(rows, one_key, 2),
        )
        self.assertEqual(len(_sequences(family, constants)), 2)

        two_keys = [order_item("a.k1"), order_item("a.k2")]
        parsed = logical_sort(two_keys, key1_type="Decimal(2,0)")
        database, family = _logical_family(parsed, 2)
        constants = _database_constants(database, rows)
        self.assertEqual(
            _sequences(family, constants),
            _decimal_reference_sequences(rows, two_keys, 2),
        )
        self.assertEqual(len(_sequences(family, constants)), 1)


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
        script = smt.Script()
        top_database = Database(top, 3, script)
        scalar = ScalarEncoder(script)
        top_family = RelationEvaluator(top, top_database, scalar).root()
        explicit_family = RelationEvaluator(
            explicit,
            top_database,
            scalar,
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
                comparison = compare_families(
                    relation(values, True),
                    relation(tuple(reversed(values)), False),
                    scalar,
                )
                self.assertTrue(
                    _sequences(comparison.left, {})
                    == _sequences(comparison.right, {})
                )
        comparison = compare_families(
            relation((1, 2), True),
            relation((2, 1), False),
            scalar,
        )
        self.assertNotEqual(
            _sequences(comparison.left, {}),
            _sequences(comparison.right, {}),
        )

    def test_missing_four_row_order_uses_the_symbolic_sequence_language(self):
        columns = (Column("value", "Int64", False),)
        rows = tuple(
            Row(
                smt.TRUE,
                {"value": Value("Int64", smt.FALSE, smt.int_value(value))},
            )
            for value in range(4)
        )
        script = smt.Script()
        comparison = compare_families(
            single(Relation(columns, rows, sequence=True)),
            single(Relation(columns, rows)),
            ScalarEncoder(script),
        )

        self.assertEqual(len(comparison.right.outcomes), 1)
        outcome = comparison.right.outcomes[0]
        self.assertEqual(len(outcome.choices), 4)
        self.assertEqual(len(outcome.relation.ordinals), 4)
        self.assertEqual(
            _sequences(comparison.right, {}),
            set(permutations(((0,), (1,), (2,), (3,)))),
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

    def test_refining_a_tie_with_an_extra_key_changes_the_sequence_language(self):
        coarse = logical_sort([order_item("a.k1")])
        refined = logical_sort([
            order_item("a.k1"),
            order_item("a.k2"),
        ])
        rows = ((0, 1, 10), (0, 0, 11))
        problem = build_logical_kernel_problem_for_tests(coarse, refined, 2)
        self.assertTrue(_counterexample_formula_holds(problem, rows))

    def test_symbolic_family_equality_proves_self_and_finds_direction_mutation(self):
        ascending = [order_item("a.k1", True, False)]
        descending = [order_item("a.k1", False, False)]
        rows = ((0, 0, 0), (1, 0, 1))
        with patch.object(relation, "MAX_OUTCOME_ALTERNATIVES", 1):
            same = build_logical_kernel_problem_for_tests(
                logical_sort(ascending),
                logical_sort(ascending),
                2,
            )
            changed = build_logical_kernel_problem_for_tests(
                logical_sort(ascending),
                logical_sort(descending),
                2,
            )
        self.assertFalse(_counterexample_formula_holds(same, rows))
        self.assertTrue(_counterexample_formula_holds(changed, rows))

    def test_shared_renderer_keeps_large_symbolic_sort_formula_bounded(self):
        parsed = logical_sort([order_item("a.k1")])
        formula = build_logical_kernel_problem_for_tests(parsed, parsed, 24).formula()

        self.assertLess(len(formula), 2_000_000)
        self.assertIn("(let ", formula)
        self.assertIn("(exists ", formula)

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

    def test_sort_enumerates_through_three_rows_then_uses_ordinals(self):
        parsed = logical_sort([order_item("a.k1")])
        for row_bound, outcome_count in ((3, 6), (4, 1), (48, 1)):
            with self.subTest(row_bound=row_bound):
                script = smt.Script()
                database = Database(parsed, row_bound, script)
                family = RelationEvaluator(
                    parsed,
                    database,
                    ScalarEncoder(script),
                ).root()
                self.assertEqual(len(family.outcomes), outcome_count)
                if row_bound == 3:
                    self.assertTrue(all(
                        not outcome.choices
                        and outcome.relation.ordinals is None
                        for outcome in family.outcomes
                    ))
                else:
                    self.assertEqual(
                        len(family.outcomes[0].choices),
                        row_bound,
                    )
                    self.assertEqual(
                        len(family.outcomes[0].relation.ordinals),
                        row_bound,
                    )

    def test_sort_and_latent_sequence_pair_bounds_precede_large_allocations(self):
        parsed = logical_sort([order_item("a.k1")])
        with (
            patch.object(relation, "MAX_RELATION_ROW_PAIRS", 5),
            patch.object(
                relation,
                "factorial",
                side_effect=AssertionError("factorial must not run"),
            ),
            patch.object(
                relation,
                "_fresh_ordinals",
                side_effect=AssertionError("ordinals must not be allocated"),
            ),
        ):
            with self.assertRaisesRegex(
                VerificationError,
                "sort construction requires 6 candidate-row pairs.*5 pair construction",
            ):
                build_logical_kernel_problem_for_tests(parsed, parsed, 4)

        unordered_snapshot = parse_snapshot(snapshot([scan()], "scan"))
        script = smt.Script()
        database = Database(unordered_snapshot, 4, script)
        scalar = ScalarEncoder(script)
        unordered = RelationEvaluator(
            unordered_snapshot,
            database,
            scalar,
        ).root()
        source = unordered.certain()
        ordered = single(
            Relation(
                source.columns,
                source.rows,
                sequence=True,
            )
        )
        with (
            patch.object(relation, "MAX_RELATION_ROW_PAIRS", 5),
            patch.object(
                relation,
                "factorial",
                side_effect=AssertionError("factorial must not run"),
            ),
            patch.object(
                relation,
                "_fresh_ordinals",
                side_effect=AssertionError("ordinals must not be allocated"),
            ),
        ):
            with self.assertRaisesRegex(
                RelationError,
                "latent sequence construction requires 6 candidate-row pairs.*5 pair construction",
            ):
                family_equal(ordered, unordered, scalar)


class MergeEncodingTest(unittest.TestCase):
    COLUMNS = (
        Column("k", "Int64", False),
        Column("payload", "Int64", False),
    )
    ORDER = (SortOrder("k", True, False),)

    @staticmethod
    def _row(payload):
        return Row(
            smt.TRUE,
            {
                "k": Value("Int64", smt.FALSE, smt.ZERO),
                "payload": Value("Int64", smt.FALSE, smt.int_value(payload)),
            },
        )

    def test_enumerated_merge_observes_reversed_concrete_input_ordinals(self):
        source = RelationFamily((
            Outcome(
                smt.TRUE,
                Relation(
                    self.COLUMNS,
                    (self._row(10), self._row(20)),
                    sequence=True,
                    order=self.ORDER,
                    ordinals=(smt.ONE, smt.ZERO),
                ),
            ),
        ))

        merged = merge_family(source, self.ORDER, ((0, 1),), smt.Script(), "merge")

        self.assertEqual(
            _sequences(merged, {}),
            {((0, 20), (0, 10))},
        )

    def test_symbolic_merge_preserves_each_producer_tie_order(self):
        source = single(
            Relation(
                self.COLUMNS,
                (self._row(10), self._row(20), self._row(30)),
                sequence=True,
                order=self.ORDER,
            )
        )
        with patch.object(relation, "MAX_OUTCOME_ALTERNATIVES", 1):
            merged = merge_family(
                source,
                self.ORDER,
                ((0, 1), (2,)),
                smt.Script(),
                "merge",
            )

        self.assertEqual(len(merged.outcomes[0].choices), 3)
        self.assertEqual(
            _sequences(merged, {}),
            {
                ((0, 10), (0, 20), (0, 30)),
                ((0, 10), (0, 30), (0, 20)),
                ((0, 30), (0, 10), (0, 20)),
            },
        )

    def test_merge_pair_bound_precedes_factorial_and_ordinal_allocation(self):
        source = single(
            Relation(
                self.COLUMNS,
                tuple(self._row(value) for value in range(4)),
                sequence=True,
                order=self.ORDER,
            )
        )
        with (
            patch.object(relation, "MAX_RELATION_ROW_PAIRS", 5),
            patch.object(
                relation,
                "factorial",
                side_effect=AssertionError("factorial must not run"),
            ),
            patch.object(
                relation,
                "_fresh_ordinals",
                side_effect=AssertionError("ordinals must not be allocated"),
            ),
        ):
            with self.assertRaisesRegex(
                RelationError,
                "merge construction requires 6 candidate-row pairs.*5 pair construction",
            ):
                merge_family(
                    source,
                    self.ORDER,
                    ((0, 1), (2, 3)),
                    smt.Script(),
                    "merge",
                )

        symbolic = single(
            Relation(
                self.COLUMNS,
                tuple(self._row(value) for value in range(3)),
                sequence=True,
                order=self.ORDER,
            )
        )
        with (
            patch.object(relation, "MAX_OUTCOME_ALTERNATIVES", 0),
            patch.object(relation, "MAX_RELATION_ROW_PAIRS", 5),
            patch.object(
                relation,
                "_fresh_ordinals",
                side_effect=AssertionError("ordinals must not be allocated"),
            ),
        ):
            with self.assertRaisesRegex(
                RelationError,
                "merge ordinal construction requires 9 candidate-row pairs.*5 pair construction",
            ):
                merge_family(
                    symbolic,
                    self.ORDER,
                    ((0, 1, 2),),
                    smt.Script(),
                    "merge",
                )


class StageTopSortMergeTest(unittest.TestCase):
    ORDER = [order_item("a.k1", True, False)]

    @staticmethod
    def _families(staged, key1_type="Int64"):
        logical = logical_ordered_limit(
            StageTopSortMergeTest.ORDER,
            count=1,
            offset=1,
            key1_type=key1_type,
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
        return database, router, logical_family, staged_family

    def test_two_task_local_top_sort_merge_and_final_limit_are_equivalent(self):
        staged = staged_top_sort_merge(self.ORDER)
        self.assertEqual(stage_task_counts(staged), {"source": 2, "root": 1})
        database, router, logical_family, staged_family = self._families(staged)
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
                    _sequences(logical_family, constants),
                    _sequences(staged_family, constants),
                )

    def test_date_local_sort_and_merge_preserve_bounds_nulls_and_direction(self):
        staged = staged_top_sort_merge(self.ORDER, key1_type=DATE)
        database, router, logical_family, staged_family = self._families(staged, DATE)
        rows_and_tasks = (
            (((None, 0, 0), (0, 0, 1)), (False, True)),
            (((0, 0, 0), (MAX_DATE - 1, 0, 1)), (False, False)),
            (((MAX_DATE - 1, 0, 0), (None, 0, 1)), (True, False)),
        )
        for rows, tasks in rows_and_tasks:
            with self.subTest(rows=rows, tasks=tasks):
                constants = _database_constants(database, rows, router, tasks)
                self.assertEqual(
                    _sequences(logical_family, constants),
                    _sequences(staged_family, constants),
                )

        descending = [order_item("a.k1", False, False)]
        mutated = staged_top_sort_merge(
            self.ORDER,
            partial_order=descending,
            merge_order=descending,
            key1_type=DATE,
        )
        database, router, logical_family, staged_family = self._families(mutated, DATE)
        constants = _database_constants(
            database,
            ((0, 0, 0), (MAX_DATE - 1, 0, 1)),
            router,
            (False, False),
        )
        self.assertNotEqual(
            _sequences(logical_family, constants),
            _sequences(staged_family, constants),
        )

    def test_string_local_top_sort_merge_is_equivalent_and_direction_is_observable(self):
        descending = [order_item("a.k1", False, False)]
        for scalar_type in ("String", "Utf8"):
            logical = logical_ordered_limit(
                self.ORDER,
                count=1,
                offset=1,
                key1_type=scalar_type,
            )
            staged = staged_top_sort_merge(self.ORDER, key1_type=scalar_type)
            problem = build_problem(logical, staged, 2)
            problem.formula()  # Seal the finite rank universe before grounding.
            for rows in (
                ((0, 0, 0), (1, 0, 1)),
                ((None, 0, 0), (1, 0, 1)),
                ((1, 0, 0), (1, 0, 1)),
            ):
                with self.subTest(scalar_type=scalar_type, rows=rows):
                    self.assertFalse(_counterexample_formula_holds(problem, rows))

            mutated = staged_top_sort_merge(
                self.ORDER,
                partial_order=descending,
                merge_order=descending,
                key1_type=scalar_type,
            )
            corrupted = build_problem(logical, mutated, 2)
            corrupted.formula()
            self.assertTrue(
                _counterexample_formula_holds(
                    corrupted,
                    ((0, 0, 0), (1, 0, 1)),
                )
            )

    def test_decimal_local_sort_and_merge_preserve_total_special_order(self):
        decimal_type = "Decimal(2,0)"
        keys = (
            None,
            -REFERENCE_DECIMAL_INF,
            -1,
            0,
            1,
            REFERENCE_DECIMAL_INF,
            REFERENCE_DECIMAL_NAN,
        )
        for ascending, nulls_first in product((False, True), repeat=2):
            order = [order_item("a.k1", ascending, nulls_first)]
            staged = staged_top_sort_merge(order, key1_type=decimal_type)
            script = smt.Script()
            database = Database(staged, 2, script)
            scalar = ScalarEncoder(script)
            router = Router(script)
            family = StageEvaluator(staged, database, scalar, router).root()
            for key_pair, tasks in product(
                product(keys, repeat=2),
                product((False, True), repeat=2),
            ):
                rows = tuple(
                    (key, 0, slot) for slot, key in enumerate(key_pair)
                )
                with self.subTest(
                    ascending=ascending,
                    nulls_first=nulls_first,
                    rows=rows,
                    tasks=tasks,
                ):
                    constants = _database_constants(database, rows, router, tasks)
                    expected = {
                        sequence[1:2]
                        for sequence in _decimal_reference_sequences(rows, order, 2)
                    }
                    self.assertEqual(_sequences(family, constants), expected)

        descending = [order_item("a.k1", False, False)]
        mutated = staged_top_sort_merge(
            self.ORDER,
            partial_order=descending,
            merge_order=descending,
            key1_type=decimal_type,
        )
        database, router, logical_family, staged_family = self._families(
            mutated,
            decimal_type,
        )
        constants = _database_constants(
            database,
            ((decimal.INF, 0, 0), (decimal.NAN, 0, 1)),
            router,
            (False, False),
        )
        self.assertNotEqual(
            _sequences(logical_family, constants),
            _sequences(staged_family, constants),
        )

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
                database, router, logical_family, staged_family = self._families(staged)
                constants = _database_constants(database, rows, router, tasks)
                self.assertNotEqual(
                    _sequences(logical_family, constants),
                    _sequences(staged_family, constants),
                )

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
