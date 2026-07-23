"""Independent concrete reference checks for StageGraph connections.

The production StageGraph evaluator exposes symbolic bags at connection and
root boundaries.  This test grounds those bags for every routing choice and
compares the resulting set with a small concrete two-task router below.  The
reference deliberately does not import stages.py or relation.py.
"""

import copy
import unittest
from collections import Counter
from dataclasses import dataclass
from itertools import product

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem


TASKS = 2
BUILTIN_TERMS = frozenset(
    {
        "symbol",
        "bool",
        "int",
        "not",
        "and",
        "or",
        "=",
        "<",
        "ite",
        "+",
        "-",
        "*",
        "mod",
    }
)


@dataclass(frozen=True)
class Cell:
    is_null: bool
    value: int


@dataclass(frozen=True)
class GroundRow:
    present: bool
    cells: tuple[tuple[str, Cell], ...]

    def cell(self, name):
        return dict(self.cells)[name]

    def visible(self, names):
        return tuple(
            None if self.cell(name).is_null else self.cell(name).value
            for name in names
        )


ONE_COLUMN_ROWS = tuple(
    GroundRow(present, (("k", Cell(is_null, value)),))
    for present, is_null, value in product(
        (False, True), (False, True), (0, 1)
    )
)


def _cell(value, hidden=0):
    return Cell(value is None, hidden if value is None else value)


def _row(k=None, x=None, *, present=True, null_payload=0):
    return GroundRow(
        present,
        (("k", _cell(k, null_payload)), ("x", _cell(x, null_payload))),
    )


def _absent_local(null_payload=0):
    return _row(None, None, present=False, null_payload=null_payload)


def _table(name, columns=("k",)):
    return {
        "name": name,
        "columns": [
            {"name": column, "type": "Int64", "nullable": True}
            for column in columns
        ],
        "unique_keys": [],
    }


def _scan(node_id, table, columns=("k",)):
    prefix = table.lower()
    return {
        "id": node_id,
        "op": "scan",
        "table": table,
        "columns": [
            {"source": column, "output": f"{prefix}.{column}"}
            for column in columns
        ],
        "pushed_limit": None,
    }


def _stage(stage_id, nodes, inputs, outputs, source_storage=None):
    return {
        "id": stage_id,
        "nodes": list(nodes),
        "inputs": list(inputs),
        "outputs": [
            {"index": index, "node": node}
            for index, node in enumerate(outputs)
        ],
        "source_storage": source_storage,
    }


def _edge(edge_id, producer, consumer, consumer_input, connection):
    return {
        "id": edge_id,
        "producer": producer,
        "consumer": consumer,
        "occurrence": 0,
        "producer_output": 0,
        "consumer_input": consumer_input,
        **connection,
    }


def _snapshot(tables, nodes, root, output, stages=None, edges=None):
    return parse_snapshot(
        {
            "format": "ydb-rbo-semantic-snapshot",
            "version": 1,
            "schema": {"tables": copy.deepcopy(tables)},
            "plan": {
                "nodes": copy.deepcopy(nodes),
                "root": root,
                "output": list(output),
                "subplans": [],
            },
            "stage_graph": (
                None
                if stages is None
                else {
                    "root_stage": stages[-1]["id"],
                    "stages": copy.deepcopy(stages),
                    "edges": copy.deepcopy(edges),
                    "assumptions": [],
                }
            ),
        }
    )


def _connection_pair(connection):
    table = _table("A")
    scan = _scan("a", "A")
    project = {
        "id": "project",
        "op": "project",
        "input": "a",
        "ordered": False,
        "columns": [
            {
                "output": "result",
                "expression": {"kind": "column", "column": "a.k"},
            }
        ],
    }
    nodes = [scan, project]
    before = _snapshot([table], nodes, "project", ["result"])
    after = _snapshot(
        [table],
        nodes,
        "project",
        ["result"],
        [
            _stage("source", ["a"], [], ["a"], "row"),
            _stage("consumer", ["project"], ["a"], ["project"]),
        ],
        [_edge("tested", "source", "consumer", 0, connection)],
    )
    return before, after


def _join_pair(left_connection, right_connection):
    tables = [_table("A", ("k", "x")), _table("B", ("k", "x"))]
    left = _scan("a", "A", ("k", "x"))
    right = _scan("b", "B", ("k", "x"))
    join = {
        "id": "join",
        "op": "join",
        "left": "a",
        "right": "b",
        "kind": "inner",
        "predicate": {
            "kind": "eq",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "column", "column": "b.k"},
        },
    }
    nodes = [left, right, join]
    output = ["a.k", "b.k"]
    before = _snapshot(tables, nodes, "join", output)
    after = _snapshot(
        tables,
        nodes,
        "join",
        output,
        [
            _stage("left", ["a"], [], ["a"], "row"),
            _stage("right", ["b"], [], ["b"], "row"),
            _stage("join_stage", ["join"], ["a", "b"], ["join"]),
        ],
        [
            _edge("left_edge", "left", "join_stage", 0, left_connection),
            _edge("right_edge", "right", "join_stage", 1, right_connection),
        ],
    )
    return before, after


def _terms(families):
    for family in families:
        for outcome in family.outcomes:
            yield outcome.enabled
            for row in outcome.relation.rows:
                yield row.present
                for fact in row.partition_facts:
                    yield fact.term
                for column in outcome.relation.columns:
                    value = row.values[column.name]
                    yield value.is_null
                    yield value.value


def _walk(term):
    yield term
    for argument in term.arguments:
        yield from _walk(argument)


def _ground_term(term, symbols, functions):
    operation = term.operation
    if operation == "symbol":
        return symbols[term.atom]
    if operation in {"bool", "int"}:
        return term.atom
    arguments = tuple(_ground_term(argument, symbols, functions) for argument in term.arguments)
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
    return functions[(operation, arguments)]


def _bind(environment, term, value):
    if term.operation == "symbol":
        environment[term.atom] = value
    elif _ground_term(term, environment, {}) != value:
        raise AssertionError(f"cannot bind constant {term.render()} to {value!r}")


def _witness_environment(problem, database):
    environment = {}
    for table, symbolic_rows in problem.witness.items():
        concrete_rows = database[table]
        if len(symbolic_rows) != len(concrete_rows):
            raise AssertionError("symbolic and concrete row bounds differ")
        for symbolic, concrete in zip(symbolic_rows, concrete_rows):
            _bind(environment, symbolic.present, concrete.present)
            for name, symbolic_cell in symbolic.cells.items():
                concrete_cell = concrete.cell(name)
                _bind(environment, symbolic_cell.is_null, concrete_cell.is_null)
                _bind(environment, symbolic_cell.value, concrete_cell.value)
    return environment


def _bag(family, symbols, functions):
    enabled = [
        outcome
        for outcome in family.outcomes
        if _ground_term(outcome.enabled, symbols, functions)
    ]
    if len(enabled) != 1:
        raise AssertionError("reference cases require one enabled deterministic outcome")
    outcome = enabled[0]
    rows = Counter()
    for row in outcome.relation.rows:
        present = _ground_term(row.present, symbols, functions)
        if present:
            for fact in row.partition_facts:
                if _ground_term(fact.term, symbols, functions) != fact.value:
                    raise AssertionError(
                        "present symbolic row contradicts its partition fact"
                    )
        if not present:
            continue
        visible = []
        for column in outcome.relation.columns:
            value = row.values[column.name]
            visible.append(
                None
                if _ground_term(value.is_null, symbols, functions)
                else _ground_term(value.value, symbols, functions)
            )
        rows[tuple(visible)] += 1
    return _bag_key(rows)


def _bag_key(rows):
    return frozenset(rows.items())


def _production_possibilities(problem, families, database):
    terms = tuple(_terms(families))
    base = _witness_environment(problem, database)
    free_symbols = sorted(
        {
            term.atom
            for root in terms
            for term in _walk(root)
            if term.operation == "symbol" and term.atom not in base
        }
    )
    symbol_sorts = {
        term.atom: term.sort
        for root in terms
        for term in _walk(root)
        if term.operation == "symbol" and term.atom in free_symbols
    }
    if set(symbol_sorts.values()) - {"Bool"}:
        raise AssertionError("reference cases only admit free Boolean routing symbols")

    applications = tuple(
        term
        for root in terms
        for term in _walk(root)
        if term.operation not in BUILTIN_TERMS
    )
    results = set()
    for symbol_values in product((False, True), repeat=len(free_symbols)):
        symbols = dict(base, **dict(zip(free_symbols, symbol_values)))
        application_sorts = {}
        for application in applications:
            arguments = tuple(
                _ground_term(argument, symbols, {})
                for argument in application.arguments
            )
            application_sorts[(application.operation, arguments)] = application.sort
        if set(application_sorts.values()) - {"Bool"}:
            raise AssertionError("reference cases only admit Boolean routing functions")
        keys = sorted(application_sorts, key=repr)
        for function_values in product((False, True), repeat=len(keys)):
            functions = dict(zip(keys, function_values))
            results.add(tuple(_bag(family, symbols, functions) for family in families))
    return results


class SymbolicStageGraph:
    """One constructed obligation plus its publicly observed symbolic bags."""

    def __init__(self, before, after):
        self.edges = {}
        self.roots = {}

        def edge_observer(edge, task, family):
            self.edges[(edge.id, task)] = family

        def boundary_observer(side, family):
            self.roots[side] = family

        self.problem = build_problem(
            before,
            after,
            2,
            after_edge_observer=edge_observer,
            boundary_observer=boundary_observer,
        )

    def edge_possibilities(self, edge_id, database):
        tasks = sorted(task for edge, task in self.edges if edge == edge_id)
        if tasks != list(range(len(tasks))):
            raise AssertionError("observed task indices are not contiguous")
        return _production_possibilities(
            self.problem,
            [self.edges[(edge_id, task)] for task in tasks],
            database,
        )

    def join_possibilities(self, database):
        families = [
            self.edges[(edge, task)]
            for edge in ("left_edge", "right_edge")
            for task in range(TASKS)
        ]
        families.append(self.roots["after"])
        return _production_possibilities(self.problem, families, database)


def _source_partitions(rows, routes, columns=("k",)):
    result = [Counter(), Counter()]
    for row, task in zip(rows, routes):
        if row.present:
            result[task][row.visible(columns)] += 1
    return tuple(result)


def _hash_key(row, connection):
    cells = []
    for name in connection["keys"]:
        cell = row.cell(name.rsplit(".", 1)[-1])
        cells.append((cell.is_null, 0 if cell.is_null else cell.value))
    return connection["hash_function"], tuple(cells)


def _reference_connection(rows, connection, consumer_tasks):
    kind = connection["kind"]
    results = set()
    for routes in product(range(TASKS), repeat=len(rows)):
        source = _source_partitions(rows, routes)
        gathered = source[0] + source[1]
        if kind == "map":
            connected = source
        elif kind == "broadcast":
            connected = tuple(gathered.copy() for _ in range(consumer_tasks))
        elif kind == "union_all" and not connection["parallel"]:
            connected = (gathered,)
        elif kind == "union_all" and connection["parallel"]:
            connected = source if consumer_tasks == TASKS else (gathered,)
        elif kind == "hash_shuffle":
            keys = sorted({_hash_key(row, connection) for row in rows if row.present}, key=repr)
            for targets in product(range(consumer_tasks), repeat=len(keys)):
                routing = dict(zip(keys, targets))
                partitions = [Counter() for _ in range(consumer_tasks)]
                for row in rows:
                    if row.present:
                        partitions[routing[_hash_key(row, connection)]][row.visible(("k",))] += 1
                results.add(tuple(_bag_key(partition) for partition in partitions))
            continue
        else:
            raise AssertionError(f"unsupported reference connection {connection!r}")
        results.add(tuple(_bag_key(partition) for partition in connected))
    return results


def _connect_local(rows, routes, connection, hash_routes):
    source = _source_partitions(rows, routes, ("k", "x"))
    gathered = source[0] + source[1]
    kind = connection["kind"]
    if kind == "map" or kind == "union_all" and connection["parallel"]:
        return source
    if kind == "broadcast":
        return gathered.copy(), gathered.copy()
    if kind == "hash_shuffle":
        partitions = [Counter(), Counter()]
        for row in rows:
            if row.present:
                partitions[hash_routes[_hash_key(row, connection)]][row.visible(("k", "x"))] += 1
        return tuple(partitions)
    raise AssertionError(f"unsupported two-task local connection {connection!r}")


def _local_join(left, right):
    result = Counter()
    for left_row, left_count in left.items():
        for right_row, right_count in right.items():
            if left_row[0] is not None and left_row[0] == right_row[0]:
                result[(left_row[0], right_row[0])] += left_count * right_count
    return result


def _visible_rows(rows, columns):
    return [row.visible(columns) for row in rows if row.present]


def _global_join(database):
    left = Counter(_visible_rows(database["A"], ("k", "x")))
    right = Counter(_visible_rows(database["B"], ("k", "x")))
    return _bag_key(_local_join(left, right))


def _reference_join(database, left_connection, right_connection):
    left_rows = database["A"]
    right_rows = database["B"]
    hash_keys = sorted(
        {
            _hash_key(row, connection)
            for rows, connection in (
                (left_rows, left_connection),
                (right_rows, right_connection),
            )
            if connection["kind"] == "hash_shuffle"
            for row in rows
            if row.present
        },
        key=repr,
    )
    results = set()
    for source_routes in product(range(TASKS), repeat=len(left_rows) + len(right_rows)):
        left_routes = source_routes[: len(left_rows)]
        right_routes = source_routes[len(left_rows) :]
        for targets in product(range(TASKS), repeat=len(hash_keys)):
            hash_routes = dict(zip(hash_keys, targets))
            left = _connect_local(left_rows, left_routes, left_connection, hash_routes)
            right = _connect_local(right_rows, right_routes, right_connection, hash_routes)
            joined = _local_join(left[0], right[0]) + _local_join(left[1], right[1])
            results.add(
                tuple(_bag_key(partition) for partition in (*left, *right, joined))
            )
    return results


MAP = {"kind": "map"}
BROADCAST = {"kind": "broadcast"}
SERIAL_UNION = {"kind": "union_all", "parallel": False}
PARALLEL_UNION = {"kind": "union_all", "parallel": True}


def _shuffle(prefix, key="k", function="HashV1"):
    return {
        "kind": "hash_shuffle",
        "keys": [f"{prefix}.{key}"],
        "hash_function": function,
        "use_spilling": False,
    }


class StageGraphReferenceTest(unittest.TestCase):
    maxDiff = None

    def test_every_connection_matches_the_exhaustive_two_slot_reference(self):
        connections = (
            ("map", MAP, TASKS),
            ("hash_shuffle", _shuffle("a"), TASKS),
            ("broadcast", BROADCAST, 1),
            ("serial_union_all", SERIAL_UNION, 1),
            ("parallel_union_all", PARALLEL_UNION, TASKS),
        )
        for name, connection, consumer_tasks in connections:
            before, after = _connection_pair(connection)
            symbolic = SymbolicStageGraph(before, after)
            for left, right in product(ONE_COLUMN_ROWS, repeat=2):
                database = {"A": (left, right)}
                with self.subTest(connection=name, left=left, right=right):
                    self.assertEqual(
                        symbolic.edge_possibilities("tested", database),
                        _reference_connection(
                            database["A"], connection, consumer_tasks
                        ),
                    )

    def test_reference_distinguishes_routing_and_union_mutations(self):
        duplicate = GroundRow(True, (("k", Cell(False, 0)),))
        rows = (duplicate, duplicate)
        self.assertNotEqual(
            _reference_connection(rows, _shuffle("a"), TASKS),
            _reference_connection(rows, MAP, TASKS),
        )
        self.assertNotEqual(
            _reference_connection(rows, SERIAL_UNION, 1),
            _reference_connection(rows, PARALLEL_UNION, TASKS),
        )
        self.assertNotEqual(
            _reference_connection(rows, BROADCAST, TASKS),
            _reference_connection(rows, PARALLEL_UNION, TASKS),
        )

    def test_broadcast_and_parallel_union_local_join_match_the_reference(self):
        cases = _join_cases()
        for right_connection in (BROADCAST, PARALLEL_UNION):
            before, after = _join_pair(MAP, right_connection)
            symbolic = SymbolicStageGraph(before, after)
            for name, database in cases:
                with self.subTest(connection=right_connection["kind"], case=name):
                    self.assertEqual(
                        symbolic.join_possibilities(database),
                        _reference_join(database, MAP, right_connection),
                    )

        for name, database in cases:
            with self.subTest(connection="broadcast", case=name, check="global"):
                roots = {
                    state[-1]
                    for state in _reference_join(database, MAP, BROADCAST)
                }
                self.assertEqual(roots, {_global_join(database)})

        matching = dict(cases)["one_match"]
        correct_roots = {
            state[-1] for state in _reference_join(matching, MAP, BROADCAST)
        }
        mutated_roots = {
            state[-1] for state in _reference_join(matching, MAP, PARALLEL_UNION)
        }
        self.assertEqual(len(correct_roots), 1)
        self.assertGreater(len(mutated_roots), 1)
        self.assertNotEqual(correct_roots, mutated_roots)

    def test_matching_and_wrong_hash_local_joins_match_the_reference(self):
        variants = (
            ("matching", _shuffle("a"), _shuffle("b")),
            ("wrong_function", _shuffle("a"), _shuffle("b", function="HashV2")),
            ("wrong_key", _shuffle("a"), _shuffle("b", key="x")),
        )
        cases = _join_cases()
        for name, left_connection, right_connection in variants:
            before, after = _join_pair(left_connection, right_connection)
            symbolic = SymbolicStageGraph(before, after)
            for case, database in cases:
                with self.subTest(variant=name, case=case):
                    self.assertEqual(
                        symbolic.join_possibilities(database),
                        _reference_join(database, left_connection, right_connection),
                    )

        for case, database in cases:
            with self.subTest(variant="matching", case=case, check="global"):
                roots = {
                    state[-1]
                    for state in _reference_join(
                        database, _shuffle("a"), _shuffle("b")
                    )
                }
                self.assertEqual(roots, {_global_join(database)})

        matching = dict(cases)["one_match"]
        matching_roots = {
            state[-1]
            for state in _reference_join(matching, _shuffle("a"), _shuffle("b"))
        }
        wrong_function_roots = {
            state[-1]
            for state in _reference_join(
                matching,
                _shuffle("a"),
                _shuffle("b", function="HashV2"),
            )
        }
        wrong_key_roots = {
            state[-1]
            for state in _reference_join(
                matching,
                _shuffle("a"),
                _shuffle("b", key="x"),
            )
        }
        self.assertEqual(len(matching_roots), 1)
        self.assertGreater(len(wrong_function_roots), 1)
        self.assertGreater(len(wrong_key_roots), 1)


def _join_cases():
    absent = _absent_local()

    def database(left, right):
        return {
            "A": tuple(left) + (absent,) * (2 - len(left)),
            "B": tuple(right) + (absent,) * (2 - len(right)),
        }

    return (
        ("empty", database((), ())),
        ("one_match", database((_row(0, 10),), (_row(0, 20),))),
        ("mismatch", database((_row(0, 10),), (_row(1, 20),))),
        (
            "sql_null",
            database(
                (_row(None, 10, null_payload=1),),
                (_row(None, 20, null_payload=0),),
            ),
        ),
        ("duplicate_left", database((_row(0, 10), _row(0, 11)), (_row(0, 20),))),
        ("duplicate_right", database((_row(0, 10),), (_row(0, 20), _row(0, 21)))),
        ("duplicate_both", database((_row(0, 10), _row(0, 11)), (_row(0, 20), _row(0, 21)))),
        (
            "absent_payload",
            {
                "A": (_row(0, 10), _absent_local(null_payload=1)),
                "B": (_row(0, 20), _absent_local(null_payload=0)),
            },
        ),
    )


if __name__ == "__main__":
    unittest.main()
