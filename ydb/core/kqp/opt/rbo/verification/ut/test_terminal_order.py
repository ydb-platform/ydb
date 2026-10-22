"""Pure certificate admission and independent finite sorted-language controls."""
import copy
import contextlib
import io
import json
from itertools import permutations, product
from types import SimpleNamespace
import unittest
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.analysis import ValidatedPlan
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import Project, SnapshotError, Sort, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.terminal_order import certify


def _col(name):
    return {"kind": "column", "column": name}


def _project(name, source, names):
    return {"id": name, "op": "project", "input": source, "ordered": True,
            "columns": [{"output": output, "expression": _col(column)} for output, column in names]}


def _stage(name, nodes, inputs, output, storage=None):
    return {"id": name, "nodes": nodes, "inputs": inputs,
            "outputs": [{"index": 0, "node": output}], "source_storage": storage}


def _raw(*, staged=False, key_type="Int32", nullable=True, join_kind="inner"):
    columns = ["lj", "lk", "lv", "rj", "rv"]
    nodes = [
        {"id": "left", "op": "scan", "table": "L", "columns": [
            {"source": source, "output": output} for source, output in (("j", "lj"), ("k", "lk"), ("v", "lv"))]},
        {"id": "right", "op": "scan", "table": "R", "columns": [
            {"source": source, "output": output} for source, output in (("j", "rj"), ("v", "rv"))]},
        {"id": "join", "op": "join", "kind": join_kind, "left": "left", "right": "right",
         "keys": [{"left": "lj", "right": "rj"}],
         "predicate": {"kind": "literal", "type": "Bool", "value": True}},
    ]
    sort_input, sort_key = "join", "lk"
    if not staged:
        nodes.append(_project("aliases", "join", [(n, n) for n in columns] + [("key_alias", "lk")]))
        sort_input, sort_key = "aliases", "key_alias"
    order = [{"column": sort_key, "ascending": True, "nulls_first": True}]
    nodes.append({"id": "sort", "op": "sort", "input": sort_input, "order": order,
                  "limit": None, "phase": "intermediate" if staged else "undefined"})
    nodes.append(_project("post", "sort", [(n, n) for n in columns] + ([] if staged else [("key_alias", "key_alias")])))
    nodes.append(_project("result", "post", [("outk", sort_key), ("outl", "lv"), ("outr", "rv")]))
    graph = None
    if staged:
        graph = {"root_stage": "root", "assumptions": [], "stages": [
            _stage("source_l", ["left"], [], "left", "row"),
            _stage("source_r", ["right"], [], "right", "row"),
            _stage("compute", ["join", "sort"], ["left", "right"], "sort"),
            _stage("root", ["post", "result"], ["sort"], "result"),
        ], "edges": [
            {"id": "el", "kind": "hash_shuffle", "producer": "source_l", "producer_output": 0,
             "consumer": "compute", "consumer_input": 0, "occurrence": 0,
             "keys": ["lj"], "hash_function": "HashV2", "use_spilling": True},
            {"id": "er", "kind": "hash_shuffle", "producer": "source_r", "producer_output": 0,
             "consumer": "compute", "consumer_input": 1, "occurrence": 0,
             "keys": ["rj"], "hash_function": "HashV2", "use_spilling": True},
            {"id": "merge", "kind": "merge", "producer": "compute", "producer_output": 0,
             "consumer": "root", "consumer_input": 0, "occurrence": 0, "order": copy.deepcopy(order)},
        ]}
    return {"format": "ydb-rbo-semantic-snapshot", "version": 1,
            "schema": {"tables": [
                {"name": "L", "columns": [
                    {"name": "j", "type": "Int32", "nullable": False},
                    {"name": "k", "type": key_type, "nullable": nullable},
                    {"name": "v", "type": "Utf8", "nullable": True}], "unique_keys": []},
                {"name": "R", "columns": [
                    {"name": "j", "type": "Int32", "nullable": False},
                    {"name": "v", "type": "Utf8", "nullable": True}], "unique_keys": []},
            ]},
            "plan": {"nodes": nodes, "root": "result", "output": ["outk", "outl", "outr"], "subplans": []},
            "stage_graph": graph}


def _plans(**kwargs):
    return tuple(ValidatedPlan(parse_snapshot(_raw(staged=staged, **kwargs))) for staged in (False, True))


class TerminalOrderAdmissionTest(unittest.TestCase):
    def test_exact_visible_aliases_nullable_keys_and_decimal_types_are_admitted(self):
        for kind in ("Int32", "String", "Utf8", "Date", "Timestamp64", "Decimal(5,0)", "Decimal(18,8)", "Decimal(35,4)"):
            for nullable, join_kind in product((False, True), ("inner", "left")):
                with self.subTest(kind=kind, nullable=nullable, join=join_kind):
                    before, after = _plans(key_type=kind, nullable=nullable, join_kind=join_kind)
                    cert = certify(before, after)
                    self.assertIsNotNone(cert)
                    self.assertIs(cert.original_before, before.snapshot)
                    self.assertIs(cert.original_after, after.snapshot)
                    self.assertEqual(cert.visible_order[0].output, "outk")
                    self.assertEqual(cert.visible_order[0].type, kind)
                    self.assertEqual(cert.visible_order[0].nullable, nullable)

    def test_only_terminal_sort_and_merge_change_and_routing_is_retained(self):
        before, after = _plans()
        cert = certify(before, after)
        self.assertIsNotNone(cert)
        self.assertTrue(any(isinstance(n, Sort) for n in before.snapshot.plan.nodes))
        for original, reduced in ((before, cert.before), (after, cert.after)):
            for old, new in zip(original.snapshot.plan.nodes, reduced.snapshot.plan.nodes):
                if isinstance(old, Sort):
                    self.assertIsInstance(new, Project)
                    self.assertFalse(new.ordered)
                    self.assertEqual(tuple(p.output for p in new.columns), tuple(original.schemas[old.input]))
                else:
                    self.assertIs(old, new)
            self.assertIs(original.snapshot.tables, reduced.snapshot.tables)
            self.assertEqual(original.output_schema, reduced.output_schema)
        old_edges, new_edges = after.snapshot.stage_graph.edges, cert.after.snapshot.stage_graph.edges
        self.assertIs(old_edges[0], new_edges[0])
        self.assertIs(old_edges[1], new_edges[1])
        self.assertEqual(new_edges[2].kind, "union_all")
        self.assertFalse(new_edges[2].parallel)
        self.assertEqual(new_edges[2].order, ())

    def test_changed_routing_remains_in_the_obligation(self):
        before, _ = _plans()
        raw = _raw(staged=True)
        raw["stage_graph"]["edges"][0]["keys"] = ["lk"]
        after = ValidatedPlan(parse_snapshot(raw))
        cert = certify(before, after)
        self.assertIsNotNone(cert)
        self.assertEqual(cert.after.snapshot.stage_graph.edges[0].keys, ("lk",))
        self.assertIs(cert.after.snapshot.stage_graph.edges[0], after.snapshot.stage_graph.edges[0])

    def test_hidden_order_key_does_not_get_a_certificate(self):
        plans = []
        for staged in (False, True):
            raw = _raw(staged=staged)
            raw["plan"]["nodes"][-1]["columns"] = raw["plan"]["nodes"][-1]["columns"][1:]
            raw["plan"]["output"] = ["outl", "outr"]
            plans.append(ValidatedPlan(parse_snapshot(raw)))
        self.assertIsNone(certify(*plans))

    def test_direction_null_order_and_key_priority_must_match(self):
        before, _ = _plans()
        for field in ("ascending", "nulls_first"):
            raw = _raw(staged=True)
            sort = next(n for n in raw["plan"]["nodes"] if n["op"] == "sort")
            sort["order"][0][field] = False
            raw["stage_graph"]["edges"][-1]["order"][0][field] = False
            self.assertIsNone(certify(before, ValidatedPlan(parse_snapshot(raw))))
        plans = []
        for staged in (False, True):
            raw = _raw(staged=staged)
            sort = next(n for n in raw["plan"]["nodes"] if n["op"] == "sort")
            sort["order"].append({"column": "lv", "ascending": True, "nulls_first": True})
            if staged:
                sort["order"].reverse()
                raw["stage_graph"]["edges"][-1]["order"] = copy.deepcopy(sort["order"])
            plans.append(ValidatedPlan(parse_snapshot(raw)))
        self.assertIsNone(certify(*plans))

    def test_merge_order_must_be_the_local_sort_order(self):
        before, _ = _plans()
        raw = _raw(staged=True)
        raw["stage_graph"]["edges"][-1]["order"][0]["ascending"] = False
        self.assertIsNone(certify(before, ValidatedPlan(parse_snapshot(raw))))

    def test_top_sort_pushed_limit_and_unordered_wrapper_keep_generic_path(self):
        for mutation in ("top", "pushed", "unordered"):
            raw = _raw(staged=mutation == "pushed")
            if mutation == "top":
                next(n for n in raw["plan"]["nodes"] if n["op"] == "sort")["limit"] = {
                    "kind": "literal", "type": "Uint64", "value": 1}
            elif mutation == "pushed":
                raw["plan"]["nodes"][0]["pushed_limit"] = {"kind": "literal", "type": "Uint64", "value": 1}
                raw["stage_graph"]["stages"][0]["source_storage"] = "column"
            else:
                raw["plan"]["nodes"][-1]["ordered"] = False
            changed = ValidatedPlan(parse_snapshot(raw))
            self.assertIsNone(certify(_plans()[0], changed) if mutation == "pushed" else certify(changed, _plans()[1]))

    def test_error_and_totality_metadata_keep_generic_path(self):
        for before_sort, totality in ((False, False), (False, True), (True, True)):
            raw_before = _raw(key_type="String")
            raw_after = _raw(staged=True, key_type="String")
            projection = (raw_before["plan"]["nodes"][3]["columns"][-1] if before_sort
                          else raw_before["plan"]["nodes"][-1]["columns"][0])
            projection.update(error_on_null=True, require_total=totality)
            raw_after["plan"]["nodes"][-1]["columns"][0].update(error_on_null=True, require_total=totality)
            before, after = (ValidatedPlan(parse_snapshot(raw)) for raw in (raw_before, raw_after))
            self.assertEqual(before.output_schema, after.output_schema)
            self.assertIsNone(certify(before, after))

    def test_computed_projection_keeps_generic_path(self):
        for location in (3, -1):
            raw = _raw()
            raw["plan"]["nodes"][location]["columns"][0]["expression"] = {
                "kind": "literal", "type": "Int32", "value": 7}
            self.assertIsNone(certify(ValidatedPlan(parse_snapshot(raw)), _plans()[1]))

    def test_cardinality_error_metadata_keeps_generic_path(self):
        plans = []
        for staged in (False, True):
            raw = _raw(staged=staged)
            raw["schema"]["tables"][0]["columns"].append({"name": "n", "type": "Uint64", "nullable": False})
            raw["plan"]["nodes"][0]["columns"].append({"source": "n", "output": "ln"})
            for node in raw["plan"]["nodes"]:
                if node["op"] == "project" and node["id"] != "result":
                    node["columns"].append({"output": "ln", "expression": _col("ln")})
            raw["plan"]["nodes"][-1]["columns"][1]["require_at_most_one"] = "ln"
            plans.append(ValidatedPlan(parse_snapshot(raw)))
        self.assertEqual(plans[0].output_schema, plans[1].output_schema)
        self.assertIsNone(certify(*plans))

    def test_sort_consumed_by_a_join_is_not_terminal(self):
        raw = _raw()
        nodes = {n["id"]: n for n in raw["plan"]["nodes"]}
        nodes["sort"]["input"] = "left"
        nodes["sort"]["order"][0]["column"] = "lk"
        nodes["join"]["left"] = "sort"
        nodes["post"]["input"] = "aliases"
        raw["plan"]["nodes"] = [nodes[name] for name in ("left", "right", "sort", "join", "aliases", "post", "result")]
        self.assertIsNone(certify(ValidatedPlan(parse_snapshot(raw)), _plans()[1]))

    def test_scan_predicate_and_nontrivial_join_predicate_keep_generic_path(self):
        for index in (0, 2):
            raw = _raw()
            raw["plan"]["nodes"][index]["predicate"] = {"kind": "literal", "type": "Bool", "value": False}
            self.assertIsNone(certify(ValidatedPlan(parse_snapshot(raw)), _plans()[1]))

    def test_binary64_and_comparison_tags_are_not_admitted(self):
        plans = []
        for staged in (False, True):
            raw = _raw(staged=staged)
            raw["semantic_mode"] = "binary64_uf_universal_v1"
            plans.append(ValidatedPlan(parse_snapshot(raw)))
        self.assertIsNone(certify(*plans))
        for tag in ("integral_avg_rank_v1", "binary64_total_v1", "unknown_total_order"):
            raw = _raw()
            next(n for n in raw["plan"]["nodes"] if n["op"] == "sort")["order"][0]["comparison"] = tag
            with self.assertRaises(SnapshotError):
                parse_snapshot(raw)

    def test_missing_merge_and_missing_local_sort_keep_generic_path(self):
        before, _ = _plans()
        raw = _raw(staged=True)
        raw["stage_graph"]["edges"][-1].pop("order")
        raw["stage_graph"]["edges"][-1].update(kind="union_all", parallel=False)
        self.assertIsNone(certify(before, ValidatedPlan(parse_snapshot(raw))))
        raw = _raw(staged=True)
        at = next(i for i, n in enumerate(raw["plan"]["nodes"]) if n["op"] == "sort")
        raw["plan"]["nodes"][at] = _project("sort", "join", [(n, n) for n in ("lj", "lk", "lv", "rj", "rv")])
        self.assertIsNone(certify(before, ValidatedPlan(parse_snapshot(raw))))


class TerminalOrderBuilderScopeTest(unittest.TestCase):
    def test_certified_endpoint_keeps_normal_verdict_json_fields(self):
        # Mock the symbolic/solver phases only to exercise endpoint metadata;
        # this test does not claim a proof or construct a formula.
        from ydb.core.kqp.opt.rbo.verification.rbo_verifier import cli, smt, terminal_order, verify
        before, after = (plan.snapshot for plan in _plans())
        problem = verify.Problem(script=object(), witness={})
        scalar = SimpleNamespace(register_parameters=lambda value: None, semantic_mode=None,
                                 uses_abstract_integral_average=False, parameters={}, list_parameters={})
        with mock.patch.object(smt, "Script", return_value=object()), \
                mock.patch.object(verify, "Database", return_value=object()), \
                mock.patch.object(verify, "ScalarEncoder", return_value=scalar), \
                mock.patch.object(verify, "Router", return_value=object()), \
                mock.patch.object(verify, "_evaluate_boundary", return_value=object()), \
                mock.patch.object(verify, "family_mismatch", return_value=object()), \
                mock.patch.object(verify, "_finish_problem", return_value=problem), \
                mock.patch.object(terminal_order, "certify", wraps=terminal_order.certify) as inspected:
            built = verify.build_problem(before, after, 2)
            inspected.assert_called_once()
        self.assertIs(built, problem)
        self.assertIsNone(built.observation_kind)
        for status in ("VERIFIED_BOUNDED", "COUNTEREXAMPLE"):
            output = io.StringIO()
            witness = {"L": [], "R": []} if status == "COUNTEREXAMPLE" else None
            result = verify.Result(status, 2, witness=witness)
            with mock.patch.object(cli, "load_snapshot", side_effect=(before, after)), \
                    mock.patch.object(cli, "build_problem", return_value=built), \
                    mock.patch.object(cli, "solve", return_value=result), contextlib.redirect_stdout(output):
                code = cli.main(["before.json", "after.json", "--rows", "2", "--solver", "never-started"])
            self.assertEqual(code, 0 if status == "VERIFIED_BOUNDED" else 1)
            self.assertEqual(json.loads(output.getvalue()), result.to_json())
            self.assertNotIn("observation_kind", json.loads(output.getvalue()))

    def test_only_plain_endpoint_requests_certificate_before_any_smt_allocation(self):
        from ydb.core.kqp.opt.rbo.verification.rbo_verifier import smt, terminal_order, verify
        before, after = (plan.snapshot for plan in _plans())

        class StopBeforeConstruction(Exception):
            pass
        for observer_name in (None, "before_node_observer", "after_node_observer", "after_edge_observer",
                              "boundary_observer", "comparison_observer"):
            options = {} if observer_name is None else {observer_name: lambda *args: None}
            with mock.patch.object(smt, "Script", side_effect=StopBeforeConstruction), \
                    mock.patch.object(terminal_order, "certify", wraps=terminal_order.certify) as inspected:
                with self.assertRaises(StopBeforeConstruction):
                    verify.build_problem(before, after, 2, **options)
                self.assertEqual(inspected.call_count, 1 if observer_name is None else 0)

    def test_bundle_and_transformation_prefix_remain_generic_before_smt_allocation(self):
        from ydb.core.kqp.opt.rbo.verification.rbo_verifier import bundle, smt, terminal_order, verify
        before, after = (plan.snapshot for plan in _plans())

        class StopBeforeConstruction(Exception):
            pass
        builders = (
            lambda: bundle.build_bundle_problem(((before, after),), 2),
            lambda: bundle.build_bundle_problem(((before, after), (before, after)), 2),
            lambda: verify.build_transformation_prefix_problem(before, after, 2),
        )
        for builder in builders:
            with mock.patch.object(smt, "Script", side_effect=StopBeforeConstruction), \
                    mock.patch.object(terminal_order, "certify", wraps=terminal_order.certify) as inspected:
                with self.assertRaises(StopBeforeConstruction):
                    builder()
                inspected.assert_not_called()


# Independent concrete reference: no SMT terms, verifier evaluator, native
# comparator, or certificate implementation is used by these language helpers.
def _less(left, right, order):
    for position, ascending, nulls_first in order:
        a, b = left[position], right[position]
        if a == b:
            continue
        if a is None or b is None:
            return (a is None) == nulls_first
        return a < b if ascending else a > b
    return False


def _sorted(rows, order):
    return all(not _less(rows[j], rows[i], order) for i in range(len(rows)) for j in range(i + 1, len(rows)))


def _global(rows, order):
    return {p for p in permutations(rows) if _sorted(p, order)}


def _interleave(left, right):
    if not left:
        yield right
    elif not right:
        yield left
    else:
        for tail in _interleave(left[1:], right):
            yield (left[0],) + tail
        for tail in _interleave(left, right[1:]):
            yield (right[0],) + tail


def _merged(rows, route, order):
    groups = tuple(tuple(row for row, task in zip(rows, route) if task == group) for group in (0, 1))
    return {sequence for a in _global(groups[0], order) for b in _global(groups[1], order)
            for sequence in _interleave(a, b) if _sorted(sequence, order)}


class TerminalOrderLanguageReferenceTest(unittest.TestCase):
    def test_exhaustive_small_integer_null_bags_and_all_routes(self):
        for size in range(5):
            for keys in product((None, 0, 1), repeat=size):
                rows = tuple((key, index) for index, key in enumerate(keys))
                for ascending, nulls_first in product((False, True), repeat=2):
                    order = ((0, ascending, nulls_first),)
                    global_language = _global(rows, order)
                    self.assertTrue(global_language)
                    for route in product((0, 1), repeat=size):
                        self.assertEqual(_merged(rows, route, order), global_language)

    def test_decimal_null_nan_infinities_and_finite_values_all_routes(self):
        # Native Decimal Sort compares signed int128 codes, not IEEE NaNs or
        # ordinary SQL Decimal equality.  Spell the independent domain here.
        infinity = 10 ** 35
        codes = (None, -infinity, -2, 0, 2, infinity, infinity + 1)
        self.assertEqual(tuple(sorted(codes[1:])), codes[1:])
        for keys in product(codes, repeat=3):
            rows = tuple((key, index) for index, key in enumerate(keys))
            for ascending, nulls_first in product((False, True), repeat=2):
                order = ((0, ascending, nulls_first),)
                expected = _global(rows, order)
                for route in product((0, 1), repeat=3):
                    self.assertEqual(_merged(rows, route, order), expected)

    def test_multikey_visible_projection_duplicate_payloads_and_multiplicity(self):
        rows = ((None, 1, "same", "hidden-a"), (None, 1, "same", "hidden-b"),
                (0, None, "other", "hidden-c"), (0, 2, "third", "hidden-d"))
        order = ((0, False, True), (1, True, False))
        visible = tuple(row[:3] for row in rows)
        projected = {tuple(row[:3] for row in sequence) for sequence in _global(rows, order)}
        self.assertEqual(projected, _global(visible, order))
        for route in product((0, 1), repeat=4):
            self.assertEqual(_merged(visible, route, order), projected)
        self.assertNotEqual(_global(visible[:-1], order), projected)
        self.assertNotEqual(_global(visible + visible[:1], order), projected)

    def test_fixed_local_ties_and_correlated_local_choices_are_incomplete(self):
        rows = ((0, "a"), (0, "b"), (0, "c"), (0, "d"))
        order = ((0, True, True),)
        expected = _global(rows, order)
        fixed = set(_interleave(rows[:2], rows[2:]))
        correlated = fixed | set(_interleave(tuple(reversed(rows[:2])), tuple(reversed(rows[2:]))))
        self.assertLess(len(fixed), len(expected))
        self.assertLess(len(correlated), len(expected))
        self.assertEqual(_merged(rows, (0, 0, 1, 1), order), expected)

    def test_hidden_keys_directions_null_order_and_errors_are_observable(self):
        left, right = ((0, "a"), (1, "b")), ((1, "a"), (0, "b"))
        order = ((0, True, True),)

        def projection(language):
            return {tuple(row[1] for row in sequence) for sequence in language}

        self.assertEqual(sorted(row[1] for row in left), sorted(row[1] for row in right))
        self.assertNotEqual(projection(_global(left, order)), projection(_global(right, order)))
        self.assertNotEqual(_global(left, order), _global(left, ((0, False, True),)))
        nullable = ((None, "a"), (1, "b"))
        self.assertNotEqual(_global(nullable, order), _global(nullable, ((0, True, False),)))
        success = {("success", sequence) for sequence in _global(left, order)}
        error = {("error",)}
        self.assertNotEqual(success, error)
        self.assertNotEqual({("success", ())}, error)


if __name__ == "__main__":
    unittest.main()
