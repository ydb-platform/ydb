"""Passive stage ports preserve one producer, including its local choices."""
import copy
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import SnapshotError, parse_snapshot
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem, query_solver
if __package__:
    from .test_verify import SOLVER
else:
    from test_verify import SOLVER


def _column(name):
    return {"kind": "column", "column": name}


def _stage(name, nodes, inputs, outputs, storage=None):
    return {"id": name, "nodes": nodes, "inputs": inputs, "source_storage": storage,
            "outputs": [{"index": index, "node": node} for index, node in enumerate(outputs)]}


def _snapshot(*, staged=False, storage="row", shared_limit=False, wrong_hash=False):
    nodes = [{"id": "scan", "op": "scan", "table": "T", "columns": [
        {"source": name, "output": name} for name in ("k", "x")]}]
    producer = "scan"
    if shared_limit:
        nodes.append({"id": "limit", "op": "limit", "input": producer,
                      "count": {"kind": "literal", "type": "Uint64", "value": 1}, "offset": None, "phase": "undefined"})
        producer = "limit"
    nodes += [{"id": port, "op": "project", "input": producer, "ordered": True,
               "columns": [{"output": prefix + name, "expression": _column(name)} for name in ("k", "x")]}
              for port, prefix in (("p0", "l."), ("p1", "r."))]
    nodes.append({"id": "join", "op": "join", "kind": "inner", "left": "p0", "right": "p1",
                  "keys": [{"left": "l.k", "right": "r.k"}], "predicate": {"kind": "literal", "type": "Bool", "value": True}})
    graph = None
    if staged:
        stages = [_stage("source", ["scan"], [], ["scan"], storage)] if shared_limit else []
        stages += [_stage("ports", (["limit"] if shared_limit else ["scan"]) + ["p0", "p1"],
                          ["scan"] if shared_limit else [], ["p0", "p1"], None if shared_limit else storage),
                   _stage("root", ["join"], ["p0", "p1"], ["join"])]
        edges = [{"id": "source", "producer": "source", "consumer": "ports", "occurrence": 0,
                  "producer_output": 0, "consumer_input": 0, "kind": "union_all", "parallel": False}] if shared_limit else []
        for index, name in enumerate(("l.k", "r.x" if wrong_hash else "r.k")):
            connection = ({"kind": "union_all", "parallel": False} if shared_limit else
                          {"kind": "hash_shuffle", "keys": [name], "hash_function": "HashV2", "use_spilling": True})
            edges.append({"id": f"port{index}", "producer": "ports", "consumer": "root", "occurrence": index,
                          "producer_output": index, "consumer_input": index, **connection})
        graph = {"root_stage": "root", "stages": stages, "edges": edges, "assumptions": []}
    return {"format": "ydb-rbo-semantic-snapshot", "version": 1,
            "schema": {"tables": [{"name": "T", "columns": [
                {"name": name, "type": "Int32", "nullable": False} for name in ("k", "x")], "unique_keys": []}]},
            "plan": {"nodes": nodes, "root": "join", "output": ["l.k", "l.x", "r.x"], "subplans": []},
            "stage_graph": graph}


class StagePortValidationTest(unittest.TestCase):
    def test_row_column_and_compute_stages_accept_complete_passive_ports(self):
        for storage, limited in (("row", False), ("column", False), ("row", True)):
            with self.subTest(storage=storage, limited=limited):
                parse_snapshot(_snapshot(staged=True, storage=storage, shared_limit=limited))

    def test_ports_reject_branch_work_and_incomplete_column_correspondences(self):
        changes = ({"ordered": False}, {"columns": [
            {"output": "r.k", "expression": _column("k")},
            {"output": "r.x", "expression": _column("k")}]}, {"columns": [
                {"output": "r.k", "expression": _column("k")},
                {"output": "r.x", "expression": {"kind": "literal", "type": "Int32", "value": 0}}]})
        for change in changes:
            with self.subTest(change=change):
                raw = _snapshot(staged=True)
                raw["plan"]["nodes"][2].update(change)
                with self.assertRaisesRegex(SnapshotError, "local output sink or passive ports"):
                    parse_snapshot(raw)

    def test_ports_must_share_one_local_input(self):
        raw = _snapshot(staged=True)
        other = copy.deepcopy(raw["plan"]["nodes"][0])
        other["id"] = "other_scan"
        raw["plan"]["nodes"].insert(1, other)
        raw["plan"]["nodes"][3]["input"] = "other_scan"
        raw["stage_graph"]["stages"][0]["nodes"].insert(1, "other_scan")
        with self.assertRaisesRegex(SnapshotError, "local output sink or passive ports"):
            parse_snapshot(raw)

    def test_every_port_must_be_exported_once_and_wired_to_its_actual_input(self):
        for mutation in ("duplicate", "missing", "swapped"):
            with self.subTest(mutation=mutation):
                raw = _snapshot(staged=True)
                outputs = raw["stage_graph"]["stages"][0]["outputs"]
                if mutation == "duplicate":
                    outputs[1]["node"] = "p0"
                elif mutation == "missing":
                    outputs.pop()
                else:
                    raw["stage_graph"]["edges"][0]["producer_output"] = 1
                with self.assertRaises(SnapshotError):
                    parse_snapshot(raw)


@unittest.skipUnless(SOLVER, "requires the pinned solver")
class StagePortProofTest(unittest.TestCase):
    def test_hash_ports_prove_and_a_wrong_key_is_a_counterexample(self):
        for storage in ("row", "column"):
            for wrong in (False, True):
                with self.subTest(storage=storage, wrong=wrong):
                    problem = build_problem(parse_snapshot(_snapshot()),
                                            parse_snapshot(_snapshot(staged=True, storage=storage, wrong_hash=wrong)), 2)
                    self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, "sat" if wrong else "unsat")

    def test_one_shared_limit_choice_is_preserved_across_both_ports(self):
        problem = build_problem(parse_snapshot(_snapshot(shared_limit=True)),
                                parse_snapshot(_snapshot(staged=True, shared_limit=True)), 2)
        self.assertEqual(query_solver(problem, SOLVER, timeout_ms=10_000).status, "unsat")


if __name__ == "__main__":
    unittest.main()
