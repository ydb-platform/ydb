import copy
import unittest

from ydb.core.kqp.opt.rbo.verification.rbo_verifier.ir import SnapshotError, parse_snapshot


def minimal_snapshot():
    return {
        "format": "ydb-rbo-semantic-snapshot",
        "version": 1,
        "schema": {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "k", "type": "Int64", "nullable": False},
                        {"name": "flag", "type": "Bool", "nullable": True},
                    ],
                    "unique_keys": [],
                }
            ]
        },
        "plan": {
            "nodes": [
                {
                    "id": "scan",
                    "op": "scan",
                    "table": "A",
                    "columns": [
                        {"source": "k", "output": "a.k"},
                        {"source": "flag", "output": "a.flag"},
                    ],
                    "predicate": None,
                    "pushed_limit": None,
                },
                {
                    "id": "filter",
                    "op": "filter",
                    "input": "scan",
                    "predicate": {"kind": "column", "column": "a.flag"},
                },
            ],
            "root": "filter",
            "output": ["a.k"],
        },
        "stage_graph": None,
    }


class SnapshotTest(unittest.TestCase):
    def test_valid_snapshot_has_inferred_root_schema(self):
        snapshot = parse_snapshot(minimal_snapshot())
        self.assertEqual([(column.name, column.type, column.nullable) for column in snapshot.output_schema()], [
            ("a.k", "Int64", False)
        ])

    def test_legacy_v1_scan_without_pushdowns_defaults_to_none(self):
        value = minimal_snapshot()
        del value["plan"]["nodes"][0]["predicate"]
        del value["plan"]["nodes"][0]["pushed_limit"]
        snapshot = parse_snapshot(value)
        self.assertIsNone(snapshot.plan.nodes[0].predicate)
        self.assertIsNone(snapshot.plan.nodes[0].pushed_limit)

    def test_pushed_scan_predicate_is_strict_typed_and_column_only(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][0]["predicate"] = {
            "kind": "gte",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "Int64", "value": 30},
        }
        value["stage_graph"] = {
            "root_stage": "source",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan", "filter"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": "column",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        snapshot = parse_snapshot(value)
        self.assertEqual(snapshot.plan.nodes[0].predicate.kind, "gte")

        row_source = copy.deepcopy(value)
        row_source["plan"]["nodes"] = [row_source["plan"]["nodes"][0]]
        row_source["plan"]["root"] = "scan"
        row_source["stage_graph"]["stages"][0]["nodes"] = ["scan"]
        row_source["stage_graph"]["stages"][0]["outputs"][0]["node"] = "scan"
        row_source["stage_graph"]["stages"][0]["source_storage"] = "row"
        with self.assertRaisesRegex(SnapshotError, "pushed scan predicate or limit"):
            parse_snapshot(row_source)

        unavailable = copy.deepcopy(value)
        unavailable["plan"]["nodes"][0]["predicate"]["left"]["column"] = "missing"
        with self.assertRaisesRegex(SnapshotError, "column 'missing' is not available"):
            parse_snapshot(unavailable)

        non_boolean = copy.deepcopy(value)
        non_boolean["plan"]["nodes"][0]["predicate"] = {
            "kind": "column",
            "column": "a.k",
        }
        with self.assertRaisesRegex(SnapshotError, "scan predicate must be Boolean"):
            parse_snapshot(non_boolean)

    def test_ordered_comparison_requires_matching_integer_types(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "lt",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "Uint64", "value": 1},
        }
        with self.assertRaisesRegex(SnapshotError, "comparison type mismatch"):
            parse_snapshot(value)

        value["schema"]["tables"][0]["columns"][0]["type"] = "String"
        value["plan"]["nodes"][1]["predicate"]["right"] = {
            "kind": "literal",
            "type": "String",
            "value": "1",
        }
        with self.assertRaisesRegex(SnapshotError, "lt requires integer arguments"):
            parse_snapshot(value)

        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "gte",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "Int64", "value": 1},
            "null_safe": True,
        }
        with self.assertRaisesRegex(SnapshotError, "valid only for equality"):
            parse_snapshot(value)

    def test_unknown_field_is_rejected(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][0]["estimate"] = 42
        with self.assertRaisesRegex(SnapshotError, "unknown fields: estimate"):
            parse_snapshot(value)

    def test_unavailable_expression_column_is_rejected(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {"kind": "column", "column": "missing"}
        with self.assertRaisesRegex(SnapshotError, "column 'missing' is not available"):
            parse_snapshot(value)

    def test_expression_types_are_checked(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][1]["predicate"] = {
            "kind": "eq",
            "left": {"kind": "column", "column": "a.k"},
            "right": {"kind": "literal", "type": "String", "value": "1"},
        }
        with self.assertRaisesRegex(SnapshotError, "equality type mismatch"):
            parse_snapshot(value)

    def test_abstract_scalar_names_are_rejected(self):
        value = minimal_snapshot()
        value["schema"]["tables"][0]["columns"][0]["type"] = "int"
        with self.assertRaisesRegex(SnapshotError, "unsupported scalar type 'int'"):
            parse_snapshot(value)

    def test_aggregate_contract_is_strict_and_typed(self):
        value = minimal_snapshot()
        value["plan"]["nodes"][-1] = {
            "id": "aggregate",
            "op": "aggregate",
            "input": "scan",
            "keys": [],
            "aggregates": [
                {
                    "input": "a.k",
                    "function": "count",
                    "output": "result",
                    "type": "Uint64",
                    "nullable": False,
                    "distinct": False,
                    "unwrap": False,
                }
            ],
            "phase": "undefined",
            "distinct_all": False,
        }
        value["plan"]["root"] = "aggregate"
        value["plan"]["output"] = ["result"]
        snapshot = parse_snapshot(value)
        self.assertEqual(
            [(column.name, column.type, column.nullable) for column in snapshot.output_schema()],
            [("result", "Uint64", False)],
        )

        wrong_type = copy.deepcopy(value)
        wrong_type["plan"]["nodes"][-1]["aggregates"][0]["type"] = "Int64"
        with self.assertRaisesRegex(SnapshotError, "count output must"):
            parse_snapshot(wrong_type)

        unknown_field = copy.deepcopy(value)
        unknown_field["plan"]["nodes"][-1]["aggregates"][0]["state"] = "opaque"
        with self.assertRaisesRegex(SnapshotError, "unknown fields: state"):
            parse_snapshot(unknown_field)

        bad_phase = copy.deepcopy(value)
        bad_phase["plan"]["nodes"][-1]["phase"] = "partial"
        with self.assertRaisesRegex(SnapshotError, "unsupported aggregate phase"):
            parse_snapshot(bad_phase)

    def test_incomplete_stage_graph_is_rejected(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {"stages": []}
        with self.assertRaisesRegex(SnapshotError, "missing fields: assumptions, edges, root_stage"):
            parse_snapshot(value)

    def test_strict_stage_graph_is_accepted(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {
            "root_stage": "s0",
            "stages": [
                {
                    "id": "s0",
                    "nodes": ["scan", "filter"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": "column",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        snapshot = parse_snapshot(value)
        self.assertEqual(snapshot.stage_graph.root_stage, "s0")

    def test_row_storage_source_stage_must_contain_only_the_scan(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {
            "root_stage": "s0",
            "stages": [
                {
                    "id": "s0",
                    "nodes": ["scan", "filter"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": "row",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "row-storage source stage.*only its scan"):
            parse_snapshot(value)

    def test_repeated_shuffle_keys_preserve_order(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {
            "root_stage": "consumer",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "scan"}],
                    "source_storage": "row",
                },
                {
                    "id": "consumer",
                    "nodes": ["filter"],
                    "inputs": ["scan"],
                    "outputs": [{"index": 0, "node": "filter"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "edge",
                    "producer": "source",
                    "consumer": "consumer",
                    "occurrence": 0,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "hash_shuffle",
                    "keys": ["a.k", "a.k"],
                    "hash_function": "HashV1",
                    "use_spilling": False,
                }
            ],
            "assumptions": [],
        }
        snapshot = parse_snapshot(value)
        self.assertEqual(snapshot.stage_graph.edges[0].keys, ("a.k", "a.k"))

        value["stage_graph"]["edges"][0]["hash_function"] = "ColumnShardHashV1"
        with self.assertRaisesRegex(SnapshotError, "unsupported hash function"):
            parse_snapshot(value)

    def test_occurrences_follow_effective_consumer_order(self):
        value = copy.deepcopy(minimal_snapshot())
        value["plan"] = {
            "nodes": [
                value["plan"]["nodes"][0],
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                    ],
                    "output": ["u.k"],
                },
            ],
            "root": "union",
            "output": ["u.k"],
        }
        value["stage_graph"] = {
            "root_stage": "consumer",
            "stages": [
                {
                    "id": "source",
                    "nodes": ["scan"],
                    "inputs": [],
                    "outputs": [
                        {"index": 0, "node": "scan"},
                        {"index": 1, "node": "scan"},
                    ],
                    "source_storage": "row",
                },
                {
                    "id": "consumer",
                    "nodes": ["union"],
                    "inputs": ["scan", "scan"],
                    "outputs": [{"index": 0, "node": "union"}],
                    "source_storage": None,
                },
            ],
            "edges": [
                {
                    "id": "first",
                    "producer": "source",
                    "consumer": "consumer",
                    "occurrence": 1,
                    "producer_output": 0,
                    "consumer_input": 0,
                    "kind": "union_all",
                    "parallel": True,
                },
                {
                    "id": "second",
                    "producer": "source",
                    "consumer": "consumer",
                    "occurrence": 0,
                    "producer_output": 1,
                    "consumer_input": 1,
                    "kind": "union_all",
                    "parallel": True,
                },
            ],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "effective consumer input order"):
            parse_snapshot(value)

    def test_union_inputs_must_cross_stage_boundaries(self):
        value = copy.deepcopy(minimal_snapshot())
        value["plan"] = {
            "nodes": [
                value["plan"]["nodes"][0],
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                    ],
                    "output": ["u.k"],
                },
            ],
            "root": "union",
            "output": ["u.k"],
        }
        value["stage_graph"] = {
            "root_stage": "stage",
            "stages": [
                {
                    "id": "stage",
                    "nodes": ["scan", "union"],
                    "inputs": [],
                    "outputs": [{"index": 0, "node": "union"}],
                    "source_storage": "row",
                }
            ],
            "edges": [],
            "assumptions": [],
        }
        with self.assertRaisesRegex(SnapshotError, "must cross stage boundaries"):
            parse_snapshot(value)

    def test_union_all_is_exactly_binary(self):
        value = copy.deepcopy(minimal_snapshot())
        value["plan"] = {
            "nodes": [
                value["plan"]["nodes"][0],
                {
                    "id": "union",
                    "op": "union_all",
                    "inputs": [
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                        {"node": "scan", "columns": ["a.k"]},
                    ],
                    "output": ["u.k"],
                },
            ],
            "root": "union",
            "output": ["u.k"],
        }
        with self.assertRaisesRegex(SnapshotError, "requires exactly two inputs"):
            parse_snapshot(value)

    def test_boolean_is_not_accepted_as_version_one(self):
        value = minimal_snapshot()
        value["version"] = True
        with self.assertRaisesRegex(SnapshotError, "expected version 1"):
            parse_snapshot(value)

    def test_disconnected_nodes_are_rejected(self):
        value = minimal_snapshot()
        value["plan"]["nodes"].append(
            {
                "id": "unused",
                "op": "scan",
                "table": "A",
                "columns": [{"source": "k", "output": "unused.k"}],
                "pushed_limit": None,
            }
        )
        with self.assertRaisesRegex(SnapshotError, "not reachable from the root: unused"):
            parse_snapshot(value)


if __name__ == "__main__":
    unittest.main()
