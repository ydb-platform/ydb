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

    def test_non_null_stage_graph_is_rejected_in_version_one(self):
        value = copy.deepcopy(minimal_snapshot())
        value["stage_graph"] = {"stages": []}
        with self.assertRaisesRegex(SnapshotError, "StageGraph is not implemented"):
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
            }
        )
        with self.assertRaisesRegex(SnapshotError, "not reachable from the root: unused"):
            parse_snapshot(value)


if __name__ == "__main__":
    unittest.main()
