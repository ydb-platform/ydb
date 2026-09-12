import copy
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from ydb.core.kqp.opt.rbo.verification.inspector import cli, formulas
from ydb.core.kqp.opt.rbo.verification.inspector.plan import InspectionError
from ydb.core.kqp.opt.rbo.verification.rbo_verifier import floating, ir, smt
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.scalar import (
    DecimalAverageState, DecimalSumState, Value,
)
from ydb.core.kqp.opt.rbo.verification.rbo_verifier.verify import build_problem


def _sources():
    before = {
        "format": ir.FORMAT, "version": ir.VERSION,
        "schema": {"tables": [{
            "name": "A", "columns": [{"name": "x", "type": "Int64", "nullable": True}],
            "unique_keys": [],
        }]},
        "plan": {
            "nodes": [{
                "id": "scan", "op": "scan", "table": "A",
                "columns": [{"source": "x", "output": "x"}],
                "predicate": None, "pushed_limit": None,
            }],
            "root": "scan", "output": ["x"], "subplans": [],
        },
        "stage_graph": None,
    }
    after = copy.deepcopy(before)
    after["semantic_mode"] = ir.BINARY64_SEMANTIC_MODE
    after["stage_graph"] = {
        "root_stage": "s", "stages": [{
            "id": "s", "nodes": ["scan"], "inputs": [],
            "outputs": [{"index": 0, "node": "scan"}], "source_storage": "column",
        }], "edges": [], "assumptions": [],
    }
    return tuple(json.dumps(value).encode() for value in (before, after))


def _rebuild(nodes):
    terms = {}
    for node in nodes:
        atom = node.get("atom")
        if node["op"] == "int":
            atom = int(atom)
        terms[node["id"]] = smt.Term(
            node["sort"], node["op"], tuple(terms[ref] for ref in node["args"]), atom,
        )
    return terms


class FormulaExportTest(unittest.TestCase):
    def test_exact_ast_keeps_sharing_quantifier_scope_and_declarations(self):
        script = smt.Script()
        record = script.fresh_product_sort("row", (smt.INT, smt.BOOL))
        function = script.fresh_defined_function(
            "row payload", (record,), smt.INT, lambda parameters: record.select(parameters[0], 0),
        )
        x = script.fresh_constant("outer witness", smt.INT)
        local = smt.symbol("local", smt.INT)
        shared = smt.add(x, local)
        large = smt.int_value(2**80 + 1)
        body = smt.and_(smt.lt(shared, large), smt.eq(function(record.pack(shared, smt.TRUE)), x))
        root = smt.forall((x,), smt.exists((local,), body))
        dag = formulas.TermDag(script.declarations)
        declarations = [dag.declaration(item) for item in script.declarations]
        reference = dag.ref(root)
        self.assertEqual(_rebuild(dag.nodes)[reference].render(), root.render())
        self.assertEqual(dag.ref(shared), dag.ref(shared))
        self.assertEqual(sum(node["op"] == "+" for node in dag.nodes), 1)
        self.assertEqual(dag.nodes[int(dag.ref(large)[1:])]["atom"], str(2**80 + 1))
        self.assertEqual(declarations[0]["fields"][0], {"selector": record.selectors[0].name, "sort": "Int"})
        self.assertEqual(declarations[1]["kind"], "definition")
        self.assertEqual(_rebuild(dag.nodes)[declarations[1]["body"]].render(), script.declarations[1].body.render())

    def test_observed_export_is_deterministic_bound_and_does_not_change_formula(self):
        before, after = _sources()
        export = formulas.prepare_formulas(before, after, 2)
        document = export.document
        self.assertEqual(document, formulas.prepare_formulas(before, after, 2).document)
        self.assertEqual(document["inputs"]["before_sha256"], hashlib.sha256(before).hexdigest())
        self.assertEqual(document["semantic_mode"], ir.BINARY64_SEMANTIC_MODE)
        self.assertIsNone(document["semantic_modes"]["before"])
        self.assertEqual(document["status"], "FORMULAS_GENERATED")
        plain = build_problem(ir.parse_snapshot(json.loads(before)), ir.parse_snapshot(json.loads(after)), 2)
        self.assertEqual(export.problem.formula(), plain.formula())
        reconstructed = _rebuild(document["terms"])
        self.assertEqual(
            [reconstructed[ref].render() for ref in document["assertions"]],
            [term.render() for term in export.problem.script.assertions],
        )
        self.assertEqual(len(document["before"]["operators"]), 1)
        self.assertEqual(len(document["after"]["operators"]), 2)
        row = document["before"]["operators"][0]["result"]["outcomes"][0]["rows"][0]
        self.assertEqual(reconstructed[row["present"]].sort, "Bool")
        self.assertEqual(reconstructed[row["values"][0]["value"]].sort, "Int")
        self.assertEqual(reconstructed[row["values"][0]["is_null"]].sort, "Bool")
        changed = formulas.prepare_formulas(before + b"\n", after, 2).document
        self.assertNotEqual(changed["inputs"]["before_sha256"], document["inputs"]["before_sha256"])
        self.assertEqual(changed["terms"], document["terms"])

    def test_hidden_state_is_explicitly_distinguished_from_proof_metadata(self):
        dag = formulas.TermDag(())
        cell = Value(
            "Decimal(35,2)", smt.FALSE, smt.ONE,
            average_metadata=DecimalAverageState("Decimal(35,2)", smt.ONE, smt.ONE, 2**80, 2),
            decimal_sum_state=DecimalSumState("Decimal(35,2)", smt.TRUE, smt.FALSE, smt.FALSE, smt.FALSE, smt.ONE, 2**80),
        )
        metadata = dag.cell(cell)["metadata"]
        self.assertEqual([item["role"] for item in metadata], ["physical_state", "proof_metadata"])
        self.assertEqual(metadata[1]["bounds"]["finite_abs"], str(2**80))
        variance = dag.cell(Value(
            "Double", smt.FALSE, smt.ONE,
            binary64_state=floating.VarianceState(floating.ZERO, floating.ONE, floating.ZERO),
        ))["metadata"][0]
        self.assertEqual(variance["role"], "physical_state")
        self.assertEqual(set(variance["terms"]), {"mean", "count", "m2"})

    def test_limits_and_unsupported_input_fail_without_partial_cli_output(self):
        with self.assertRaisesRegex(InspectionError, "unknown SMT operation"):
            formulas.TermDag(()).ref(smt.Term("Bool", "future"))
        before, after = _sources()
        with self.assertRaisesRegex(InspectionError, "duplicate snapshot JSON key"):
            formulas.prepare_formulas(b'{"format": 1, "format": 2}', after, 2)
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("before.json", "after.json")]
            for path, source in zip(paths, (before, after)):
                path.write_bytes(source)
            output, errors = io.StringIO(), io.StringIO()
            with mock.patch.object(formulas, "MAX_FORMULA_TERMS", 1), redirect_stdout(output), redirect_stderr(errors):
                code = cli.main(["formulas", *(str(path) for path in paths)])
            self.assertEqual(code, 2)
            self.assertEqual(output.getvalue(), "")
            self.assertIn("term export limit", errors.getvalue())
            with redirect_stdout(output):
                code = cli.main(["formulas", *(str(path) for path in paths)])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())["status"], "FORMULAS_GENERATED")
