#!/usr/bin/env python3
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'run_spin_verify.py'
SPEC = importlib.util.spec_from_file_location('runner', SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class ClassificationTest(unittest.TestCase):
    def test_complete_zero_errors(self):
        self.assertEqual(RUNNER.classify('errors: 0', 0), 'holds')

    def test_limits_never_prove(self):
        for message in ('max search depth too small', 'Search not completed', 'out of memory', 'Interrupted'):
            self.assertEqual(RUNNER.classify(message + '\nerrors: 0', 0), 'unknown')
        self.assertEqual(RUNNER.classify('errors: 0', 0, timed_out=True), 'unknown')

    def test_tool_failure_is_not_violation(self):
        self.assertEqual(RUNNER.classify('too many processes\nerrors: 0', 0), 'error')
        self.assertEqual(RUNNER.classify('errors: 0', 1), 'error')
        self.assertEqual(RUNNER.classify('assertion violated\nerrors: 1', 0), 'error')

    def test_real_counterexample(self):
        self.assertEqual(RUNNER.classify('assertion violated\nerrors: 1', 0, trail=True), 'violated')

    def test_invalid_arguments(self):
        for value in ('0', '-1'):
            with self.assertRaises(Exception):
                RUNNER.positive(value)


@unittest.skipUnless(shutil.which('spin') and shutil.which('cc'), 'Spin and cc required')
class IntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='spin-runner-test-')
        self.root = Path(self.temp.name)
        self.source = self.root / 'models'
        self.source.mkdir()
        self.model = self.source / 'model.pml'
        self.output = self.root / 'results'

    def tearDown(self):
        self.temp.cleanup()

    def verify(self, text, *flags):
        existing_pan = (self.source / 'pan.c').exists()
        self.model.write_text(text)
        args = RUNNER.parser().parse_args(
            ['--model', str(self.model), '--output-dir', str(self.output), '--timeout', '20'] + list(flags)
        )
        code = RUNNER.run(args)
        manifest = json.loads((self.output / 'manifest.json').read_text())
        self.assertEqual(self.model.read_text(), text)
        self.assertEqual((self.source / 'pan.c').exists(), existing_pan)
        return code, manifest

    def test_safety_and_preserved_source_artifacts(self):
        (self.source / 'pan.c').write_text('original')
        code, manifest = self.verify('init { assert(1); }', '--safety')
        self.assertEqual((self.source / 'pan.c').read_text(), 'original')
        self.assertEqual((code, manifest['status']), (0, 'holds'))
        self.assertIn('model.pml', manifest['inputs_sha256'])

    def test_assertion_counterexample(self):
        code, manifest = self.verify('init { assert(0); }', '--safety')
        self.assertEqual((code, manifest['status']), (1, 'violated'))
        self.assertTrue(manifest['trails'])

    def test_named_ltl_acceptance_cycle(self):
        code, manifest = self.verify(
            'bool done = false;\nbool tick; active proctype Worker() { do :: tick = !tick od }\n'
            'ltl safe_ok { [] (!done) }\nltl live_done { <> done }\n',
            '--ltl',
            'live_done',
        )
        self.assertEqual((code, manifest['status']), (1, 'violated'))
        self.assertIn('-a', manifest['runtime_flags'])
        self.assertIn('live_done', manifest['runtime_flags'])

    def test_depth_limit(self):
        code, manifest = self.verify(
            'byte n; init { do :: n < 20 -> n++ :: else -> break od }', '--safety', '--depth', '2'
        )
        self.assertEqual((code, manifest['status']), (2, 'unknown'))

    def test_missing_claim(self):
        code, manifest = self.verify('bool done; init { done = true }\nltl live_done { <> done }', '--ltl', 'absent')
        self.assertEqual((code, manifest['status']), (2, 'error'))

    def test_parent_include(self):
        nested = self.source / 'nested'
        nested.mkdir()
        self.model = nested / 'model.pml'
        (self.source / 'bounds.h').write_text('#define BOUND 2\n')
        code, manifest = self.verify(
            '#include "../bounds.h"\ninit { assert(BOUND == 2) }', '--safety', '--source-root', str(self.source)
        )
        self.assertEqual((code, manifest['status']), (0, 'holds'))
        self.assertIn('bounds.h', manifest['inputs_sha256'])

    def test_escaping_include_rejected(self):
        header = self.root / 'external.h'
        header.write_text('#define VALUE 1\n')
        for include in ('../external.h', str(header)):
            with self.subTest(include=include):
                model = self.source / 'model.pml'
                model.write_text('#include "{}"\ninit {{ assert(VALUE) }}'.format(include))
                with self.assertRaises(ValueError):
                    RUNNER.validate_includes(model, self.source)

    def test_macro_include_rejected(self):
        self.model.write_text('#define HEADER "missing.h"\n#include HEADER\ninit { skip }')
        with self.assertRaises(ValueError):
            RUNNER.validate_includes(self.model, self.source)

    def test_fair_ltl(self):
        code, manifest = self.verify(
            'bool done; init { done = true }\nltl live_done { <> done }', '--ltl', 'live_done', '--fair'
        )
        self.assertEqual((code, manifest['status']), (0, 'holds'))
        self.assertIn('-f', manifest['runtime_flags'])
        self.assertIn('-DNFAIR=3', manifest['compile_flags'])

    def test_existing_output_refused(self):
        self.model.write_text('init { skip }')
        self.output.mkdir()
        (self.output / 'keep').write_text('original')
        args = RUNNER.parser().parse_args(['--model', str(self.model), '--safety', '--output-dir', str(self.output)])
        with self.assertRaises(FileExistsError):
            RUNNER.run(args)
        self.assertEqual((self.output / 'keep').read_text(), 'original')


if __name__ == '__main__':
    unittest.main()
