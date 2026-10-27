import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


script = Path(__file__).resolve().parents[4] / '.github/scripts/graph_compare.py'
spec = importlib.util.spec_from_file_location('sdk_snippets_graph_compare', script)
graph_compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(graph_compare)


class NativePreparationTests(unittest.TestCase):
    def test_prepares_each_checkout_before_creating_its_graph(self):
        events = []
        with patch.object(graph_compare, 'exec', side_effect=lambda command: events.append(command)), \
                patch.object(graph_compare, 'prepare_sdk_snippets', side_effect=lambda: events.append('prepare')), \
                patch.dict(os.environ, {'workdir': '/tmp/graphs'}):
            graph_compare.main('./ya make', '/tmp/graph.json', '/tmp/context.json', 'base', 'head')
        self.assertEqual(events[0:3], [
            'git checkout base', 'prepare',
            './ya make ydb --cache-tests --save-graph-to /tmp/graphs/graph_base.json '
            '--save-context-to /tmp/graphs/context_base.json',
        ])
        self.assertEqual(events[3:6], [
            'git checkout head', 'prepare',
            './ya make ydb --cache-tests --save-graph-to /tmp/graphs/graph_head.json '
            '--save-context-to /tmp/graphs/context_head.json',
        ])

    def test_uses_the_preparation_python_for_a_locked_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            previous = Path.cwd()
            try:
                os.chdir(directory)
                docs = Path('ydb/docs')
                docs.mkdir(parents=True)
                (docs / 'sdk-snippets.lock.yaml').write_text('version: 1\n')
                with patch.object(graph_compare.subprocess, 'run') as run, \
                        patch.dict(os.environ, {'SDK_SNIPPETS_PYTHON': '/tmp/python'}):
                    graph_compare.prepare_sdk_snippets()
                run.assert_called_once_with([
                    '/tmp/python', 'ydb/docs/tools/sdk-snippets', 'prepare',
                ], check=True)
            finally:
                os.chdir(previous)

    def test_removes_staging_when_base_has_no_snippet_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            previous = Path.cwd()
            try:
                os.chdir(directory)
                staging = Path('ydb/docs/.generated/sdk-snippets')
                staging.mkdir(parents=True)
                (staging / 'head-only.cpp').write_text('code')
                sibling = staging.parent / 'other'
                sibling.write_text('keep')
                graph_compare.prepare_sdk_snippets()
                self.assertFalse(staging.exists())
                self.assertEqual(sibling.read_text(), 'keep')
            finally:
                os.chdir(previous)

    def test_does_not_build_a_graph_after_preparation_fails(self):
        with patch.object(graph_compare, 'exec') as execute, \
                patch.object(graph_compare, 'prepare_sdk_snippets',
                             side_effect=subprocess.CalledProcessError(1, 'prepare')):
            with self.assertRaises(subprocess.CalledProcessError):
                graph_compare.main('./ya make', '/tmp/graph.json', '/tmp/context.json', 'base', 'head')
        execute.assert_called_once_with('git checkout base')
