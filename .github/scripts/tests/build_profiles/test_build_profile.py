from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import build_profile
import graph_compare

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / '.github/scripts/build_profile.py'
PROFILES = ROOT / 'ydb/ci/build_profiles.yaml'


class BuildProfilesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'profiles.yaml'
        self.patch = patch.object(build_profile, 'PROFILE_PATH', self.path)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_yaml_anchor_and_whitespace(self):
        self.path.write_text(
            'release: &release\n  build_type: release\n  definition_flags:\n    CFLAGS: "-one -two"\ndebug:\n  <<: *release\n  build_type: debug\n'
        )
        self.assertEqual(
            build_profile.resolve_profile('debug'), {'args': ['--build=debug', '-DCFLAGS=-one -two'], 'sanitize': ''}
        )

    def test_missing_file_uses_fallback(self):
        self.assertEqual(
            build_profile.resolve_profile('release', {'release': {'build_type': 'release'}})['args'],
            ['--build=release'],
        )

    def test_missing_profile_uses_fallback(self):
        self.path.write_text('old:\n  build_type: debug\n')
        self.assertEqual(
            build_profile.resolve_profile('new', {'new': {'build_type': 'release'}})['args'], ['--build=release']
        )

    def test_existing_profile_wins(self):
        self.path.write_text('release:\n  build_type: debug\n')
        self.assertEqual(
            build_profile.resolve_profile('release', {'release': {'build_type': 'release'}})['args'], ['--build=debug']
        )

    def test_unknown_profile_rejected(self):
        self.path.write_text('release:\n  build_type: release\n')
        with self.assertRaisesRegex(ValueError, 'Unknown build profile'):
            build_profile.resolve_profile('unknown', {})

    def test_workflow_presets_resolve(self):
        # Public presets used by the workflow matrices and choice inputs.
        presets = ['release', 'debug', 'relwithdebinfo', 'release-asan', 'release-tsan', 'release-msan']
        with patch.object(build_profile, 'PROFILE_PATH', PROFILES):
            for name in presets:
                with self.subTest(name=name):
                    self.assertTrue(build_profile.resolve_profile(name)['args'])

    def test_cli_reports_configuration_error(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), 'missing', '--output-dir', 'out'],
            cwd=self.tmp.name, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn('error:', result.stderr)
        self.assertNotIn('Traceback', result.stderr)

    def test_build_type_required(self):
        self.path.write_text('release: {}\n')
        with self.assertRaisesRegex(ValueError, 'build_type'):
            build_profile.resolve_profile('release')

    def test_summary_survives_missing_profiles(self):
        with self.assertLogs(level='WARNING'):
            self.assertEqual(build_profile.reproduction_args('release'), '')

    def test_summary_survives_invalid_yaml(self):
        self.path.write_text('release: [')
        with self.assertLogs(level='WARNING'):
            self.assertEqual(build_profile.reproduction_args('release'), '')

    def test_invalid_field_types_rejected(self):
        cases = [
            ('definition_flags: [one]', 'definition_flags'),
            ('definition_flags: null', 'definition_flags'),
            ('definition_flags: {HARDENING: yes}', 'string definition_flags'),
            ('definition_flags: {1: "yes"}', 'string definition_flags'),
            ('sanitize: yes', 'string sanitize'),
            ('thinlto: "no"', 'boolean thinlto'),
        ]
        for field, message in cases:
            with self.subTest(field=field):
                self.path.write_text('release:\n  build_type: release\n  ' + field + '\n')
                with self.assertRaisesRegex(ValueError, message):
                    build_profile.resolve_profile('release')
                with self.assertLogs(level='WARNING'):
                    self.assertEqual(build_profile.reproduction_args('release'), '')

    def test_summary_command_quotes_flags(self):
        self.path.write_text('release:\n  build_type: release\n  definition_flags:\n    CFLAGS: "-one -two"\n')
        self.assertIn('-DCFLAGS=-one -two', shlex.split(build_profile.reproduction_args('release')))

    def test_cli_optional_sanitizer_and_args_file(self):
        root = Path(self.tmp.name)
        profiles = root / 'ydb/ci/build_profiles.yaml'
        profiles.parent.mkdir(parents=True)
        profiles.write_text('release:\n  build_type: release\n  definition_flags:\n    CFLAGS: "-one -two"\n')
        subprocess.run([sys.executable, str(SCRIPT), 'release', '--output-dir', 'out'], cwd=root, check=True)
        self.assertEqual((root / 'out/sanitize').read_text(), '\n')
        self.assertEqual((root / 'out/args').read_text().splitlines(), ['--build=release', '-DCFLAGS=-one -two'])

    def test_cli_rejects_newline(self):
        root = Path(self.tmp.name)
        profiles = root / 'ydb/ci/build_profiles.yaml'
        profiles.parent.mkdir(parents=True)
        profiles.write_text('release:\n  build_type: release\n  definition_flags:\n    CFLAGS: "one\\ntwo"\n')
        result = subprocess.run(
            [sys.executable, str(SCRIPT), 'release', '--output-dir', 'out'], cwd=root, capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 2)
        self.assertNotIn('Traceback', result.stderr)
        self.assertIn('must not contain line breaks', result.stderr)

    def test_graph_command_preserves_user_overrides(self):
        profiles = {'release': {'build_type': 'release', 'definition_flags': {'HARDENING': 'yes'}}}
        with patch.object(graph_compare, 'load_profiles', return_value=profiles), patch.object(
            graph_compare, 'resolve_profile', return_value={'args': ['--build=release', '-DHARDENING=yes']}
        ), patch.object(graph_compare, 'exec') as execute:
            graph_compare.main('./ya make -DHARDENING=no', 'graph.json', 'context.json', 'base', 'head', 'release')
        for call in (execute.call_args_list[1], execute.call_args_list[3]):
            args = call.args[0]
            self.assertLess(args.index('-DHARDENING=yes'), args.index('-DHARDENING=no'))

    def test_debug_keeps_full_debug_information(self):
        profiles = yaml.safe_load(PROFILES.read_text())
        self.assertNotIn('DEBUGINFO_LINES_ONLY', profiles['debug']['definition_flags'])
        self.assertNotIn('HARDENING', profiles['debug']['definition_flags'])
        self.assertNotIn('asan', profiles)
        self.assertNotIn('tsan', profiles)
        for profile in profiles.values():
            self.assertIn('-Wno-unknown-argument', profile['definition_flags']['CFLAGS'])


if __name__ == '__main__':
    unittest.main()
