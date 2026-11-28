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

    def test_reproduction_omits_runner_blacklist(self):
        flags = ['--build=release', '-DHARDENING=yes', '-DHARDENING=no', '-DCFLAGS=one two']
        for blacklist in [
            ['--test-blacklist-path', '/runner/temp/blacklist.yaml'],
            ['--test-blacklist-path=/runner/temp/blacklist.yaml'],
        ]:
            with self.subTest(blacklist=blacklist):
                self.assertEqual(
                    shlex.split(build_profile.portable_reproduction_args(flags + blacklist + ['--thinlto'])),
                    flags + ['--thinlto'],
                )

    def test_reproduction_preserves_all_profile_definitions(self):
        profile = ['--build=release', '-DALLOCATOR=MIM', '-DHARDENING=yes', '-DCFLAGS=-one -two']
        overrides = ['-D', 'HARDENING=no', '--test-env', 'TOKEN=secret', '-DSECRET=hidden']
        self.assertEqual(
            shlex.split(build_profile.portable_reproduction_args(overrides, profile)),
            profile + ['-DHARDENING=no'],
        )

    def test_reproduction_overrides_profile_definitions(self):
        profile = ['--build=release', '-DDEFAULT_ALLOCATOR=MIM']
        for overrides, expected in [
            (['-DDEFAULT_ALLOCATOR=SYSTEM'], ['-DDEFAULT_ALLOCATOR=SYSTEM']),
            (['-D', 'DEFAULT_ALLOCATOR=SYSTEM'], ['-DDEFAULT_ALLOCATOR=SYSTEM']),
            (['-D', 'DEFAULT_ALLOCATOR'], ['-DDEFAULT_ALLOCATOR']),
            (['-DDEFAULT_ALLOCATOR=SYSTEM', '-DDEFAULT_ALLOCATOR=MIM'],
             ['-DDEFAULT_ALLOCATOR=SYSTEM', '-DDEFAULT_ALLOCATOR=MIM']),
        ]:
            with self.subTest(overrides=overrides):
                self.assertEqual(
                    shlex.split(build_profile.portable_reproduction_args(
                        overrides + ['-DOTHER_TOKEN=secret', '--test-env=TOKEN=secret'], profile)),
                    profile + expected,
                )

    def test_reproduction_definition_forms_and_order(self):
        args = ['-DHARDENING=yes', '-D', 'HARDENING=no',
                '-D', 'CFLAGS=-one -two', '-DCFLAGS=-three',
                '-D', 'DEBUGINFO_LINES_ONLY', '-D', 'SECRET=hidden']
        self.assertEqual(shlex.split(build_profile.portable_reproduction_args(args)),
                         ['-DHARDENING=yes', '-DHARDENING=no', '-DCFLAGS=-one -two',
                          '-DCFLAGS=-three', '-DDEBUGINFO_LINES_ONLY'])
        self.assertEqual(build_profile.portable_reproduction_args(['-D']), '')

    def test_reproduction_value_option_forms(self):
        args = ['--build=release', '--build', 'debug', '--sanitize=address',
                '--sanitize', 'thread', '--thinlto', '--no-thinlto']
        self.assertEqual(shlex.split(build_profile.portable_reproduction_args(args)), args)

    def test_public_reproduction_excludes_environment_and_filters(self):
        args = ['--test-env', 'TOKEN=secret', '--test-env=OTHER=secret',
                '--test-filter', 'Suite::*', '-FCase::*', '--test-filter=Other::*',
                '-DSECRET=secret', '--build', 'release', '--sanitize=address', '-DHARDENING=no']
        self.assertEqual(shlex.split(build_profile.portable_reproduction_args(args)),
                         ['--build', 'release', '--sanitize=address', '-DHARDENING=no'])

    def test_workflow_presets_resolve(self):
        # Public presets used by the workflow matrices and choice inputs.
        presets = ['release', 'debug', 'relwithdebinfo', 'profile', 'release-asan', 'release-tsan', 'release-msan']
        with patch.object(build_profile, 'PROFILE_PATH', PROFILES):
            for name in presets:
                with self.subTest(name=name):
                    resolved = build_profile.resolve_profile(name)
                    self.assertTrue(resolved['args'])
                    output = Path(self.tmp.name) / name
                    subprocess.run([sys.executable, str(SCRIPT), name, '--output-dir', str(output)],
                                   cwd=ROOT, check=True, capture_output=True, text=True)
                    self.assertEqual((output / 'args').read_text().splitlines(), resolved['args'])
                    self.assertEqual((output / 'sanitize').read_text(), resolved['sanitize'] + '\n')
                    expected = {'release-asan': 'address', 'release-tsan': 'thread', 'release-msan': 'memory'}
                    if name in expected:
                        self.assertEqual(resolved['sanitize'], expected[name])
                        self.assertIn('--sanitize=' + expected[name], resolved['args'])

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
