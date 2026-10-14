import ast
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / 'format.py'
spec = importlib.util.spec_from_file_location('cpp_format', SCRIPT)
cpp_format = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cpp_format)


class FormatTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.git('init', '-q')
        self.write(cpp_format.AUTOINCLUDES, json.dumps(['shared', 'local', 'disabled']))
        self.write(
            'shared/linters.make.inc',
            '''
# STYLE_CPP() in a comment must not count.
IF (MODULE_LANG == CPP)
    SET(MODULE_COMMON_CONFIGS_DIR config)
    STYLE_CPP(CONFIG_TYPE .clang-format) # shared rules
ENDIF()
''',
        )
        self.write('local/linters.make.inc', 'STYLE_CPP(CONFIG_TYPE .clang-format)\n')
        self.write('disabled/linters.make.inc', '# STYLE_CPP(CONFIG_TYPE .clang-format)\nSTYLE_PYTHON()\n')
        self.write(
            'config/.clang-format', 'BasedOnStyle: Google\nIndentWidth: 4\nAllowShortFunctionsOnASingleLine: None\n'
        )
        self.write(
            'local/.clang-format', 'BasedOnStyle: Google\nIndentWidth: 2\nAllowShortFunctionsOnASingleLine: None\n'
        )

    def write(self, path, content):
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
        return file

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.root), *args], stderr=subprocess.PIPE)

    def copy_checkout(self, parent):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        return shutil.copytree(src=self.root, dst=Path(temp.name) / parent / 'ydb')

    def test_discovery_uses_declared_or_local_config_and_ignores_disabled_roots(self):
        self.assertEqual(
            cpp_format.discover(self.root),
            {
                'shared': self.root / 'config/.clang-format',
                'local': self.root / 'local/.clang-format',
            },
        )

    def test_future_root_is_enabled_by_autoinclude_and_include_only(self):
        self.write('future/linters.make.inc', (self.root / 'shared/linters.make.inc').read_text())
        self.write('future/a.cpp', 'int x;\n')
        self.git('add', '.')
        self.assertNotIn('future', cpp_format.discover(self.root))
        self.write(cpp_format.AUTOINCLUDES, json.dumps(['shared', 'local', 'disabled', 'future']))
        configs = cpp_format.discover(self.root)
        self.assertEqual(configs['future'], self.root / 'config/.clang-format')
        self.assertEqual(cpp_format.selected_files(self.root, configs)['future'], ['future/a.cpp'])

    def test_selection_handles_staged_sources_and_excludes_outputs_links_and_deleted_files(self):
        expected = ['shared/a.cpp', 'shared/with space\n.h', 'shared/subdir/template.ipp']
        for path in expected + ['shared/delete.cpp', 'shared/data.proto', 'shared_extra/a.cpp', 'disabled/a.cpp']:
            self.write(path, 'int x;\n')
        (self.root / 'shared/link.cpp').symlink_to('a.cpp')
        self.git('add', '.')
        self.write('shared/untracked.cpp', 'int y;\n')
        (self.root / 'shared/delete.cpp').unlink()
        selected = cpp_format.selected_files(self.root, cpp_format.discover(self.root))
        self.assertEqual(selected, {'local': [], 'shared': sorted(expected)})

    def test_list_limits_roots_without_resolving_formatter(self):
        self.write('shared/a.cpp', 'int x;\n')
        self.write('local/a.cpp', 'int x;\n')
        self.git('add', '.')
        output = io.StringIO()
        with mock.patch.object(cpp_format, 'ROOT', self.root), mock.patch.object(
            cpp_format, 'resolve_formatter'
        ) as resolve, contextlib.redirect_stdout(output):
            self.assertEqual(cpp_format.main(['--list', '--root', 'shared/']), 0)
        self.assertEqual(output.getvalue(), 'shared/a.cpp\n')
        resolve.assert_not_called()

    def test_selection_respects_native_style_skips(self):
        for path, content in (
            ('shared/contrib/vhost/bio.h', 'struct  Example {};\n'),
            ('shared/vendor/a.cpp', 'int  x;\n'),
            ('shared/generated/a.h', 'int  x;\n'),
            ('shared/no_style.cpp', '// DO_NOT_STYLE\nint  x;\n'),
            ('shared/no_style.h', '# DO_NOT_STYLE\nint  x;\n'),
            ('shared/license.cpp', '// THIS SOFTWARE IS PROVIDED AS IS\nint  x;\n'),
            ('shared/warranty.h', '// WITHOUT WARRANTIES\nint  x;\n'),
            ('shared/regular.cpp', 'int  x;\n'),
            ('shared/contrib/.yandex_meta/adapter.h', 'int  x;\n'),
        ):
            self.write(path, content)
        self.git('add', '.')
        for parent in ('checkout', 'vendor', 'generated', 'contrib', '.yandex_meta', 'devtools/contrib'):
            with self.subTest(parent=parent):
                root = self.copy_checkout(parent=parent)
                selected = cpp_format.selected_files(root, cpp_format.discover(root))
                self.assertEqual(
                    selected['shared'],
                    [
                        'shared/contrib/.yandex_meta/adapter.h',
                        'shared/regular.cpp',
                    ],
                )

    def test_list_respects_no_lint_module_and_keeps_separate_child_modules(self):
        self.write('shared/ya.make', 'LIBRARY()\nEND()\n')
        self.write('shared/regular.cpp', 'int x;\n')
        self.write('shared/codegen/ya.make', 'LIBRARY()\nNO_LINT()\nEND()\n')
        for path in ('shared/codegen/adapter.cpp', 'shared/codegen/gen-cpp/types.cpp', 'shared/codegen/gen-cpp/types.h'):
            self.write(path, '// Autogenerated by Thrift Compiler\n')
        self.write('shared/codegen/handwritten/ya.make', 'LIBRARY()\nEND()\n')
        self.write('shared/codegen/handwritten/client.cpp', 'int x;\n')
        self.write('local/regular.cpp', 'int x;\n')
        self.git('add', '.')
        output = io.StringIO()
        with mock.patch.object(cpp_format, 'ROOT', self.root), mock.patch.object(
            cpp_format, 'resolve_formatter'
        ) as resolve, contextlib.redirect_stdout(output):
            self.assertEqual(cpp_format.main(['--list']), 0)
        self.assertEqual(
            output.getvalue().splitlines(),
            ['local/regular.cpp', 'shared/codegen/handwritten/client.cpp', 'shared/regular.cpp'],
        )
        resolve.assert_not_called()

    def test_only_unconditional_cpp_no_lint_disables_local_selection(self):
        self.write('shared/a.cpp', 'int x;\n')
        self.git('add', '.')
        for setting, disabled in (
            ('NO_LINT()', True),
            ('NO_LINT( # generated sources\n)', True),
            ('# NO_LINT()', False),
            ('NO_LINT(ktlint)', False),
            ('IF (AUTOCHECK)\nNO_LINT()\nENDIF()', False),
            ('IF ((AUTOCHECK))\nNO_LINT()\nENDIF()', False),
            ('IF (OS_LINUX)\nIF (AUTOCHECK)\nNO_LINT()\nENDIF()\nENDIF()', False),
            ('IF (AUTOCHECK)\nNO_LINT(ktlint)\nENDIF()\nNO_LINT()', True),
        ):
            with self.subTest(setting=setting):
                self.write('shared/ya.make', f'LIBRARY()\n{setting}\nEND()\n')
                selected = cpp_format.selected_files(self.root, cpp_format.discover(self.root))
                self.assertEqual(selected['shared'], [] if disabled else ['shared/a.cpp'])

    def test_native_top_level_directory_skips(self):
        for folder in ('vendor', 'generated', 'contrib'):
            with self.subTest(folder=folder):
                file = self.write(path=f'{folder}/example.cpp', content='int  x;\n')
                self.assertTrue(cpp_format.skip_style(self.root, file))

    def test_native_devtools_contrib_exception(self):
        file = self.write('devtools/contrib/example.cpp', 'int  x;\n')
        self.assertFalse(cpp_format.skip_style(self.root, file))
        nested = self.write('devtools/contrib/project/contrib/example.cpp', 'int  x;\n')
        self.assertTrue(cpp_format.skip_style(self.root, nested))

    def test_unknown_root_is_an_error(self):
        with mock.patch.object(cpp_format, 'ROOT', self.root), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                cpp_format.main(['--fix', '--root', 'not-enabled'])
        self.assertEqual(error.exception.code, 2)

    def test_unsupported_conditions_and_missing_configs_fail_before_any_edits(self):
        self.write('shared/a.cpp', 'int  x;\n')
        self.git('add', '.')
        for include in (
            'IF (OS_LINUX)\nSTYLE_CPP(CONFIG_TYPE .clang-format)\nENDIF()\n',
            'SET(MODULE_COMMON_CONFIGS_DIR missing)\nSTYLE_CPP(CONFIG_TYPE .clang-format)\n',
        ):
            with self.subTest(include=include):
                self.write('local/linters.make.inc', include)
                with mock.patch.object(cpp_format, 'ROOT', self.root), mock.patch.object(
                    cpp_format, 'format_files'
                ) as formatter:
                    with self.assertRaises(ValueError):
                        cpp_format.main(['--fix'])
                formatter.assert_not_called()

    def test_overlapping_roots_and_external_config_paths_are_rejected(self):
        self.write(cpp_format.AUTOINCLUDES, json.dumps(['shared', 'shared/nested']))
        with self.assertRaisesRegex(ValueError, 'Overlapping'):
            cpp_format.discover(self.root)
        self.write(cpp_format.AUTOINCLUDES, json.dumps(['shared']))
        self.write(
            'shared/linters.make.inc',
            'SET(MODULE_COMMON_CONFIGS_DIR ../outside)\nSTYLE_CPP(CONFIG_TYPE .clang-format)\n',
        )
        with self.assertRaises(ValueError):
            cpp_format.discover(self.root)

    def test_source_extensions_match_native_style_check(self):
        module = ast.parse((cpp_format.ROOT / 'build/plugins/lib/test_const/__init__.py').read_text())
        extensions = set()
        for node in module.body:
            if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id in ('STYLE_CPP_SOURCE_EXTS', 'STYLE_CPP_HEADER_EXTS')
                for target in node.targets
            ):
                extensions.update(ast.literal_eval(node.value))
        self.assertEqual(cpp_format.EXTENSIONS, extensions)

    def test_fix_progress_counts_actual_changes_across_roots_and_batches(self):
        groups = {'local': ['local/a.cpp'], 'shared': [f'shared/{i:02}.cpp' for i in range(33)]}
        changed = {'shared/00.cpp', 'shared/32.cpp'}
        for fail in (False, True):
            with self.subTest(failed_batch=fail):
                for files in groups.values():
                    for path in files:
                        self.write(path, 'int  x;\n')

                def run(command, cwd):
                    self.assertEqual(command[2], '-i')
                    batch = [Path(path) for path in command[3:]]
                    for path in batch:
                        if path.relative_to(self.root).as_posix() in changed:
                            path.write_text('int x;\n')
                    code = int(fail and self.root / 'shared/00.cpp' in batch)
                    return subprocess.CompletedProcess(command, code)

                output = io.StringIO()
                with mock.patch.object(cpp_format, 'ROOT', self.root), mock.patch.object(
                    cpp_format, 'selected_files', return_value=groups
                ), mock.patch.object(cpp_format, 'resolve_formatter', return_value='mock-clang-format'), mock.patch.object(
                    cpp_format.subprocess, 'run', side_effect=run
                ) as formatter, contextlib.redirect_stdout(output):
                    self.assertEqual(cpp_format.main(['--fix']), int(fail))
                self.assertEqual(formatter.call_count, 3)
                self.assertIn(
                    'Checked 32 / 33 files in shared; formatted 1 (overall 33 / 34 checked, 1 formatted)',
                    output.getvalue(),
                )
                self.assertIn(
                    'Checked 33 / 33 files in shared; formatted 2 (overall 34 / 34 checked, 2 formatted)',
                    output.getvalue(),
                )
                status = 'Finished with errors' if fail else 'Finished'
                self.assertTrue(output.getvalue().endswith(f'{status}: 2 formatted / 34 checked files.\n'))
                self.assertEqual((self.root / 'local/a.cpp').read_text(), 'int  x;\n')

    def test_check_progress_preserves_files_and_reports_failures(self):
        file = self.write('shared/a.cpp', 'int  x;\n')
        output = io.StringIO()
        with mock.patch.object(cpp_format, 'ROOT', self.root), mock.patch.object(
            cpp_format, 'selected_files', return_value={'shared': ['shared/a.cpp']}
        ), mock.patch.object(cpp_format, 'resolve_formatter', return_value='mock-clang-format'), mock.patch.object(
            cpp_format.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1)
        ) as formatter, contextlib.redirect_stdout(output):
            self.assertEqual(cpp_format.main(['--check']), 1)
        self.assertNotIn('-i', formatter.call_args.args[0])
        self.assertIn('--dry-run', formatter.call_args.args[0])
        self.assertEqual(file.read_text(), 'int  x;\n')
        self.assertIn('Checked 1 / 1 files in shared (overall 1 / 1 checked)', output.getvalue())
        self.assertTrue(output.getvalue().endswith('Finished with errors: 1 / 1 files checked.\n'))

    @unittest.skipUnless(os.environ.get('YDB_CLANG_FORMAT'), 'set YDB_CLANG_FORMAT for real formatter coverage')
    def test_real_formatter_checks_fixes_and_preserves_per_root_styles(self):
        self.root = self.copy_checkout(parent='vendor/contrib/generated')
        original = 'int  main(){return 0;}\n'
        shared = self.write('shared/a.cpp', original)
        local = self.write('local/a.cpp', original)
        disabled = self.write('disabled/a.cpp', original)
        vendored = self.write('shared/contrib/a.cpp', original)
        no_style = self.write('shared/no_style.cpp', '// DO_NOT_STYLE\n' + original)
        self.git('add', '.')
        with mock.patch.object(cpp_format, 'ROOT', self.root), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cpp_format.main(['--check']), 1)
            self.assertEqual(shared.read_text(), original)
            self.assertEqual(local.read_text(), original)
            self.assertEqual(cpp_format.main(['--fix']), 0)
            self.assertIn('\n    return 0;', shared.read_text())
            self.assertIn('\n  return 0;', local.read_text())
            self.assertEqual(disabled.read_text(), original)
            self.assertEqual(vendored.read_text(), original)
            self.assertEqual(no_style.read_text(), '// DO_NOT_STYLE\n' + original)
            fixed = shared.read_bytes(), local.read_bytes()
            self.assertEqual(cpp_format.main(['--check']), 0)
            self.assertEqual(cpp_format.main(['--fix']), 0)
            self.assertEqual((shared.read_bytes(), local.read_bytes()), fixed)


if __name__ == '__main__':
    unittest.main()
