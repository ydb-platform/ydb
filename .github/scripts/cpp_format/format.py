#!/usr/bin/env python3
"""Format tracked C/C++ files in folders opted in through Ya autoincludes."""

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
AUTOINCLUDES = Path('build/internal/conf/autoincludes.json')
EXTENSIONS = {'.c', '.C', '.cc', '.cpp', '.cxx', '.h', '.H', '.hh', '.hpp', '.hxx', '.ipp'}


def repository_path(root, path):
    if not isinstance(path, str) or not path or PurePosixPath(path).is_absolute() or '..' in path.split('/'):
        raise ValueError(f'Expected a repository-relative path: {path!r}')
    result = root / path
    result.resolve().relative_to(root.resolve())
    return result


def read_style(root, folder):
    include = repository_path(root, folder) / 'linters.make.inc'
    if not include.is_file():
        return None

    text = re.sub(r'#[^\n]*', '', include.read_text()).strip()
    if not re.search(r'\bSTYLE_CPP\s*\(', text):
        return None

    # Recognize the explicit opt-in template, not arbitrary ymake conditions.
    # Refuse unsupported constructs before formatting any files.
    guarded = re.fullmatch(r'IF\s*\(\s*MODULE_LANG\s*==\s*CPP\s*\)(.*?)ENDIF\s*\(\s*\)', text, re.S)
    if guarded:
        text = guarded.group(1)

    match = re.fullmatch(
        r'\s*(?:SET\s*\(\s*MODULE_COMMON_CONFIGS_DIR\s+([^\s()$]+)\s*\)\s*)?'
        r'STYLE_CPP\s*\(\s*CONFIG_TYPE\s+\.clang-format\s*\)\s*',
        text,
    )
    if not match:
        raise ValueError(f'{include}: unsupported format configuration; use the template in cpp_format/README.md')

    style = repository_path(root, match.group(1) or folder) / '.clang-format'
    style.resolve().relative_to(root.resolve())
    if not style.is_file():
        raise ValueError(f'Format configuration does not exist: {style}')

    return style


def discover(root):
    folders = json.loads((root / AUTOINCLUDES).read_text())
    if not isinstance(folders, list) or any(not isinstance(folder, str) for folder in folders):
        raise ValueError(f'{AUTOINCLUDES}: expected a list of folder paths')

    configs = {}
    for folder in folders:
        repository_path(root, folder)
        folder = PurePosixPath(folder).as_posix()
        if folder == '.':
            raise ValueError('Repository-wide autoincludes are not supported by this script')
        configs[folder] = read_style(root, folder)

    for folder in configs:
        if any(parent.as_posix() in configs for parent in PurePosixPath(folder).parents):
            raise ValueError(f'Overlapping autoinclude roots at {folder}; resolve the scope before formatting')

    return {folder: style for folder, style in configs.items() if style is not None}


def skip_style(file):
    # Match library.python.testing.style.rules.get_skip_reason, which is bundled
    # with Ya and used by tools/cpp_style_checker/wrapper.py. Keep these rules in
    # sync with that helper; the module is not available to system Python.
    path = file.as_posix()
    if '/generated/' in path or '/vendor/' in path:
        return True

    path_without_prefix = path
    prefix = 'devtools/contrib/'
    if prefix in path_without_prefix:
        path_without_prefix = path_without_prefix.split(prefix, 1)[1]
    if '/contrib/' in path_without_prefix and '/.yandex_meta/' not in path:
        return True

    content = file.read_bytes()
    return any(
        marker in content
        for marker in (
            b'# DO_NOT_STYLE',
            b'// DO_NOT_STYLE',
            b'THIS SOFTWARE',
            b'WITHOUT WARRANT',
        )
    )


def selected_files(root, configs):
    output = subprocess.check_output(['git', '-C', str(root), 'ls-files', '-z'])
    groups = {folder: [] for folder in sorted(configs)}
    for path in sorted({os.fsdecode(path) for path in output.split(b'\0') if path}):
        if PurePosixPath(path).suffix not in EXTENSIONS:
            continue

        for folder in groups:
            if not path.startswith(folder + '/'):
                continue
            file = root / path
            if file.is_symlink() or not file.is_file():
                break
            file.resolve().relative_to(root.resolve())
            if not skip_style(file):
                groups[folder].append(path)
            break

    return groups


def resolve_formatter(root):
    override = os.environ.get('YDB_CLANG_FORMAT')
    if override:
        binary = shutil.which(override)
        if not binary:
            raise ValueError(f'YDB_CLANG_FORMAT is not executable: {override}')
        return binary
    try:
        binary = subprocess.check_output(
            [str(root / 'ya'), 'tool', 'clang-format-18', '--print-path'],
            cwd=root,
            text=True,
            stderr=subprocess.PIPE,
        ).strip()
    except subprocess.CalledProcessError as error:
        raise ValueError(f'Cannot resolve the Ya formatter: {error.stderr.strip()}') from error

    if not binary:
        raise ValueError('Ya returned an empty clang-format path')

    return binary


def format_files(root, files, style, binary, fix):
    failed = False
    for start in range(0, len(files), 32):
        command = [binary, '--style=file:' + str(style)]
        command += ['-i'] if fix else ['--dry-run', '--Werror', '--ferror-limit=1']
        command += [str(root / path) for path in files[start : start + 32]]
        failed |= subprocess.run(command, cwd=root).returncode != 0
    return int(failed)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--fix', action='store_true', help='apply formatting in place')
    action.add_argument('--check', action='store_true', help='check formatting without changing files')
    action.add_argument('--list', action='store_true', help='list selected files without running clang-format')
    action.add_argument('--print-binary', action='store_true', help='print the Ya clang-format binary path')
    parser.add_argument(
        '--root',
        action='append',
        default=[],
        metavar='FOLDER',
        help='limit to this enabled autoinclude root, relative to the repository; repeatable',
    )

    args = parser.parse_args(argv)
    if args.print_binary:
        if args.root:
            parser.error('--root does not apply to --print-binary')
        print(resolve_formatter(ROOT))
        return 0

    configs = discover(ROOT)
    if args.root:
        requested = {PurePosixPath(folder).as_posix() for folder in args.root}
        unknown = requested - configs.keys()
        if unknown:
            parser.error('Not enabled for C++ formatting in autoincludes: ' + ', '.join(sorted(unknown)))
        configs = {folder: style for folder, style in configs.items() if folder in requested}

    groups = selected_files(ROOT, configs)
    if args.list:
        for files in groups.values():
            for path in files:
                print(path)
        return 0

    if not any(groups.values()):
        print('No tracked C/C++ files in enabled autoinclude folders.')
        return 0

    binary = resolve_formatter(ROOT)
    failed = False
    for folder, files in groups.items():
        if not files:
            continue

        style = configs[folder]
        print(
            f'{"Formatting" if args.fix else "Checking"} {len(files)} files in {folder} '
            f'with {style.relative_to(ROOT)}',
            flush=True,
        )
        failed |= format_files(ROOT, files, style, binary, args.fix) != 0

    return int(failed)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'cpp-format: {error}', file=sys.stderr)
        sys.exit(2)
