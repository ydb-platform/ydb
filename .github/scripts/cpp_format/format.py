#!/usr/bin/env python3
"""Format tracked C/C++ files in folders opted in through Ya autoincludes."""

import argparse
import difflib
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


def skip_style(root, file):
    # Match library.python.testing.style.rules.get_skip_reason, which is bundled
    # with Ya and used by tools/cpp_style_checker/wrapper.py. Keep these rules in
    # sync with that helper; the module is not available to system Python.
    # Ignore checkout ancestors; keep a leading slash to match top-level folders.
    path = '/' + file.relative_to(root).as_posix()
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


def unconditional_no_lint(makefile):
    # Only a literal, unconditional NO_LINT() disables C++ lint here.
    # Conditional settings remain Ya's responsibility; NO_LINT(ktlint) does
    # not disable the native C++ checker either.
    text = re.sub(r'#[^\n]*', '', makefile.read_text())
    depth = 0
    for match in re.finditer(r'(?m)^[ \t]*(IF|ENDIF|NO_LINT)\s*\(', text):
        macro = match.group(1)
        if macro == 'IF':
            depth += 1
        elif macro == 'ENDIF':
            depth -= 1
        elif depth == 0 and re.match(r'\s*\)', text[match.end() :]):
            return True
    return False


def module_no_lint(root, directory, cache):
    # A parent module can own sources in subdirectories without ya.make.
    # A child with its own ya.make starts a separate module, not an inherited
    # NO_LINT scope. Cache the result for every visited source directory.
    pending = []
    while directory not in cache:
        pending.append(directory)
        makefile = directory / 'ya.make'
        if makefile.is_file():
            disabled = unconditional_no_lint(makefile)
            break
        if directory == root:
            disabled = False
            break
        directory = directory.parent
    else:
        disabled = cache[directory]
    for directory in pending:
        cache[directory] = disabled
    return disabled


def selected_files(root, configs):
    output = subprocess.check_output(['git', '-C', str(root), 'ls-files', '-z'])
    groups = {folder: [] for folder in sorted(configs)}
    no_lint_cache = {}
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
            if not module_no_lint(root, file.parent, no_lint_cache) and not skip_style(root, file):
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


def check_files(root, files, style, binary, clear_progress=None):
    command = [binary, '--style=file:' + str(style)]
    result = subprocess.run(
        command + ['--dry-run', '--Werror', '--ferror-limit=1'] + [str(path) for path in files],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode == 0:
        return 0

    # Match the native checker: clang-format can report replacements that leave
    # the bytes unchanged. A failed dry run needs an actual output comparison.
    failed = False
    for path in files:
        original = path.read_bytes()
        result = subprocess.run(
            command + ['--assume-filename=' + str(path)], cwd=root, input=original, capture_output=True
        )
        diagnostics = result.stderr.decode(errors='replace')
        if result.returncode != 0:
            failed = True
            if not diagnostics:
                diagnostics = f'{path}: clang-format failed with exit status {result.returncode}\n'
        elif result.stdout != original:
            failed = True
            diagnostics += f'{path}: error: code should be clang-formatted\n'
            diagnostics += ''.join(
                difflib.unified_diff(
                    original.decode(errors='replace').splitlines(keepends=True),
                    result.stdout.decode(errors='replace').splitlines(keepends=True),
                    fromfile=str(path),
                    tofile=str(path) + ' (formatted)',
                )
            )
        if diagnostics:
            if clear_progress is not None:
                clear_progress()
            print(diagnostics, end='' if diagnostics.endswith('\n') else '\n', file=sys.stderr, flush=True)
    return int(failed)


def format_files(root, files, style, binary, fix, progress=None, clear_progress=None):
    failed = False
    for start in range(0, len(files), 32):
        batch = [root / path for path in files[start : start + 32]]
        if not fix:
            failed |= check_files(root, batch, style, binary, clear_progress) != 0
            if progress is not None:
                progress(len(batch), 0)
            continue

        before = [path.read_bytes() for path in batch]
        command = [binary, '--style=file:' + str(style), '-i'] + [str(path) for path in batch]
        if clear_progress is None:
            result = subprocess.run(command, cwd=root)
        else:
            # Keep formatter diagnostics separate from the live progress row.
            result = subprocess.run(command, cwd=root, capture_output=True, text=True, errors='replace')
            if result.stdout or result.stderr:
                clear_progress()
                for output, stream in ((result.stdout, sys.stdout), (result.stderr, sys.stderr)):
                    if output:
                        print(output, end='' if output.endswith('\n') else '\n', file=stream, flush=True)
        failed |= result.returncode != 0
        changed = sum(path.read_bytes() != content for path, content in zip(batch, before))
        if progress is not None:
            progress(len(batch), changed)
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
    total = sum(len(files) for files in groups.values())
    checked = formatted = 0
    folder_summaries = []
    interactive = sys.stdout.isatty()
    for folder, files in groups.items():
        if not files:
            continue

        style = configs[folder]
        print(
            f'{"Formatting" if args.fix else "Checking"} {len(files)} files in {folder} '
            f'with {style.relative_to(ROOT)}',
            flush=True,
        )
        folder_checked = folder_formatted = 0

        def clear_progress():
            print('\r\033[2K', end='', flush=True)

        def report_progress(batch_checked, batch_formatted):
            nonlocal checked, formatted, folder_checked, folder_formatted
            checked += batch_checked
            formatted += batch_formatted
            folder_checked += batch_checked
            folder_formatted += batch_formatted
            message = f'Checked {folder_checked} / {len(files)} files'
            if not interactive:
                message += f' in {folder}'
            overall = f'overall {checked} / {total} checked'
            if args.fix:
                message += f'. Formatted {folder_formatted}'
                overall += f', {formatted} formatted'
            message = f'{message} ({overall})'
            if interactive:
                # The heading identifies the folder. Keep the live row within
                # the terminal width so a redraw cannot leave wrapped lines.
                width = max(1, shutil.get_terminal_size().columns - 1)
                if len(message) > width:
                    message = message[: max(0, width - 1)] + '…'
                clear_progress()
            print(message, end='' if interactive else '\n', flush=True)

        try:
            failed |= format_files(
                ROOT, files, style, binary, args.fix, report_progress, clear_progress if interactive else None
            ) != 0
        finally:
            if interactive:
                print(flush=True)
        if folder_formatted:
            folder_summaries.append((folder, folder_formatted, folder_checked))

    status = 'Finished with errors' if failed else 'Finished'
    counts = f'{formatted} formatted / {checked} checked files' if args.fix else f'{checked} / {total} files checked'
    summary_heading = ' Per folder summary:' if folder_summaries else ''
    print(f'{status}: {counts}.{summary_heading}', flush=True)
    for folder, folder_formatted, folder_checked in sorted(folder_summaries, key=lambda row: (-row[1], row[0])):
        print(f'  - {folder}: {folder_formatted} / {folder_checked}', flush=True)

    return int(failed)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'cpp-format: {error}', file=sys.stderr)
        sys.exit(2)
