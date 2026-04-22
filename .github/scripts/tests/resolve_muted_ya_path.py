#!/usr/bin/env python3
"""Resolve mute list path under .github/config from CI build preset / tests_monitor build_type."""
from __future__ import annotations

import argparse
import os
import shlex
import sys

CONFIG_DIR = os.path.join('.github', 'config')

# Dedicated file per preset (relwithdebinfo keeps historical name muted_ya.txt).
DEDICATED_NAMES: dict[str, str] = {
    'relwithdebinfo': 'muted_ya.txt',
    'debug': 'muted_ya.txt',
    'release': 'muted_ya.txt',
    'release-asan': 'muted_ya_asan.txt',
    'release-tsan': 'muted_ya_tsan.txt',
    'release-msan': 'muted_ya_msan.txt',
}


def dedicated_relative(preset: str) -> str:
    fn = DEDICATED_NAMES.get(preset, 'muted_ya.txt')
    return os.path.join(CONFIG_DIR, fn).replace('\\', '/')


def resolve_for_workspace(repo_root: str, preset: str) -> tuple[str, bool]:
    """
    Returns (path relative to repo_root, used_fallback).
    If the dedicated file is missing, use muted_ya.txt (backward compatible).
    """
    rel = dedicated_relative(preset)
    full = os.path.normpath(os.path.join(repo_root, rel))
    if os.path.isfile(full):
        return rel.replace('\\', '/'), False
    fb = os.path.join(repo_root, CONFIG_DIR, 'muted_ya.txt')
    if not os.path.isfile(fb):
        print(f'error: neither {full} nor fallback {fb} exists', file=sys.stderr)
        sys.exit(2)
    return os.path.join(CONFIG_DIR, 'muted_ya.txt').replace('\\', '/'), True


def emit_bash_env(repo_root: str, preset: str) -> None:
    """Print `export ...` for the current shell and append KEY=value to GITHUB_ENV when set."""
    path, used_fallback = resolve_for_workspace(repo_root, preset)
    fb = '1' if used_fallback else '0'
    print(f'export MUTED_YA_FILE={shlex.quote(path)}')
    print(f'export MUTED_YA_IS_FALLBACK={fb}')
    gh_env = os.environ.get('GITHUB_ENV')
    if gh_env:
        with open(gh_env, 'a', encoding='utf-8') as f:
            f.write(f'MUTED_YA_FILE={path}\n')
            f.write(f'MUTED_YA_IS_FALLBACK={fb}\n')


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--preset', required=True, help='build_preset / BUILD_TYPE, e.g. relwithdebinfo, release-asan')
    p.add_argument(
        '--repo-root',
        default='.',
        help='Repository root for existence checks (default: cwd)',
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument(
        '--emit-bash-env',
        action='store_true',
        help='Print export lines for current shell; also append to GITHUB_ENV if set (next steps)',
    )
    g.add_argument(
        '--print-dedicated-relative',
        action='store_true',
        help='Print dedicated path for preset (no fallback); for git cat-file checks',
    )
    g.add_argument(
        '--print-resolved-relative',
        action='store_true',
        help='Print path used for ya/transform (with workspace fallback)',
    )
    args = p.parse_args()
    repo_root = os.path.abspath(args.repo_root)
    preset = args.preset.strip()
    if args.emit_bash_env:
        emit_bash_env(repo_root, preset)
        return
    if args.print_dedicated_relative:
        print(dedicated_relative(preset))
        return
    if args.print_resolved_relative:
        rel, _ = resolve_for_workspace(repo_root, preset)
        print(rel)
        return


if __name__ == '__main__':
    main()
