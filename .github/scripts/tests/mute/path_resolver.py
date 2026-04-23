from __future__ import annotations

import os
import shlex

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
    preset = preset.strip().lower()
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
        raise FileNotFoundError(f'neither {full} nor fallback {fb} exists')
    return os.path.join(CONFIG_DIR, 'muted_ya.txt').replace('\\', '/'), True


def bash_exports_for_workspace(repo_root: str, preset: str) -> tuple[str, str, str]:
    path, used_fallback = resolve_for_workspace(repo_root, preset)
    fallback_flag = '1' if used_fallback else '0'
    exports = [
        f'export MUTED_YA_FILE={shlex.quote(path)}',
        f'export MUTED_YA_IS_FALLBACK={fallback_flag}',
    ]
    return path, fallback_flag, '\n'.join(exports)
