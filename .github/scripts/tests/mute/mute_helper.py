#!/usr/bin/env python3
"""GitHub Actions helpers for mute workflows: matrix generation and mute path resolve."""

from __future__ import annotations

import argparse
import json
import os
import sys

from mute_utils import bash_exports_for_workspace, dedicated_relative, resolve_for_workspace


def _parse_dispatch_branches(raw: str) -> list[str]:
    return [p.strip() for p in raw.replace('\n', ',').split(',') if p.strip()]


def parse_allowed_build_types(raw: str) -> list[str]:
    """Presets from a comma-separated CLI value: order preserved, duplicates dropped, lowercased."""
    if not (raw or '').strip():
        raise ValueError('--allowed-build-types must be non-empty (comma-separated presets)')
    out: list[str] = []
    seen: set[str] = set()
    for part in raw.split(','):
        t = part.strip().lower()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    if not out:
        raise ValueError('--allowed-build-types contains no non-empty tokens')
    return out


def _normalize_build_types(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        t = str(item).strip().lower()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def load_build_type_policy(config_path: str) -> tuple[list[str], dict[str, list[str]]]:
    """
    Load branch build-type policy for mute updates.

    Expected format:
    {
      "default_build_types": ["relwithdebinfo"],
      "branch_overrides": {
        "main": ["relwithdebinfo", "release-asan"],
        "stable-26-1": ["relwithdebinfo"]
      }
    }
    """
    with open(config_path, encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f'{config_path}: expected JSON object')

    defaults_raw = data.get('default_build_types')
    if not isinstance(defaults_raw, list):
        raise ValueError(f'{config_path}: "default_build_types" must be a JSON array')
    defaults = _normalize_build_types(defaults_raw)
    if not defaults:
        raise ValueError(f'{config_path}: "default_build_types" must contain at least one build type')

    overrides_raw = data.get('branch_overrides', {})
    if overrides_raw is None:
        overrides_raw = {}
    if not isinstance(overrides_raw, dict):
        raise ValueError(f'{config_path}: "branch_overrides" must be a JSON object')

    overrides: dict[str, list[str]] = {}
    for branch, raw_value in overrides_raw.items():
        branch_name = str(branch).strip()
        if not branch_name:
            continue
        if isinstance(raw_value, list):
            values = raw_value
        elif isinstance(raw_value, dict) and isinstance(raw_value.get('build_types'), list):
            values = raw_value['build_types']
        else:
            raise ValueError(
                f'{config_path}: override for branch "{branch_name}" must be an array or '
                '{"build_types": [...]}'
            )
        normalized = _normalize_build_types(values)
        if not normalized:
            raise ValueError(f'{config_path}: override for branch "{branch_name}" has no valid build types')
        overrides[branch_name] = normalized

    return defaults, overrides


def parse_requested_build_types(raw: str) -> set[str]:
    return {p.strip().lower() for p in (raw or '').split(',') if p.strip()}


def load_branches(branches_file: str) -> list[str]:
    with open(branches_file, encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f'{branches_file}: expected JSON array of branch names')
    return [str(b).strip() for b in data if str(b).strip()]


def build_matrix(
    *,
    branches_file: str,
    branches_override: str,
    allowed_presets: list[str],
    build_type_policy: tuple[list[str], dict[str, list[str]]] | None,
    event_name: str,
    build_types_raw: str,
    explicit_dispatch_types: bool,
) -> list[dict[str, str]]:
    trim_override = (branches_override or '').strip()
    if trim_override:
        branch_list = _parse_dispatch_branches(trim_override)
    else:
        branch_list = load_branches(branches_file)

    requested_presets = parse_requested_build_types(build_types_raw)

    out: list[dict[str, str]] = []
    for branch in branch_list:
        if build_type_policy is not None:
            defaults, overrides = build_type_policy
            branch_allowed = list(overrides.get(branch, defaults))
        else:
            branch_allowed = list(allowed_presets)

        if event_name == 'workflow_dispatch':
            raw = (build_types_raw or '').strip().lower()
            if raw and raw != 'all':
                branch_allowed = [p for p in branch_allowed if p in requested_presets]

        if event_name == 'workflow_dispatch' and explicit_dispatch_types and not branch_allowed:
            continue

        for preset in branch_allowed:
            preset_l = preset.strip().lower()
            out.append(
                {
                    'BASE_BRANCH': branch,
                    'BUILD_TYPE': preset,
                    'MUTED_YA_RELATIVE': dedicated_relative(preset_l),
                }
            )
    return out


def _append_github_env(key: str, value: str) -> None:
    path = os.environ.get('GITHUB_ENV')
    if path:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f'{key}={value}\n')


def _append_github_output(key: str, value: str) -> None:
    path = os.environ.get('GITHUB_OUTPUT')
    if path:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f'{key}={value}\n')


def _emit_matrix_to_outputs(matrix: list[dict[str, str]]) -> None:
    compact = json.dumps(matrix, separators=(',', ':'), ensure_ascii=False)
    _append_github_output('matrix_include', compact)

    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if not summary:
        return
    lines = [
        f'### Mute update matrix ({len(matrix)} jobs)\n',
        '```json\n',
        json.dumps(matrix, indent=2, ensure_ascii=False) + '\n',
        '```\n',
    ]
    with open(summary, 'a', encoding='utf-8') as f:
        f.writelines(lines)


def cmd_matrix(args: argparse.Namespace) -> int:
    override = args.branches_override if args.branches_override else os.environ.get('INPUT_BRANCHES', '')
    build_types_raw = (
        args.build_types_override
        if args.build_types_override
        else os.environ.get('INPUT_BUILD_TYPES', '')
    )
    event_name = args.event_name or os.environ.get('GITHUB_EVENT_NAME', '')

    raw_bt = (build_types_raw or '').strip().lower()
    explicit_dispatch_types = event_name == 'workflow_dispatch' and raw_bt not in ('', 'all')

    try:
        allowed_presets: list[str] = []
        policy: tuple[list[str], dict[str, list[str]]] | None = None
        build_types_config = (args.build_types_config or '').strip()
        allowed_raw = (args.allowed_build_types or '').strip()

        if build_types_config:
            policy = load_build_type_policy(build_types_config)
        elif allowed_raw:
            allowed_presets = parse_allowed_build_types(allowed_raw)
        else:
            raise ValueError('Pass either --build-types-config or --allowed-build-types')

        matrix = build_matrix(
            branches_file=args.branches_file,
            branches_override=override,
            allowed_presets=allowed_presets,
            build_type_policy=policy,
            event_name=event_name,
            build_types_raw=build_types_raw,
            explicit_dispatch_types=explicit_dispatch_types,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f'::error::{exc}', file=sys.stderr)
        return 1

    if not matrix:
        print(
            '::error::Mute update matrix is empty (no branches/build_types from config/overrides, '
            'or workflow_dispatch build_types outside policy).',
            file=sys.stderr,
        )
        return 1

    _emit_matrix_to_outputs(matrix)

    return 0


def cmd_resolve_path(args: argparse.Namespace) -> int:
    repo_root = os.path.abspath(args.repo_root)
    preset = args.preset.strip()
    try:
        if args.emit_bash_env:
            path, fallback_flag, exports = bash_exports_for_workspace(repo_root, preset)
            print(exports)
            _append_github_env('MUTED_YA_FILE', path)
            _append_github_env('MUTED_YA_IS_FALLBACK', fallback_flag)
            return 0
        if args.print_dedicated_relative:
            print(dedicated_relative(preset))
            return 0
        if args.print_resolved_relative:
            rel, _ = resolve_for_workspace(repo_root, preset)
            print(rel)
            return 0
    except (FileNotFoundError, OSError) as exc:
        print(f'::error::{exc}', file=sys.stderr)
        return 2
    print('::error::No resolve-path action selected', file=sys.stderr)
    return 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)

    pm = sub.add_parser('matrix', help='Emit matrix_include JSON for update_muted_ya setup job')
    pm.add_argument(
        '--branches-file',
        required=True,
        help='Path to stable_tests_branches.json (JSON array of branch names)',
    )
    pm.add_argument(
        '--allowed-build-types',
        default='',
        help='Legacy mode: comma-separated build presets for all branches (order preserved)',
    )
    pm.add_argument(
        '--build-types-config',
        default='',
        help='Path to mute build-type policy JSON (default + branch overrides)',
    )
    pm.add_argument(
        '--branches-override',
        default='',
        help='Comma-separated branches instead of branches-file (default: env INPUT_BRANCHES)',
    )
    pm.add_argument(
        '--build-types-override',
        default='',
        help='workflow_dispatch: env INPUT_BUILD_TYPES or "all" (default: env)',
    )
    pm.add_argument(
        '--event-name',
        default='',
        help='Override GITHUB_EVENT_NAME (default: env)',
    )
    pm.set_defaults(func=cmd_matrix)

    rp = sub.add_parser(
        'resolve-path',
        help='Resolve mute file path from build preset (dedicated or fallback)',
    )
    rp.add_argument(
        '--preset',
        required=True,
        help='build_preset / BUILD_TYPE, e.g. relwithdebinfo, release-asan',
    )
    rp.add_argument(
        '--repo-root',
        default='.',
        help='Repository root for existence checks (default: cwd)',
    )
    rg = rp.add_mutually_exclusive_group(required=True)
    rg.add_argument(
        '--emit-bash-env',
        action='store_true',
        help='Print export lines for current shell; also append to GITHUB_ENV if set (next steps)',
    )
    rg.add_argument(
        '--print-dedicated-relative',
        action='store_true',
        help='Print dedicated path for preset (no fallback); for git cat-file checks',
    )
    rg.add_argument(
        '--print-resolved-relative',
        action='store_true',
        help='Print path used for ya/transform (with workspace fallback)',
    )
    rp.set_defaults(func=cmd_resolve_path)

    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
