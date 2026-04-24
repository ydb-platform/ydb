#!/usr/bin/env python3
"""Minimal helpers for mute workflows: matrix and resolve-path."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Set, Tuple

def _parse_csv(raw: str) -> List[str]:
    return [p.strip() for p in (raw or '').replace('\n', ',').split(',') if p.strip()]


def _normalize_unique(items: List[object]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in items:
        t = str(item).strip().lower()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def load_policy(config_path: str) -> Tuple[List[str], Dict[str, List[str]]]:
    with open(config_path, encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f'{config_path}: expected JSON object')

    if not isinstance(data.get('default_build_types'), list):
        raise ValueError(f'{config_path}: "default_build_types" must be a JSON array')
    defaults = _normalize_unique(data['default_build_types'])
    if not defaults:
        raise ValueError(f'{config_path}: "default_build_types" must contain at least one build type')

    overrides_raw = data.get('branch_overrides') or {}
    if not isinstance(overrides_raw, dict):
        raise ValueError(f'{config_path}: "branch_overrides" must be a JSON object')

    overrides: Dict[str, List[str]] = {}
    for branch, values in overrides_raw.items():
        branch_name = str(branch).strip()
        if not branch_name:
            continue
        if isinstance(values, dict):
            values = values.get('build_types')
        if not isinstance(values, list):
            raise ValueError(
                f'{config_path}: override for branch "{branch_name}" must be an array or '
                '{"build_types": [...]}'
            )
        normalized = _normalize_unique(values)
        if not normalized:
            raise ValueError(f'{config_path}: override for branch "{branch_name}" has no valid build types')
        overrides[branch_name] = normalized

    return defaults, overrides


def load_branches_from_file(branches_file: str) -> List[str]:
    with open(branches_file, encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f'{branches_file}: expected JSON array of branch names')
    return [str(b).strip() for b in data if str(b).strip()]


def _append_github_output(key: str, value: str) -> None:
    path = os.environ.get('GITHUB_OUTPUT')
    if path:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f'{key}={value}\n')


def _emit_matrix_to_outputs(matrix: List[Dict[str, str]]) -> None:
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
    from mute_utils import dedicated_relative

    branches_raw = args.branches_override or os.environ.get('INPUT_BRANCHES', '')
    build_types_raw = args.build_types_override or os.environ.get('INPUT_BUILD_TYPES', '')
    event_name = args.event_name or os.environ.get('GITHUB_EVENT_NAME', '')

    try:
        defaults, overrides = load_policy(args.build_types_config)
        branch_list = _parse_csv(branches_raw) if branches_raw.strip() else load_branches_from_file(args.branches_file)

        requested: Optional[Set[str]] = None
        raw = build_types_raw.strip().lower()
        if event_name == 'workflow_dispatch' and raw and raw != 'all':
            requested = set(_normalize_unique(_parse_csv(build_types_raw)))

        matrix: List[Dict[str, str]] = []
        for branch in branch_list:
            branch_types = list(overrides.get(branch, defaults))
            if requested is not None:
                branch_types = [p for p in branch_types if p in requested]
            for preset in branch_types:
                matrix.append(
                    {
                        'BASE_BRANCH': branch,
                        'BUILD_TYPE': preset,
                        'MUTED_YA_RELATIVE': dedicated_relative(preset),
                    }
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
    from mute_utils import dedicated_relative

    print(dedicated_relative(args.preset))
    return 0


def cmd_issue_build_types(args: argparse.Namespace) -> int:
    try:
        with open(args.profiles_config, encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError(f'{args.profiles_config}: expected JSON object')

        profiles = data.get('profiles') or []
        if not isinstance(profiles, list):
            raise ValueError(f'{args.profiles_config}: "profiles" must be a JSON array')

        out: List[str] = []
        seen: Set[str] = set()
        for p in profiles:
            if not isinstance(p, dict):
                continue
            if p.get('branch') != args.branch:
                continue
            bt = p.get('build_type')
            if not isinstance(bt, str):
                continue
            bt = bt.strip()
            if not bt or bt in seen:
                continue
            seen.add(bt)
            out.append(bt)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f'::error::{exc}', file=sys.stderr)
        return 1

    if not out:
        print('::error::Issue build-type matrix is empty.', file=sys.stderr)
        return 1

    print(json.dumps(out, ensure_ascii=False))
    return 0


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
        '--build-types-config',
        required=True,
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

    rp = sub.add_parser('resolve-path', help='Print muted_ya relative path for a build preset')
    rp.add_argument(
        '--preset',
        required=True,
        help='build_preset / BUILD_TYPE, e.g. relwithdebinfo, release-asan',
    )
    rp.set_defaults(func=cmd_resolve_path)

    ib = sub.add_parser(
        'issue-build-types',
        help='Print JSON array of build types for issue workflow from profiles config',
    )
    ib.add_argument(
        '--profiles-config',
        required=True,
        help='Path to mute_issue_and_digest_config.json',
    )
    ib.add_argument(
        '--branch',
        required=True,
        help='Branch name to select matching profiles',
    )
    ib.set_defaults(func=cmd_issue_build_types)

    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
