#!/usr/bin/env python3
"""GitHub Actions helpers for update_muted_ya: matrix generation and per-job base mute fetch."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

from resolve_muted_ya_path import dedicated_relative


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


def candidate_presets(
    *,
    allowed_base: list[str],
    event_name: str,
    build_types_raw: str,
) -> list[str]:
    """Presets to consider per branch (existence on remote is checked separately)."""
    if event_name != 'workflow_dispatch':
        return list(allowed_base)

    raw = (build_types_raw or '').strip().lower()
    if not raw or raw == 'all':
        return list(allowed_base)

    base_set = set(allowed_base)
    requested_set: set[str] = set()
    for token in [p.strip().lower() for p in build_types_raw.split(',') if p.strip()]:
        if token not in base_set:
            print(
                f'::notice::Preset {token!r} is not in --allowed-build-types — skipped',
                file=sys.stderr,
            )
            continue
        requested_set.add(token)
    return [p for p in allowed_base if p in requested_set]


def _ensure_branch_fetched(git_cwd: str, branch: str, fetch_timeout_s: int = 180) -> bool:
    r = subprocess.run(
        [
            'git',
            'fetch',
            'origin',
            f'{branch}:refs/remotes/origin/{branch}',
            '--depth=1',
        ],
        cwd=git_cwd,
        capture_output=True,
        text=True,
        timeout=fetch_timeout_s,
    )
    if r.returncode != 0:
        err = (r.stderr or r.stdout or '').strip()
        print(f'::notice::Could not fetch branch {branch!r}: {err} — skipping branch', file=sys.stderr)
        return False
    return True


def _origin_dedicated_pathspec(branch: str, preset: str) -> str:
    """``origin/<branch>:<dedicated-relative>`` for git cat-file / git show (path from dedicated_relative)."""
    return f'origin/{branch.strip()}:{dedicated_relative(preset.strip().lower())}'


def _fetch_branch_full(repo_root: str, branch: str, fetch_timeout_s: int = 180) -> bool:
    """Fetch one branch ref (matches update_muted_ya prepare step; no shallow depth)."""
    r = subprocess.run(
        ['git', 'fetch', 'origin', f'{branch}:refs/remotes/origin/{branch}'],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=fetch_timeout_s,
    )
    if r.returncode != 0:
        err = (r.stderr or r.stdout or '').strip()
        print(f'::error::Could not fetch branch {branch!r}: {err}', file=sys.stderr)
        return False
    return True


def _mute_file_exists_on_remote(git_cwd: str, branch: str, preset: str) -> bool:
    ps = _origin_dedicated_pathspec(branch, preset)
    r = subprocess.run(
        ['git', 'cat-file', '-e', ps],
        cwd=git_cwd,
        capture_output=True,
    )
    return r.returncode == 0


def load_branches(branches_file: str) -> list[str]:
    with open(branches_file, encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f'{branches_file}: expected JSON array of branch names')
    return [str(b).strip() for b in data if str(b).strip()]


def build_matrix(
    *,
    branches_file: str,
    git_cwd: str,
    branches_override: str,
    allowed_presets: list[str],
    event_name: str,
    build_types_raw: str,
    explicit_dispatch_types: bool,
) -> list[dict[str, str]]:
    trim_override = (branches_override or '').strip()
    if trim_override:
        branch_list = _parse_dispatch_branches(trim_override)
    else:
        branch_list = load_branches(branches_file)

    candidates = candidate_presets(
        allowed_base=allowed_presets,
        event_name=event_name,
        build_types_raw=build_types_raw,
    )
    if event_name == 'workflow_dispatch' and explicit_dispatch_types and not candidates:
        return []

    out: list[dict[str, str]] = []
    for branch in branch_list:
        if not _ensure_branch_fetched(git_cwd, branch):
            continue
        for preset in candidates:
            if not _mute_file_exists_on_remote(git_cwd, branch, preset):
                if event_name == 'workflow_dispatch' and explicit_dispatch_types:
                    rel = dedicated_relative(preset)
                    print(
                        f'::notice::Branch {branch!r} has no {rel} — skipping ({preset})',
                        file=sys.stderr,
                    )
                continue
            out.append({'BASE_BRANCH': branch, 'BUILD_TYPE': preset})
    return out


def _append_github_env(key: str, value: str) -> None:
    path = os.environ.get('GITHUB_ENV')
    if path:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f'{key}={value}\n')


def _git_show_pathspec_to_file(git_cwd: str, pathspec: str, output_path: str) -> tuple[bool, str | None]:
    r = subprocess.run(
        ['git', 'show', pathspec],
        cwd=git_cwd,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        return False, (r.stderr or r.stdout or '').strip() or 'git show failed'
    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(r.stdout)
    return True, None


def _append_github_output(key: str, value: str) -> None:
    path = os.environ.get('GITHUB_OUTPUT')
    if path:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(f'{key}={value}\n')


def prepare_mute_update_matrix_job(
    *,
    repo_root: str,
    base_branch: str,
    build_type: str,
    pr_branch_prefix: str,
    output_path: str,
) -> int:
    """
    Fetch origin/<base_branch>, resolve dedicated mute path, write base file or set skip=true.
    Appends BASE_BRANCH, BUILD_TYPE, PR_BRANCH to GITHUB_ENV and skip to GITHUB_OUTPUT.
    """
    repo_root = os.path.abspath(repo_root)
    base_branch = base_branch.strip()
    build_type = build_type.strip().lower()
    pr_branch_prefix = pr_branch_prefix.strip()

    _append_github_env('BASE_BRANCH', base_branch)
    _append_github_env('BUILD_TYPE', build_type)
    _append_github_env('PR_BRANCH', f'{pr_branch_prefix}_{base_branch}_{build_type}')

    if not _fetch_branch_full(repo_root, base_branch):
        return 1

    ded = dedicated_relative(build_type)
    ps = _origin_dedicated_pathspec(base_branch, build_type)
    print(f'Dedicated mute path on branch: {ded}')

    # relwithdebinfo → muted_ya.txt is mandatory on target branches (fail job if missing).
    # Other presets use dedicated files that may be absent on older branches → existence check, then skip.
    if build_type == 'relwithdebinfo':
        ok, err = _git_show_pathspec_to_file(repo_root, ps, output_path)
        if not ok:
            print(f'::error::git show {ps}: {err}', file=sys.stderr)
            return 1
        _append_github_output('skip', 'false')
        print(f'✓ Retrieved base {ded} from {base_branch}')
        return 0

    if _mute_file_exists_on_remote(repo_root, base_branch, build_type):
        ok, err = _git_show_pathspec_to_file(repo_root, ps, output_path)
        if not ok:
            print(f'::error::git show {ps}: {err}', file=sys.stderr)
            return 1
        _append_github_output('skip', 'false')
        print(f'✓ Retrieved base {ded} from {base_branch}')
        return 0

    print(
        f'::notice::Branch {base_branch!r} has no {ded} — skipping mute update for build_type={build_type} (opt-in per branch).',
        file=sys.stderr,
    )
    _append_github_output('skip', 'true')
    return 0


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
        allowed_presets = parse_allowed_build_types(args.allowed_build_types)
        matrix = build_matrix(
            branches_file=args.branches_file,
            git_cwd=args.git_cwd,
            branches_override=override,
            allowed_presets=allowed_presets,
            event_name=event_name,
            build_types_raw=build_types_raw,
            explicit_dispatch_types=explicit_dispatch_types,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f'::error::{exc}', file=sys.stderr)
        return 1

    if not matrix:
        print(
            '::error::Mute update matrix is empty (no branches after fetch, no (branch,preset) with mute file '
            'on origin, or invalid dispatch filters).',
            file=sys.stderr,
        )
        return 1

    compact = json.dumps(matrix, separators=(',', ':'), ensure_ascii=False)
    gh_out = os.environ.get('GITHUB_OUTPUT')
    if gh_out:
        with open(gh_out, 'a', encoding='utf-8') as f:
            f.write(f'matrix_include={compact}\n')

    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        lines = [
            f'### Mute update matrix ({len(matrix)} jobs)\n',
            '```json\n',
            json.dumps(matrix, indent=2, ensure_ascii=False) + '\n',
            '```\n',
        ]
        with open(summary, 'a', encoding='utf-8') as f:
            f.writelines(lines)

    return 0


def cmd_prepare_job(args: argparse.Namespace) -> int:
    out = args.output.strip() or 'base_muted_ya.txt'
    out_path = out if os.path.isabs(out) else os.path.join(os.path.abspath(args.repo_root), out)
    return prepare_mute_update_matrix_job(
        repo_root=args.repo_root,
        base_branch=args.base_branch,
        build_type=args.build_type,
        pr_branch_prefix=args.pr_branch_prefix,
        output_path=out_path,
    )


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
        '--git-cwd',
        required=True,
        help='Path to a git clone of the repo (e.g. config-repo from actions/checkout)',
    )
    pm.add_argument(
        '--allowed-build-types',
        required=True,
        help='Comma-separated build presets for the matrix (order preserved); mute paths via resolve_muted_ya_path',
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

    pj = sub.add_parser(
        'prepare-job',
        help='Fetch base branch, extract dedicated mute file into --output, set GITHUB_ENV / GITHUB_OUTPUT skip',
    )
    pj.add_argument('--repo-root', default='.', help='Git repository root (default: cwd)')
    pj.add_argument('--base-branch', required=True, help='Target branch (e.g. stable-25-1)')
    pj.add_argument('--build-type', required=True, help='CI preset / BUILD_TYPE')
    pj.add_argument(
        '--pr-branch-prefix',
        required=True,
        help='Prefix for PR_BRANCH env value (workflow PR_BRANCH_PREFIX)',
    )
    pj.add_argument(
        '--output',
        default='base_muted_ya.txt',
        help='Path for git show blob (default: base_muted_ya.txt under --repo-root)',
    )
    pj.set_defaults(func=cmd_prepare_job)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == '__main__':
    raise SystemExit(main())
