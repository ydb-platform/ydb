#! /usr/bin/python3 -u

# Compares build graphs for two given refs in the current directory git repo
# Creates ya.make in the current directory listing affected ydb targets
# Parameters: base_commit_sha head_commit_sha

import os
import tempfile
import argparse
import shlex
import subprocess
from typing import List

from build_profile import load_profiles, resolve_profile


def exec(command: List[str]):
    print(f'++ {shlex.join(command)}', flush=True)
    # Inherit both streams so CI keeps the complete output, including failures.
    subprocess.run(command, check=True)


def log(msg: str):
    print(msg)


def main(
    ya_make_command: str,
    graph_path: str,
    context_path: str,
    base_commit: str,
    head_commit: str,
    build_profile: str = None,
) -> None:
    command = shlex.split(ya_make_command)
    ya = command[0]
    # Snapshot CI profiles before checkout: old revisions may not contain the file.
    fallback = load_profiles() if build_profile else None

    def graph_command():
        return command + (resolve_profile(build_profile, fallback)["args"] if build_profile else [])

    workdir = os.getenv('workdir')
    if not workdir:
        # Prefer job TMP_DIR (cleaned after build_and_test_ya) over system /tmp.
        parent = os.getenv('TMP_DIR')
        if parent:
            workdir = tempfile.mkdtemp(prefix='graph_compare_', dir=parent)
        else:
            workdir = tempfile.mkdtemp()

    log(f'Workdir: {workdir}')
    log('Checkout base commit...')
    exec(['git', 'checkout', base_commit])
    log('Build graph for base commit...')
    exec(
        graph_command()
        + [
            'ydb',
            '--cache-tests',
            '--save-graph-to',
            f'{workdir}/graph_base.json',
            '--save-context-to',
            f'{workdir}/context_base.json',
        ]
    )

    log('Checkout head commit...')
    exec(['git', 'checkout', head_commit])
    log('Build graph for head commit...')
    exec(
        graph_command()
        + [
            'ydb',
            '--cache-tests',
            '--save-graph-to',
            f'{workdir}/graph_head.json',
            '--save-context-to',
            f'{workdir}/context_head.json',
        ]
    )

    log('Generate diff graph...')
    exec(
        [
            ya,
            'tool',
            'ygdiff',
            '--old',
            f'{workdir}/graph_base.json',
            '--new',
            f'{workdir}/graph_head.json',
            '--cut',
            graph_path,
            '--dump-uids',
            f'{workdir}/uids.json',
        ]
    )

    log('Generate diff context...')
    exec(
        [
            ya,
            'tool',
            'contexts_difference',
            f'{workdir}/context_base.json',
            f'{workdir}/context_head.json',
            context_path,
            f'{workdir}/uids.json',
        ]
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--result-graph-path', '-g', type=str, dest='result_graph_path', required=True, help='Path to result graph'
    )
    parser.add_argument(
        '--result-context-path',
        '-c',
        type=str,
        dest='result_context_path',
        required=True,
        help='Path to result context',
    )
    parser.add_argument(
        '--ya-make-command', '-y', type=str, dest='ya_make_command', required=True, help='Ya make command'
    )
    parser.add_argument('--build-profile', help='Named profile from ydb/ci/build_profiles.yaml')
    parser.add_argument(dest='base_commit', help='Base commit')
    parser.add_argument(dest='head_commit', help='Head commit')
    opts = parser.parse_args()
    main(
        ya_make_command=opts.ya_make_command,
        graph_path=opts.result_graph_path,
        context_path=opts.result_context_path,
        base_commit=opts.base_commit,
        head_commit=opts.head_commit,
        build_profile=opts.build_profile,
    )
