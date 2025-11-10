#! /usr/bin/python3 -u

# Compares build graphs for two given refs in the current directory git repo
# Creates ya.make in the current directory listing affected ydb targets
# Parameters: base_commit_sha head_commit_sha

import os
import tempfile
import argparse


def exec(command: str):
    print(f'++ {command}')
    rc = os.system(command)
    if rc != 0:
        print(f'failed, return code {rc}')
        exit(1)


def log(msg: str):
    print(msg)


def main(ya_make_command: str, graph_path: str, context_path: str, base_commit: str, head_commit: str) -> None:
    ya = ya_make_command.split(' ')[0]

    workdir = os.getenv('workdir')
    if not workdir:
        workdir = tempfile.mkdtemp()

    log(f'Workdir: {workdir}')
    log('Checkout base commit...')
    exec(f'git checkout {base_commit}')
    log('Build graph for base commit...')
    exec(f'{ya_make_command} ydb -k --cache-tests --save-graph-to {workdir}/graph_base.json --save-context-to {workdir}/context_base.json')

    log('Checkout head commit...')
    exec(f'git checkout {head_commit}')
    log('Build graph for head commit...')
    exec(f'{ya_make_command} ydb -k --cache-tests --save-graph-to {workdir}/graph_head.json --save-context-to {workdir}/context_head.json')

    log('Generate diff graph...')
    exec(f'{ya} tool ygdiff --old {workdir}/graph_base.json --new {workdir}/graph_head.json --cut {graph_path} --dump-uids {workdir}/uids.json --no-cache-for-affected-nodes')

    log('Generate diff context...')
    exec(f'{ya} tool context_difference {workdir}/context_base.json {workdir}/context_head.json {context_path} {workdir}/uids.json {opts.graph_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--result-graph-path', '-g', type=str, dest='result_graph_path', required=True,
        help='Path to result graph'
    )
    parser.add_argument(
        '--result-context-path', '-c', type=str, dest='result_context_path', required=True,
        help='Path to result context'
    )
    parser.add_argument(
        '--ya-make-command', '-y', type=str, dest='ya_make_command', required=True,
        help='Ya make command'
    )
    parser.add_argument(dest='base_commit', help='Base commit')
    parser.add_argument(dest='head_commit', help='Head commit')
    opts = parser.parse_args()
    main(
        ya_make_command=opts.ya_make_command,
        graph_path=opts.result_graph_path,
        context_path=opts.result_context_path,
        base_commit=opts.base_commit,
        head_commit=opts.head_commit
    )
