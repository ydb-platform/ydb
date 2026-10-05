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


def _evlog_arg(path: str) -> str:
    return f' --evlog-file {path}' if path else ''


def main(
    ya_make_command: str,
    graph_path: str,
    context_path: str,
    base_commit: str,
    head_commit: str,
    evlog_dir: str = '',
) -> None:
    ya = ya_make_command.split(' ')[0]

    workdir = os.getenv('workdir')
    if not workdir:
        workdir = tempfile.mkdtemp()

    base_evlog = ''
    head_evlog = ''
    if evlog_dir:
        os.makedirs(evlog_dir, exist_ok=True)
        base_evlog = os.path.join(evlog_dir, 'graph_compare_base_evlog.jsonl')
        head_evlog = os.path.join(evlog_dir, 'graph_compare_head_evlog.jsonl')

    log(f'Workdir: {workdir}')
    log('Checkout base commit...')
    # -f: the debug workflow copies this branch's .github onto the PR tree so
    # local actions are the sharding ones. That dirties tracked files, and a
    # plain checkout aborts before either graph is written.
    exec(f'git checkout -f {base_commit}')
    log('Build graph for base commit...')
    exec(
        f'{ya_make_command} ydb --cache-tests'
        f' --save-graph-to {workdir}/graph_base.json'
        f' --save-context-to {workdir}/context_base.json'
        f'{_evlog_arg(base_evlog)}'
    )

    log('Checkout head commit...')
    exec(f'git checkout -f {head_commit}')
    log('Build graph for head commit...')
    exec(
        f'{ya_make_command} ydb --cache-tests'
        f' --save-graph-to {workdir}/graph_head.json'
        f' --save-context-to {workdir}/context_head.json'
        f'{_evlog_arg(head_evlog)}'
    )

    log('Generate diff graph...')
    exec(f'{ya} tool ygdiff --old {workdir}/graph_base.json --new {workdir}/graph_head.json --cut {graph_path} --dump-uids {workdir}/uids.json')

    log('Generate diff context...')
    exec(f'{ya} tool contexts_difference {workdir}/context_base.json {workdir}/context_head.json {context_path} {workdir}/uids.json')


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
    parser.add_argument(
        '--evlog-dir',
        dest='evlog_dir',
        default='',
        help='If set, write graph_compare_{base,head}_evlog.jsonl here for ci_metrics',
    )
    opts = parser.parse_args()
    main(
        ya_make_command=opts.ya_make_command,
        graph_path=opts.result_graph_path,
        context_path=opts.result_context_path,
        base_commit=opts.base_commit,
        head_commit=opts.head_commit,
        evlog_dir=opts.evlog_dir,
    )
