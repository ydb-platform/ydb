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


def do_compare(opts):
    if not opts.ya_make_command:
        print('--ya-make-command not set')
        exit(1)
    ya = opts.ya_make_command.split(' ')[0]

    workdir = os.getenv('workdir')
    if not workdir:
        workdir = tempfile.mkdtemp()

    log(f'Workdir: {workdir}')
    log('Checkout base commit...')
    exec(f'git checkout {opts.base_commit}')
    log('Build graph for base commit...')
    exec(f'{opts.ya_make_command} ydb -k --cache-tests --save-graph-to {workdir}/graph_base.json --save-context-to {workdir}/context_base.json')

    log('Checkout head commit...')
    exec(f'git checkout {opts.head_commit}')
    log('Build graph for head commit...')
    exec(f'{opts.ya_make_command} ydb -k --cache-tests --save-graph-to {workdir}/graph_head.json --save-context-to {workdir}/context_head.json')

    log('Generate diff graph...')
    exec(f'{ya} tool ygdiff --old {workdir}/graph_base.json --new {workdir}/graph_head.json --cut {opts.graph_path} --dump-uids {workdir}/uids.json  --no-cache-for-affected-nodes')

    log('Generate diff context...')
    exec(f'{ya} tool context_difference {workdir}/context_base.json {workdir}/context_head.json {opts.context_path} {workdir}/uids.json {opts.graph_path}')
    exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--result-graph-path', '-g', type=str, help='Path to result graph', dest='graph_path', required=True)
    parser.add_argument('--result-context-path', '-c', type=str, help='Path to result context', dest='context_path', required=True)
    parser.add_argument('--ya-make-command', '-y', type=str, help='Ya make command', dest='ya_make_command', required=True)
    parser.add_argument(dest='base_commit', help='Base commit')
    parser.add_argument(dest='head_commit', help='Head commit')
    do_compare(parser.parse_args())
