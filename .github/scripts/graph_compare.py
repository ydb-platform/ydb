#! /usr/bin/python3 -u

# Compares build graphs for two given refs in the current directory git repo
# Creates ya.make in the current directory listing affected ydb targets
# Parameters: base_commit_sha head_commit_sha

import os
import tempfile
import sys
import json


def exec(command: str):
    print(f'++ {command}')
    rc = os.system(command)
    if rc != 0:
        exit(rc)


def log(msg: str):
    print(f'{msg}\n')


def do_compare():
    if len(sys.argv) < 3:
        print('base or head commit not set')
        exit(1)
    base_commit = sys.argv[1]
    head_commit = sys.argv[2]

    ya_make_command = os.getenv('YA_MAKE_COMMAND')
    if not ya_make_command:
        print('YA_MAKE_COMMAND not set')
        exit(1)

    workdir = os.getenv('workdir')
    if not workdir:
        workdir = tempfile.mkdtemp()

    log(f'Workdir: {workdir}')
    log('Checkout base commit...')
    exec(f'git checkout {base_commit}')
    log('Build graph for base commit...')
    exec(f'{ya_make_command} ydb -k -A --cache-tests -Gj0 > {workdir}/graph_base.json')

    log('Checkout head commit...')
    exec(f'git checkout {head_commit}')
    log('Build graph for head commit...')
    exec(f'{ya_make_command} ydb -k -A --cache-tests -Gj0 > {workdir}/graph_head.json')

    log('Generate diff graph...')
    exec(f'./ya tool ygdiff --old {workdir}/graph_base.json --new {workdir}/graph_head.json --cut {workdir}/graph_diff.json --dump-uids-for-affected-nodes {workdir}/affected_uids.json')

    log('Read diff graph...')
    with open(f'{workdir}/graph_diff.json', 'r') as f:
        diff_graph = json.load(f)

    with open(f'{workdir}/affected_uids.json', 'r') as f:
        uids = set(json.load(f))

    tests = set()
    modules = set()

    log('Scan diff graph...')
    for target in diff_graph.get('graph', []):
        if target.get('uid') not in uids:
            continue
        if target.get('node-type') == 'test':
            path = target.get('kv', {}).get('path')
            if path is not None:
                tests.add(os.path.dirname(path))
        tp = target.get('target_properties')
        if (
            tp is not None
            and tp.get('module_type') is not None
            and tp.get('module_dir', '').startswith('ydb')
            and tp.get('module_tag', '').find('proto') < 0
        ):
            modules.add(tp.get('module_dir'))

    log('Create ya.make')

    with open('ya.make', 'w') as ya_make:
        ya_make.write('RECURSE_FOR_TESTS(\n')
        for test in sorted(tests):
            ya_make.write(f'    {test}\n')
        ya_make.write(')\n\nRECURSE (\n')
        for module in sorted(modules):
            ya_make.write(f'    {module}\n')
        ya_make.write(')\n')
    log('ya.make content:')
    exec('cat ya.make')
    exit(0)


if __name__ == '__main__':
    do_compare()
