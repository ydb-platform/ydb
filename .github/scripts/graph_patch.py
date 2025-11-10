#! /usr/bin/python3 -u

from __future__ import annotations
import argparse
import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'tests'))
import mute_utils


def get_failed_uids(muted_path: str, report_path: str) -> set[str]:
    print('Load failed uids..')
    result = set()
    mute_check = mute_utils.MuteTestCheck(muted_path) if muted_path else None
    with open(report_path) as report_file:
        report = json.load(report_file).get('results', [])
    for record in report:
        if record.get('status', 'OK') == 'OK' or record.get('suite', False):
            continue
        if mute_check is not None:
            test_name = f'{record.get("path", "")} {record.get("name", "")}.{record.get("subtest_name", "")}'
            if mute_check(test_name):
                continue
        uid = record.get('uid')
        if uid:
            result.add(uid)
    print(f'{len(result)} uids loaded')
    return result


def _strip_graph(graph: dict, uids_filter: set[str]) -> dict:
    result = {uid for uid in graph['result'] if uid in uids_filter}
    nodes = _strip_unused_nodes(graph['graph'], result)

    conf = graph.get('conf', {}).copy()
    conf['resources'] = _filter_duplicate_resources(conf.get('resources', []))

    return {'conf': conf, 'inputs': graph.get('inputs', {}), 'result': [uid for uid in result], 'graph': nodes}


def _strip_unused_nodes(graph_nodes: list, result: set[str]) -> list[dict]:
    by_uid = {n['uid']: n for n in graph_nodes}

    def visit(uid):
        if uid in by_uid:
            node = by_uid.pop(uid)
            yield node
            for dep in node['deps']:
                yield from visit(dep)

    result_nodes: list[dict] = []
    for uid in result:
        for node in visit(uid):
            if node['uid'] in result:
                node['cache'] = False
            result_nodes.append(node)

    return result_nodes


def _filter_duplicate_resources(resources: list[dict]) -> list[dict]:
    v = set()
    result = []
    for x in resources:
        if x['pattern'] not in v:
            v.add(x['pattern'])
            result.append(x)
    return result


def process_graph(in_graph_path: str, out_graph_path: str, uids_filter: set[str]) -> None:
    print('Load graph...')
    with open(in_graph_path) as f:
        in_graph = json.load(f)
    print('Strip graph...')
    out_graph = _strip_graph(in_graph, uids_filter)
    print('Save graph...')
    with open(out_graph_path, 'w') as f:
        json.dump(out_graph, f, indent=2)
    print('Process graph...OK')


def process_context(in_context_path: str, out_context_path: str, uids_filter: set[str]) -> None:
    print('Load context...')
    with open(in_context_path) as f:
        in_context = json.load(f)
    out_context = {}
    print('Strip context...')
    for k, v in in_context.items():
        if k == 'tests':
            out_context[k] = {uid: v[uid] for uid in v.keys() if uid in uids_filter}
        elif k == 'graph':
            out_context[k] = _strip_graph(v, uids_filter)
        else:
            out_context[k] = v
    print('Save context...')
    with open(out_context_path, 'w') as f:
        json.dump(out_context, f, indent=2)
    print('Process context...OK')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--in-graph', '-G', type=str, dest='in_graph', required=True,
        help='Path to input graph'
    )
    parser.add_argument(
        '--in-context', '-C', type=str, dest='in_context', required=True,
        help='Path to input context'
    )
    parser.add_argument(
        '--out-graph', '-g', type=str, dest='out_graph', required=True,
        help='Path to result graph'
    )
    parser.add_argument(
        '--out-context', '-c', type=str, dest='out_context', required=True,
        help='Path to result context'
    )
    parser.add_argument(
        '--report', '-r', type=str, dest='report', required=True,
        help='Path to json build report'
    )
    parser.add_argument('--muted', '-m', type=str, help='Path to muted tests', dest='muted')
    opts = parser.parse_args()
    uids = get_failed_uids(muted_path=opts.muted, report_path=opts.report)
    process_graph(in_graph_path=opts.in_graph, out_graph_path=opts.out_graph, uids_filter=uids)
    process_context(in_context_path=opts.in_context, out_context_path=opts.out_context, uids_filter=uids)
