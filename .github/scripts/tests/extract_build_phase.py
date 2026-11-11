"""Keep the build portion of the exact test graph for the sequencing experiment."""
import collections
import json
import sys
from pathlib import Path

source, destination = map(Path, sys.argv[1:])
graph = json.loads(source.read_text())
nodes = {node['uid']: node for node in graph['graph']}
consumers = collections.defaultdict(list)
for uid, node in nodes.items():
    for dep in node['deps']:
        consumers[dep].append(uid)

# Exclude test execution and everything that consumes its results.
removed = {
    uid for uid, node in nodes.items()
    if node.get('kv', {}).get('run_test_node')
    or node.get('kv', {}).get('p') in {'TS', 'TM', 'TL', 'YT'}
}
assert removed, 'No test nodes found: refusing to use an unrecognized graph'
queue = collections.deque(removed)
while queue:
    for uid in consumers[queue.popleft()]:
        if uid not in removed:
            removed.add(uid)
            queue.append(uid)
assert not any(nodes[u].get('kv', {}).get('p') in {'CC', 'LD', 'AR'} for u in removed), 'A build node depends on test execution'

roots = set(graph['result']) - removed
roots.update(dep for uid in removed for dep in nodes[uid]['deps'] if dep not in removed)
keep = set()
queue = list(roots)
while queue:
    uid = queue.pop()
    if uid in keep:
        continue
    assert uid in nodes and uid not in removed, uid
    keep.add(uid)
    queue.extend(nodes[uid]['deps'])

# Preserve node UIDs, commands and requirements; only restrict execution roots.
graph['graph'] = [node for node in graph['graph'] if node['uid'] in keep]
graph['result'] = sorted(roots)
destination.write_text(json.dumps(graph))
print(json.dumps({'build_nodes': len(keep), 'removed_test_and_result_nodes': len(removed),
                  'build_tags': dict(collections.Counter(n.get('kv', {}).get('p', '') for n in graph['graph']))}))
