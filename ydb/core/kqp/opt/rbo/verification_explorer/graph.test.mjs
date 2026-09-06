import test from 'node:test';
import assert from 'node:assert/strict';
import { topology, layout, expression } from './graph.mjs';

test('UnionAll input bindings retain both recorded dataflow edges', () => {
  const plan = {
    root: 'u',
    nodes: [
      { id: 'a', op: 'scan' },
      { id: 'b', op: 'scan' },
      {
        id: 'u',
        op: 'union_all',
        inputs: [
          { node: 'a', columns: ['x'] },
          { node: 'b', columns: ['y'] },
        ],
      },
    ],
  };
  assert.deepEqual(topology({ plan }).edges, [
    { from: 'a', to: 'u' },
    { from: 'b', to: 'u' },
  ]);
});

test('shared DAG nodes are not duplicated; subplan dependencies and focus retain recorded edges', () => {
  const snapshot = {
    plan: {
      root: 'join',
      nodes: [
        { id: 'scan', op: 'scan' },
        { id: 'left', op: 'filter', input: 'scan' },
        { id: 'right', op: 'project', input: 'scan' },
        { id: 'join', op: 'join', left: 'left', right: 'right' },
        { id: 'sub', op: 'scan' },
      ],
      subplans: [{ root: 'sub', consumers: ['right'], kind: 'in' }],
    },
  };
  const graph = topology(snapshot);
  const result = layout(graph);
  assert.equal(result.nodes.length, 5);
  assert.equal(result.edges.length, 5);
  assert.equal(result.issues.length, 0);
  for (const edge of result.edges)
    assert.ok(result.positions.get(edge.from).y < result.positions.get(edge.to).y);
  assert.equal(result.edges.find((e) => e.from === 'sub').subplan, true);
  const focused = layout(graph, 'right');
  assert.deepEqual(new Set(focused.nodes.map((n) => n.id)), new Set(['scan', 'right', 'join', 'sub']));
  assert.equal(focused.hidden, 1);
});

test('cycles and missing edges are visible, not an invented execution order', () => {
  const result = layout({
    nodes: [{ id: 'a' }, { id: 'b' }],
    edges: [
      { from: 'a', to: 'b' },
      { from: 'b', to: 'a' },
      { from: 'missing', to: 'a' },
    ],
  });
  assert.equal(result.nodes.length, 2);
  assert.equal(result.issues.length, 2);
  assert.match(result.issues.join(' '), /Cyclic topology/);
});

test('stage topology uses exported routing, with no task-count inference', () => {
  const snapshot = {
    plan: { nodes: [] },
    stage_graph: {
      root_stage: 'b',
      stages: [
        { id: 'a', nodes: ['n0'] },
        { id: 'b', nodes: ['n1'] },
      ],
      edges: [{ producer: 'a', consumer: 'b', kind: 'hash_shuffle', keys: ['k'] }],
    },
  };
  const graph = topology(snapshot, true);
  assert.equal(graph.nodes[1].root, true);
  assert.deepEqual(graph.edges[0].raw.keys, ['k']);
  assert.equal(graph.nodes[0].task_count, undefined);
  assert.match(expression({ kind: 'constructor' }), /^constructor\(/);
});
