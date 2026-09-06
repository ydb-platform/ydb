// Presentation only: topology from recorded IDs, never an inferred semantic plan.
export function inputs(node) {
  return [
    node.input,
    node.left,
    node.right,
    ...(node.inputs || []).map((input) => (typeof input === 'string' ? input : input.node)),
  ].filter((x) => typeof x === 'string');
}

export function topology(snapshot, stages = false) {
  if (!snapshot?.plan) return { nodes: [], edges: [], issues: [] };
  if (stages && snapshot.stage_graph) {
    const graph = snapshot.stage_graph;
    return {
      nodes: graph.stages.map((stage) => ({ ...stage, op: 'stage', root: stage.id === graph.root_stage })),
      edges: graph.edges.map((edge) => ({
        from: edge.producer,
        to: edge.consumer,
        label: edge.kind,
        raw: edge,
      })),
      issues: [],
    };
  }
  const nodes = snapshot.plan.nodes.map((node) => ({ ...node, root: node.id === snapshot.plan.root }));
  const edges = nodes.flatMap((node) => inputs(node).map((from) => ({ from, to: node.id })));
  for (const subplan of snapshot.plan.subplans || []) {
    for (const to of subplan.consumers || [])
      edges.push({ from: subplan.root, to, label: subplan.kind, subplan: true });
  }
  return { nodes, edges, issues: [] };
}

export function layout(graph, focus = null) {
  const allIds = new Set(graph.nodes.map((n) => n.id));
  const issues = graph.edges
    .filter((e) => !allIds.has(e.from) || !allIds.has(e.to))
    .map((e) => `Unresolved edge ${e.from} → ${e.to}`);
  const connected = new Set([focus]);
  if (focus)
    for (const edge of graph.edges) {
      if (edge.from === focus) connected.add(edge.to);
      if (edge.to === focus) connected.add(edge.from);
    }
  const nodes = graph.nodes.filter((n) => !focus || connected.has(n.id));
  const ids = new Set(nodes.map((n) => n.id));
  const edges = graph.edges.filter((e) => ids.has(e.from) && ids.has(e.to));
  const levels = new Map();
  const pending = new Set(ids);
  while (pending.size) {
    let progress = false;
    for (const id of pending) {
      const parents = edges.filter((e) => e.to === id).map((e) => e.from);
      if (parents.every((p) => levels.has(p))) {
        levels.set(id, Math.max(-1, ...parents.map((p) => levels.get(p))) + 1);
        pending.delete(id);
        progress = true;
      }
    }
    if (!progress) {
      issues.push('Cyclic topology: unresolved nodes are displayed together; no execution order is implied.');
      const last = Math.max(-1, ...levels.values()) + 1;
      for (const id of pending) levels.set(id, last);
      break;
    }
  }
  const layers = [];
  for (const node of nodes) (layers[levels.get(node.id)] ||= []).push(node);
  const width = Math.max(400, ...layers.map((layer) => layer.length * 202 + 44));
  const positions = new Map();
  layers.forEach((layer, depth) =>
    layer.forEach((node, index) => {
      positions.set(node.id, { x: (width - layer.length * 202) / 2 + index * 202 + 13, y: 35 + depth * 116 });
    }),
  );
  return {
    nodes,
    edges,
    positions,
    width,
    height: Math.max(220, layers.length * 116 + 38),
    issues,
    hidden: graph.nodes.length - nodes.length,
  };
}

export function shortTable(name = '') {
  const path = name.match(/;path:\d+:([^;]+);/);
  return (path?.[1] || name).split('/').filter(Boolean).at(-1) || name;
}

export function expression(expr, depth = 0) {
  if (expr == null) return '—';
  if (typeof expr !== 'object') return String(expr);
  if (depth > 5) return '…';
  const next = (x) => expression(x, depth + 1);
  if (expr.kind === 'column') return expr.column;
  if (expr.kind === 'literal')
    return typeof expr.value === 'object' ? JSON.stringify(expr.value) : String(expr.value);
  if (expr.kind === 'null') return `NULL::${expr.type}`;
  const operators = {
    eq: expr.null_safe ? '≡' : '=',
    lt: '<',
    lte: '≤',
    gt: '>',
    gte: '≥',
    add: '+',
    sub: '−',
    mul: '×',
    div: '/',
  };
  if (Object.hasOwn(operators, expr.kind))
    return `(${next(expr.left)} ${operators[expr.kind]} ${next(expr.right)})`;
  if (expr.kind === 'and' || expr.kind === 'or')
    return Array.isArray(expr.args)
      ? expr.args.map(next).join(expr.kind === 'and' ? ' ∧ ' : ' ∨ ')
      : `${expr.kind}(invalid args)`;
  return `${expr.kind}(${Object.entries(expr)
    .filter(([k, v]) => k !== 'kind' && v && typeof v === 'object')
    .map(([, v]) => (Array.isArray(v) ? v.map(next).join(', ') : next(v)))
    .join(', ')})`;
}

export function summary(node, snapshot) {
  if (node.op === 'scan') return shortTable(node.table);
  if (node.op === 'join')
    return `${String(node.kind || 'unspecified').replaceAll('_', ' ')} · ${node.keys?.length || 0} key pairs`;
  if (node.op === 'filter') return expression(node.predicate);
  if (node.op === 'aggregate')
    return `${node.keys?.length || 0} keys · ${(node.aggregates || []).map((a) => String(a.function || '?').toUpperCase()).join(', ')}`;
  if (node.op === 'sort')
    return (node.order || []).map((o) => `${o.column} ${o.ascending ? '↑' : '↓'}`).join(', ');
  if (node.op === 'project')
    return `${node.columns?.length || 0} projections${node.ordered ? ' · ordered' : ''}`;
  if (node.op === 'limit')
    return `${expression(node.count)} rows${node.offset ? ` · offset ${expression(node.offset)}` : ''}`;
  if (node.op === 'stage')
    return (node.nodes || []).map((id) => snapshot.plan.nodes.find((n) => n.id === id)?.op || id).join(' → ');
  return node.dependency || node.phase || 'Recorded operator';
}
