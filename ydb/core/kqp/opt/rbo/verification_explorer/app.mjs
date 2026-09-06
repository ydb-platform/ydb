import { loadArtifacts, parseLosslessJson } from './artifacts.mjs';
import { topology, layout, shortTable, expression, summary } from './graph.mjs';

const $ = (selector) => document.querySelector(selector);
const state = {
  cases: [],
  current: null,
  tab: 'compare',
  desk: 'fields',
  mode: 'operators',
  selected: null,
  focus: false,
  slot: 0,
  scales: {},
  outcomes: {},
};
const symbols = {
  scan: '▤',
  join: '⋈',
  filter: '▽',
  project: 'π',
  aggregate: 'Σ',
  sort: '↕',
  limit: '⌁',
  union_all: '∪',
  outer_bind: '↳',
  empty_source: '∅',
  stage: '▦',
};
const title = (text) =>
  String(text || '')
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
const json = (value) => JSON.stringify(value, null, 2);
const own = (record, key, fallback) => (Object.hasOwn(record, key) ? record[key] : fallback);

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key.startsWith('on')) node.addEventListener(key.slice(2), value);
    else if (key === 'class') node.className = value;
    else if (key.startsWith('aria-') && value != null) node.setAttribute(key, String(value));
    else if (value != null && value !== false) node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat(Infinity))
    if (child != null && child !== false)
      node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  return node;
}
function svg(tag, attrs, ...children) {
  const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
  for (const child of children)
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  return node;
}
function button(text, action, className = '', attrs = {}) {
  return h('button', { class: className, onclick: action, type: 'button', ...attrs }, text);
}
function empty(message, detail = '') {
  return h(
    'div',
    { class: 'empty' },
    h('span', { class: 'empty-symbol' }, '⌘'),
    h('p', {}, message),
    detail && h('p', { class: 'small-note' }, detail),
  );
}
function notice(message) {
  $('#notice').hidden = !message;
  $('#notice').textContent = message;
}
function active() {
  return state.current?.results?.[state.slot] || state.current;
}
function snapshot(side) {
  return side === 'unpaired' ? active()?.snapshot : active()?.[side];
}
function sideLabel(side) {
  return { before: 'Initial', after: 'Final', unpaired: 'Unpaired snapshot' }[side] || 'Unassigned boundary';
}
function statusInfo(verdict) {
  return own(
    {
      COUNTEREXAMPLE: ['Counterexample candidate', 'error'],
      VERIFIED_BOUNDED: ['Verified · bounded', ''],
      FORMULA_EMITTED: ['Formula generated', 'blue'],
      UNKNOWN: ['Proof unresolved', 'warn'],
      UNSUPPORTED: ['Unsupported model', 'neutral'],
    },
    verdict?.status,
    [verdict?.status ? title(verdict.status) : 'No saved verdict', 'neutral'],
  );
}
function badge(label, tone = '') {
  return h('span', { class: `badge ${tone}` }, label);
}
function setCase(item) {
  state.current = item;
  state.slot = 0;
  state.scales = {};
  state.outcomes = {};
  state.focus = false;
  const plan = (active()?.after || active()?.snapshot)?.plan;
  const node = plan?.nodes.find((n) => n.op === 'join') || plan?.nodes.find((n) => n.id === plan.root);
  state.selected = node ? { side: active()?.snapshot ? 'unpaired' : 'after', id: node.id } : null;
  state.mode = 'operators';
  state.desk = 'fields';
  render();
}

function render() {
  renderCases();
  renderHeader();
  renderTabs();
  renderView();
}
function renderCases() {
  const search = $('#search').value.toLowerCase();
  const cases = state.cases.filter((c) =>
    `${c.title} ${c.description} ${c.id}`.toLowerCase().includes(search),
  );
  $('#case-count').textContent = state.cases.length;
  $('#case-list').replaceChildren(
    ...cases.map((item) =>
      button(
        [
          h(
            'span',
            { class: 'case-icon', 'aria-hidden': 'true' },
            item.verdict?.status === 'COUNTEREXAMPLE' ? '⋈' : '◇',
          ),
          h(
            'span',
            {},
            h('strong', {}, item.title || item.id),
            h(
              'small',
              {},
              item.kind === 'historical'
                ? 'Historical evidence'
                : item.kind === 'mutation'
                  ? 'Deliberate mutation'
                  : statusInfo(item.verdict)[0],
            ),
          ),
        ],
        () => setCase(item),
        `case-item ${item === state.current ? 'active' : ''}`,
        { 'aria-current': item === state.current ? 'page' : null },
      ),
    ),
  );
}
function renderHeader() {
  const item = state.current;
  if (!item) {
    $('#case-header').replaceChildren(
      h('span', { class: 'eyebrow' }, 'READ-ONLY PROOF EXPLORER'),
      h('h1', {}, 'A clearer view of correctness.'),
      h('p', { class: 'subtitle' }, 'Open saved artifacts to inspect a proof, one operator at a time.'),
    );
    return;
  }
  const [label, tone] = statusInfo(item.verdict);
  const before = snapshot('before'),
    after = snapshot('after');
  const metric = (value, label) => h('span', { class: 'metric' }, h('strong', {}, value ?? '—'), label);
  $('#case-header').replaceChildren(
    h(
      'span',
      { class: 'eyebrow' },
      item.kind === 'mutation'
        ? 'MODEL EXPERIMENT / DELIBERATE MUTATION'
        : item.kind === 'historical'
          ? 'RETAINED EVIDENCE / HISTORICAL CAPTURE'
          : 'QUERY EXPLORATION / SAVED ARTIFACTS',
    ),
    h(
      'div',
      { class: 'title-row' },
      h('h1', {}, item.title || item.id),
      h(
        'div',
        { class: 'status-block' },
        badge(label, tone),
        h(
          'small',
          {},
          item.verdict?.status === 'COUNTEREXAMPLE'
            ? 'Runtime confirmation not established'
            : 'Recorded verdict · not run by this app',
        ),
      ),
    ),
    h(
      'p',
      { class: 'subtitle' },
      item.description || 'Explore the exact captured model and the evidence available for this case.',
    ),
    h(
      'div',
      { class: 'metrics' },
      metric((before || active()?.snapshot)?.schema?.tables?.length, 'tables'),
      metric(
        active()?.snapshot
          ? active().snapshot.plan.nodes.length
          : `${before?.plan?.nodes?.length ?? '—'} → ${after?.plan?.nodes?.length ?? '—'}`,
        'operators',
      ),
      metric(item.verdict?.row_bound ?? item.coverageBounds?.row_bound, 'rows / table'),
      metric(item.verdict?.task_bound ?? item.coverageBounds?.task_bound, 'task bound'),
      metric(item.revision?.slice(0, 11) || 'unrecorded', 'capture revision'),
    ),
  );
  const issues = [...new Set([...(item.issues || []), ...(active()?.issues || [])])];
  if (issues.length) $('#case-header').append(h('div', { class: 'callout warn' }, issues.join(' · ')));
}
function renderTabs() {
  const count = Object.keys(state.current?.verdict?.witness || state.current?.trace?.witness || {}).length;
  $('#tabs').replaceChildren(
    ...[
      ['compare', 'Plan comparison', ''],
      ['witness', 'Counterexample', count || ''],
      ['evidence', 'Evidence & scope', ''],
    ].map(([id, label, n]) =>
      button(
        [label, n && h('span', { class: 'tab-count' }, n)],
        () => {
          state.tab = id;
          renderTabs();
          renderView();
        },
        `tab ${state.tab === id ? 'active' : ''}`,
        { 'aria-pressed': state.tab === id },
      ),
    ),
  );
}
function renderView() {
  const view = $('#view');
  if (!state.current) {
    view.replaceChildren(
      empty('No artifacts open.', 'Choose “Open artifacts” or restore the example collection.'),
    );
    return;
  }
  const children = [];
  if (state.current.results) {
    const select = h(
      'select',
      {
        'aria-label': 'Result slot',
        onchange: (event) => {
          state.slot = Number(event.target.value);
          state.selected = null;
          state.scales = {};
          renderHeader();
          renderView();
        },
      },
      ...state.current.results.map((_, i) =>
        h('option', { value: i, selected: state.slot === i }, `Result slot ${i}`),
      ),
    );
    children.push(
      h(
        'div',
        { class: 'bundle-strip' },
        badge('Joint buffered result'),
        select,
        'The saved verdict belongs to the complete tuple, not to this slot alone.',
      ),
    );
  }
  children.push(
    state.tab === 'compare' ? compareView() : state.tab === 'witness' ? witnessView() : evidenceView(),
  );
  view.replaceChildren(...children);
}

function compareView() {
  const mode = h(
    'div',
    { class: 'segmented' },
    ...['operators', 'stages'].map((value) =>
      button(
        title(value),
        () => {
          state.mode = value;
          state.selected = null;
          state.scales = {};
          state.focus = false;
          renderView();
        },
        state.mode === value ? 'active' : '',
        { 'aria-pressed': state.mode === value },
      ),
    ),
  );
  const focus = button(
    state.focus ? 'Show full context' : 'Focus neighborhood',
    () => {
      state.focus = !state.focus;
      state.scales = {};
      renderView();
    },
    `focus-button ${state.focus ? 'active' : ''}`,
    { disabled: !state.selected, 'aria-pressed': state.focus },
  );
  return h(
    'div',
    { class: 'compare-layout' },
    h(
      'div',
      { class: 'graph-workspace' },
      h('div', { class: 'view-toolbar' }, mode, focus),
      active()?.snapshot
        ? graphPanel('unpaired')
        : h('div', { class: 'graph-pair' }, graphPanel('before'), graphPanel('after')),
      h(
        'div',
        { class: 'graph-legend' },
        h('span', {}, h('i', { class: 'legend-line' }), 'Recorded dataflow'),
        h('span', {}, h('i', { class: 'legend-line dashed' }), 'Subplan dependency'),
        h('span', {}, 'IDs are local to each plan · no inferred alignment'),
      ),
    ),
    auditDesk(),
  );
}

function graphPanel(side) {
  const snap = snapshot(side);
  const graph = topology(snap, state.mode === 'stages');
  const focused = state.focus && state.selected?.side === side ? state.selected.id : null;
  const arranged = layout(graph, focused);
  const viewport = h('div', { class: 'graph-viewport', 'data-side': side });
  const surface = h('div', { class: 'graph-surface' });
  const diagram = svg('svg', {
    viewBox: `0 0 ${arranged.width} ${arranged.height}`,
    role: 'group',
    'aria-label': `${side} ${state.mode} graph`,
  });
  const related = new Set();
  if (state.selected?.side === side)
    for (const edge of graph.edges) {
      if (edge.to === state.selected.id) related.add(edge.from);
      if (edge.from === state.selected.id) related.add(edge.to);
    }
  for (const edge of arranged.edges) {
    const a = arranged.positions.get(edge.from),
      b = arranged.positions.get(edge.to);
    const x1 = a.x + 88,
      y1 = a.y + 72,
      x2 = b.x + 88,
      y2 = b.y;
    const connected =
      state.selected?.side === side && (edge.from === state.selected.id || edge.to === state.selected.id);
    diagram.append(
      svg('path', {
        d: `M ${x1} ${y1} C ${x1} ${y1 + 22}, ${x2} ${y2 - 22}, ${x2} ${y2}`,
        class: `graph-edge ${edge.subplan ? 'subplan' : ''} ${connected ? 'connected' : ''}`,
      }),
    );
    diagram.append(
      svg('path', {
        d: `M ${x2 - 3} ${y2 - 5} L ${x2} ${y2} L ${x2 + 3} ${y2 - 5}`,
        class: `graph-edge ${connected ? 'connected' : ''}`,
      }),
    );
    if (edge.label)
      diagram.append(
        svg(
          'text',
          { x: (x1 + x2) / 2 + 5, y: (y1 + y2) / 2 - 1, class: 'edge-label' },
          edge.label.replaceAll('_', ' '),
        ),
      );
  }
  for (const node of arranged.nodes) {
    const { x, y } = arranged.positions.get(node.id);
    const selected = state.selected?.side === side && state.selected.id === node.id;
    const stage = snap?.stage_graph?.stages.find((s) => s.nodes.includes(node.id));
    const detail = summary(node, snap);
    const card = svg(
      'g',
      {
        transform: `translate(${x} ${y})`,
        class: `node-card ${selected ? 'selected' : ''} ${related.has(node.id) ? 'neighbor' : ''}`,
        role: 'button',
        tabindex: '0',
        'aria-label': `${side} ${node.op} ${node.id}`,
        'aria-pressed': selected,
      },
      svg('title', {}, `${title(node.op)} ${node.id}\n${detail}`),
      svg('rect', { width: 176, height: 72, rx: 7, class: 'node-bg' }),
      svg('circle', { cx: 88, cy: 0, r: 2.3, class: 'node-port' }),
      svg('circle', { cx: 88, cy: 72, r: 2.3, class: 'node-port' }),
      svg('text', { x: 12, y: 25, class: 'node-symbol' }, own(symbols, node.op, '◇')),
      svg('text', { x: 36, y: 25, class: 'node-title' }, title(node.op)),
      svg(
        'text',
        { x: 163, y: 25, 'text-anchor': 'end', class: 'node-id' },
        node.id.length > 9 ? `${node.id.slice(0, 8)}…` : node.id,
      ),
      svg(
        'text',
        { x: 12, y: 46, class: 'node-detail' },
        detail.length > 31 ? `${detail.slice(0, 30)}…` : detail,
      ),
      svg(
        'text',
        { x: 12, y: 61, class: 'stage-indicator' },
        node.root
          ? 'RESULT BOUNDARY'
          : stage
            ? `STAGE ${stage.id}`
            : node.phase
              ? node.phase.toUpperCase()
              : '',
      ),
    );
    const select = () => {
      const scroll = [...document.querySelectorAll('.graph-viewport')].map((v) => [
        v.dataset.side,
        v.scrollLeft,
        v.scrollTop,
      ]);
      state.selected = { side, id: node.id };
      renderView();
      for (const [s, left, top] of scroll) {
        const v = document.querySelector(`.graph-viewport[data-side="${s}"]`);
        if (v) {
          v.scrollLeft = left;
          v.scrollTop = top;
        }
      }
    };
    card.addEventListener('click', select);
    card.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        select();
      }
    });
    diagram.append(card);
  }
  if (!arranged.nodes.length)
    viewport.append(
      empty(
        snap ? 'No recorded stages.' : 'Snapshot not supplied.',
        snap && 'The logical side has no StageGraph.',
      ),
    );
  else {
    surface.append(diagram);
    viewport.append(surface);
  }
  const zoomText = h('span');
  let scale = state.scales[side] || 1;
  function resize(value) {
    scale = Math.max(0.2, Math.min(2, value));
    state.scales[side] = scale;
    diagram.setAttribute('width', String(arranged.width * scale));
    diagram.setAttribute('height', String(arranged.height * scale));
    zoomText.textContent = `${Math.round(scale * 100)}%`;
  }
  requestAnimationFrame(() => {
    const initial = !state.scales[side];
    resize(state.scales[side] || 1);
    if (initial) viewport.scrollLeft = Math.max(0, (arranged.width * scale - viewport.clientWidth) / 2);
  });
  const actualMode = state.mode === 'stages' && snap?.stage_graph ? 'stage topology' : 'operator DAG';
  return h(
    'section',
    { class: 'graph-panel' },
    h(
      'div',
      { class: 'graph-panel-header' },
      h('strong', {}, h('i', { class: `side-dot ${side}` }), sideLabel(side)),
      h(
        'small',
        {},
        side === 'before'
          ? 'Logical boundary'
          : side === 'after'
            ? 'Pre-physical boundary'
            : 'Role not assigned',
      ),
    ),
    viewport,
    h(
      'div',
      { class: 'graph-controls' },
      h(
        'span',
        {},
        `${arranged.nodes.length} nodes · ${actualMode}${arranged.hidden ? ` · ${arranged.hidden} hidden` : ''}`,
      ),
      h(
        'div',
        {},
        button('−', () => resize(scale - 0.15), '', { 'aria-label': `Zoom out ${side}` }),
        zoomText,
        button('+', () => resize(scale + 0.15), '', { 'aria-label': `Zoom in ${side}` }),
        button('Fit', () => resize((viewport.clientWidth - 12) / arranged.width), '', {
          'aria-label': `Fit ${side} graph`,
        }),
      ),
    ),
    ...arranged.issues.map((issue) => h('div', { class: 'callout warn' }, issue)),
  );
}

function field(label, value, code = false) {
  return h(
    'div',
    { class: 'field' },
    h('label', {}, label),
    h(
      code ? 'code' : 'div',
      { class: 'field-value' },
      typeof value === 'object' ? json(value) : String(value ?? '—'),
    ),
  );
}
function auditDesk() {
  const side = state.selected?.side,
    snap = snapshot(side);
  const node = topology(snap, state.mode === 'stages').nodes.find((n) => n.id === state.selected?.id);
  if (!node)
    return h(
      'aside',
      { class: 'audit-desk' },
      empty('Select an operator.', 'Its recorded fields, model trace and source locations will appear here.'),
    );
  const body = h('div', { class: 'desk-body' });
  const isStage = state.mode === 'stages' && !!snap.stage_graph;
  const rawNode = isStage
    ? snap.stage_graph.stages.find((n) => n.id === node.id)
    : snap.plan.nodes.find((n) => n.id === node.id);
  const separator = () => h('hr', { class: 'desk-separator' });
  if (state.desk === 'raw')
    body.append(
      h(
        'p',
        { class: 'small-note' },
        'Decoded snapshot fields. Large numeric lexemes remain exact; omitted fields are not inferred.',
      ),
      h('pre', { class: 'raw' }, json(rawNode)),
    );
  else if (state.desk === 'rows') {
    const events = active()?.trace?.trace?.[side]?.operators?.filter((event) => event.node === node.id) || [];
    if (!events.length)
      body.append(
        empty('No operator trace recorded.', 'A proof verdict alone does not contain a concrete execution.'),
      );
    else
      for (const [i, event] of events.entries()) {
        body.append(
          h('div', { class: 'section-caption' }, scopeName(event.scope)),
          familyCard(event.result, `${side}:${node.id}:${i}`, 'Modeled output'),
        );
      }
  } else if (state.desk === 'source') {
    const files = sourcePaths(node.op);
    body.append(
      h('div', { class: 'section-caption' }, 'Operator audit locations'),
      h(
        'p',
        { class: 'small-note' },
        'Navigation pointers only. Source text is not bundled or fetched. Review at the artifact producer revision, not an unrelated checkout.',
      ),
      separator(),
    );
    for (const path of files)
      body.append(
        h(
          'div',
          { class: 'source-map' },
          h('code', {}, `verification/${path}`),
          button(
            'Copy repository path',
            () => copy(`ydb/core/kqp/opt/rbo/verification/${path}`),
            'copy-button',
          ),
        ),
      );
    body.append(
      separator(),
      field('Capture / verdict revision', state.current.revision || 'Not recorded'),
      h(
        'div',
        { class: 'callout' },
        'Audit vertically: admission → operator semantics → composition → encoding equivalence → evidence.',
      ),
    );
  } else {
    body.append(h('div', { class: 'section-caption' }, 'Recorded semantics'));
    if (isStage) {
      body.append(
        field('Stage inputs', node.inputs),
        field('Stage outputs', node.outputs),
        field('Source storage', node.source_storage),
      );
      for (const id of node.nodes)
        body.append(
          button(
            `${id} · ${title(snap.plan.nodes.find((n) => n.id === id)?.op)}`,
            () => {
              state.mode = 'operators';
              state.selected = { side, id };
              state.scales = {};
              renderView();
            },
            'list-button',
          ),
        );
      body.append(separator(), h('div', { class: 'section-caption' }, 'Routing connections'));
      for (const edge of snap.stage_graph.edges.filter(
        (e) => e.producer === node.id || e.consumer === node.id,
      ))
        body.append(field(`${edge.producer} → ${edge.consumer}`, edge, true));
      body.append(
        h(
          'p',
          { class: 'small-note' },
          'Stage topology is recorded. Task execution and order are only shown when a concrete trace supplies them.',
        ),
      );
    } else {
      if (node.op === 'join') {
        body.append(field('Join kind', title(node.kind)), field('Inputs', `${node.left}  ⋈  ${node.right}`));
        for (const key of node.keys || [])
          body.append(field('Key pair', `${key.left}\n= ${key.right}`, true));
        body.append(field('Residual predicate (abbreviated)', expression(node.predicate), true));
        body.append(
          h(
            'p',
            { class: 'small-note' },
            'Key and predicate fields come from the capture. Use Raw IR for every exact flag; NULL/error behavior is defined by the linked audit locations, not this display.',
          ),
        );
      } else
        for (const [key, value] of Object.entries(node))
          if (!['id', 'op', 'root', 'columns'].includes(key))
            body.append(field(title(key), value, typeof value === 'object'));
      if (node.columns) {
        body.append(
          separator(),
          h(
            'div',
            { class: 'section-caption' },
            node.op === 'scan' ? 'Column bindings' : 'Projection expressions',
          ),
        );
        for (const column of node.columns)
          body.append(
            field(column.output, column.expression ? expression(column.expression) : column.source, true),
          );
        if (node.op === 'project')
          body.append(
            h(
              'p',
              { class: 'small-note' },
              'Expressions above are abbreviated. Raw IR includes exact types, flags, fingerprints and full expressions.',
            ),
          );
      }
      if (node.op === 'scan') {
        const table = snap.schema.tables.find((t) => t.name === node.table);
        if (table) {
          body.append(separator(), h('div', { class: 'section-caption' }, 'Captured table schema'));
          for (const column of table.columns)
            body.append(
              h(
                'div',
                { class: 'schema-line' },
                h('code', {}, column.name),
                h('span', {}, `${column.type}${column.nullable ? ' ?' : ''}`),
              ),
            );
          body.append(field('Unique keys', table.unique_keys, true));
        }
      }
      body.append(
        separator(),
        h('div', { class: 'section-caption' }, 'Bounded representation'),
        h(
          'div',
          { class: 'callout' },
          'Relations contain candidate row slots with a presence condition and typed values (NULL flag + payload). Outcomes also carry enablement, errors and choices.',
        ),
        h(
          'p',
          { class: 'small-note' },
          '“Model rows” shows saved concrete probes. Symbolic predicates and row lineage are not exported by the current trace; this viewer does not reconstruct them.',
        ),
      );
      const neighbors = topology(snap).edges.filter((e) => e.to === node.id || e.from === node.id);
      if (neighbors.length) {
        body.append(separator(), h('div', { class: 'section-caption' }, 'Immediate dataflow'));
        for (const edge of neighbors)
          body.append(
            button(
              `${edge.from} → ${edge.to}${edge.subplan ? ' · subplan' : ''}`,
              () => {
                state.selected = { side, id: edge.from === node.id ? edge.to : edge.from };
                renderView();
              },
              'list-button',
            ),
          );
      }
    }
  }
  return h(
    'aside',
    { class: 'audit-desk' },
    h(
      'div',
      { class: 'desk-heading' },
      h('span', { class: 'eyebrow' }, 'THE OPERATOR AUDIT DESK'),
      h(
        'div',
        { class: 'desk-title' },
        h('span', { class: 'operator-symbol' }, own(symbols, node.op, '◇')),
        h('h2', {}, title(node.op)),
        h('code', {}, node.id),
      ),
      h('p', {}, `${sideLabel(side)} · ${summary(node, snap)}`),
    ),
    h(
      'div',
      { class: 'desk-tabs' },
      ...[
        ['fields', 'Fields'],
        ['rows', 'Model rows'],
        ['raw', 'Raw IR'],
        ['source', 'Source'],
      ].map(([key, label]) =>
        button(
          label,
          () => {
            state.desk = key;
            renderView();
          },
          state.desk === key ? 'active' : '',
          { 'aria-pressed': state.desk === key },
        ),
      ),
    ),
    body,
  );
}
function sourcePaths(op) {
  const specific = own(
    {
      join: ['rbo_verifier/join.py', 'ut/test_join.py'],
      aggregate: ['rbo_verifier/aggregate.py'],
      sort: ['rbo_verifier/sort_strategy.py', 'rbo_verifier/sort_network.py'],
      stage: ['rbo_verifier/stages.py'],
    },
    op,
    [],
  );
  return [...specific, 'rbo_verifier/relation.py', 'rbo_verifier/ir.py', 'PLAN.md', 'TRUSTED_CORE.md'];
}
function scopeName(scope) {
  return !scope
    ? 'Recorded outcome family'
    : scope.kind === 'stage_task'
      ? `${scope.stage} / task ${scope.task}`
      : Object.entries(scope)
          .map(([k, v]) => `${k}: ${typeof v === 'object' ? json(v) : v}`)
          .join(' · ');
}

function cell(value) {
  if (value === null) return h('span', { class: 'null-value' }, 'NULL');
  if (value === undefined) return h('span', { class: 'null-value' }, 'not recorded');
  if (typeof value === 'object') return json(value);
  return String(value);
}
function tableView(columns, rows) {
  if (!rows.length) return h('div', { class: 'empty-table' }, '∅  No rows');
  return h(
    'div',
    { class: 'table-scroll' },
    h(
      'table',
      {},
      h('thead', {}, h('tr', {}, ...columns.map((c) => h('th', {}, c)))),
      h('tbody', {}, ...rows.map((row) => h('tr', {}, ...row.map((value) => h('td', {}, cell(value)))))),
    ),
  );
}
function familyCard(family, key, heading) {
  if (!family) return h('div', { class: 'data-card' }, empty('No outcome family recorded.'));
  const outcomes = family.outcomes || [];
  const index = Math.min(state.outcomes[key] || 0, Math.max(0, outcomes.length - 1));
  const outcome = outcomes[index];
  const select = h(
    'select',
    {
      'aria-label': `${heading} outcome`,
      onchange: (event) => {
        state.outcomes[key] = Number(event.target.value);
        renderView();
      },
    },
    ...outcomes.map((o, i) =>
      h(
        'option',
        { value: i, selected: i === index },
        `Outcome ${o.index ?? i} · ${o.status || 'status unrecorded'}`,
      ),
    ),
  );
  const present = outcome?.rows?.filter((row) => row.present === true) || [];
  const columns = family.columns || [];
  const rows = present.map((row) => columns.map((c) => row.values?.find((v) => v.column === c.name)?.value));
  const card = h(
    'div',
    { class: 'data-card' },
    h(
      'div',
      { class: 'data-card-header' },
      h('strong', {}, heading),
      h('span', {}, `${outcomes.length} recorded outcomes`),
    ),
  );
  if (!outcome) {
    card.append(h('div', { class: 'empty-table' }, 'No enabled outcomes in this trace.'));
    return card;
  }
  card.append(
    h('div', { class: 'outcome-controls' }, select, badge(outcome.sequence ? 'Sequence' : 'Bag', 'neutral')),
  );
  if (outcome.status === 'error')
    card.append(
      h(
        'div',
        { class: 'callout error' },
        'Query error. Candidate row payloads are not successful query output.',
      ),
    );
  else if (outcome.status === 'success')
    card.append(
      tableView(
        columns.map((c) => c.name),
        rows,
      ),
    );
  else card.append(h('div', { class: 'callout warn' }, 'Outcome status is not established.'));
  card.append(
    h(
      'div',
      { class: 'outcome-meta' },
      `${present.length} present / ${outcome.rows?.length || 0} candidate slots · ${family.disabled_outcome_count || 0} disabled outcomes omitted`,
      h(
        'details',
        {},
        h('summary', {}, 'Choices, order and exact outcome'),
        h('pre', { class: 'raw' }, json(outcome)),
      ),
    ),
  );
  return card;
}
function witnessView() {
  const item = state.current,
    trace = active()?.trace;
  const witness = item.verdict?.witness || trace?.witness;
  const section = h(
    'div',
    { class: 'workspace-content' },
    h(
      'div',
      { class: 'content-heading' },
      h(
        'div',
        {},
        h('h2', {}, 'A small database. A visible disagreement.'),
        h('p', {}, 'Recorded model values, not a simulation or a runtime replay.'),
      ),
      badge(trace?.trace ? 'Concrete trace available' : 'Trace unavailable', trace?.trace ? '' : 'neutral'),
    ),
  );
  if (item.results)
    section.append(
      h(
        'div',
        { class: 'callout warn' },
        'The current inspector does not export joint bundle traces. This slot cannot be treated as an independent counterexample.',
      ),
    );
  if (!witness) {
    section.append(
      empty(
        'No counterexample database is attached.',
        'A bounded proof does not provide an example execution. Use the evidence panel to inspect reachability diagnostics and proof scope.',
      ),
    );
    return section;
  }
  section.append(
    h(
      'div',
      { class: 'callout warn' },
      'A symbolic candidate is not a confirmed optimizer bug. Abstractions, the row bound and the exact captured boundary all matter.',
    ),
  );
  const database = h('div', { class: 'witness-grid' });
  for (const [table, rows] of Object.entries(witness)) {
    const schema = snapshot('before')?.schema?.tables?.find((t) => t.name === table);
    const columns = [
      ...new Set([...(schema?.columns.map((c) => c.name) || []), ...rows.flatMap((row) => Object.keys(row))]),
    ];
    database.append(
      h(
        'div',
        { class: 'data-card' },
        h(
          'div',
          { class: 'data-card-header' },
          h('strong', { title: table }, shortTable(table)),
          h('span', {}, `${rows.length} rows`),
        ),
        tableView(
          columns,
          rows.map((row) => columns.map((c) => row[c])),
        ),
      ),
    );
  }
  section.append(database);
  if (!trace?.trace) {
    section.append(
      empty(
        'The database is saved, but no operator trace is attached.',
        'Generate an inspector trace with the saved verdict to keep the candidate database fixed.',
      ),
    );
    return section;
  }
  section.append(
    h(
      'div',
      { class: 'content-heading' },
      h(
        'div',
        {},
        h('h2', {}, 'At the result boundary'),
        h(
          'p',
          {},
          'An unmatched outcome has no equivalent outcome on the other side—not merely a different schedule.',
        ),
      ),
    ),
  );
  for (const mismatch of trace.mismatches || [])
    section.append(
      h(
        'div',
        { class: 'callout error' },
        `${title(mismatch.source)}${mismatch.outcome == null ? '' : ` outcome ${mismatch.outcome}`}: ${mismatch.reason || 'no matching outcome on the other side'}.`,
      ),
    );
  section.append(
    h(
      'div',
      { class: 'outcome-pair' },
      familyCard(
        trace.trace.comparison?.before || trace.trace.before?.boundary,
        'boundary-before',
        'Initial output',
      ),
      familyCard(
        trace.trace.comparison?.after || trace.trace.after?.boundary,
        'boundary-after',
        'Final output',
      ),
    ),
    h(
      'details',
      {},
      h('summary', {}, 'Exact recorded mismatch descriptors'),
      h('pre', { class: 'raw' }, json(trace.mismatches)),
    ),
    h(
      'p',
      { class: 'small-note' },
      'Follow intermediate results in Plan comparison → select an operator → Model rows. Outcome indices are local; independent selections are not asserted to form one compatible global schedule.',
    ),
  );
  return section;
}

function evidenceView() {
  const item = state.current,
    verdict = item.verdict;
  const hasPair = !!(snapshot('before') && snapshot('after'));
  const diagnostic = verdict?.nonempty_output_diagnostic;
  const row = (name, value) =>
    h('div', { class: 'evidence-row' }, h('span', {}, name), h('strong', {}, value ?? 'Not recorded'));
  const left = h(
    'div',
    { class: 'evidence-panel' },
    h('h3', {}, 'What this evidence establishes'),
    row('Saved verifier status', verdict?.status),
    row(
      'Comparison boundary',
      verdict?.comparison_scope || (hasPair ? 'Bound Initial / Final pair' : 'Not established'),
    ),
    row('Row bound', verdict?.row_bound),
    row('Task bound', verdict?.task_bound),
    row(
      'Semantic mode',
      hasPair
        ? snapshot('before')?.semantic_mode ||
            snapshot('after')?.semantic_mode ||
            'Default modeled outcome-language equality'
        : 'Not established',
    ),
    row('Model trace', item.trace?.trace ? 'Attached · modeled values' : 'Not attached'),
    row('Runtime confirmation', 'Not established by this viewer'),
    h('hr', { class: 'desk-separator' }),
    h('h3', {}, 'Nonempty-output reachability'),
    row('Initial', diagnostic?.before?.status),
    row('Final', diagnostic?.after?.status),
    row('Model domain', diagnostic?.model_domain?.status),
    h(
      'p',
      { class: 'small-note' },
      'SAT means some modeled successful nonempty execution. It is neither an equivalence proof nor exhaustive branch coverage. UNSAT can coexist with a valid bounded proof.',
    ),
  );
  const right = h(
    'div',
    { class: 'evidence-panel' },
    h('h3', {}, 'Artifact provenance'),
    row('Binding', item.binding),
    row('Capture / verdict revision', item.revision),
    h(
      'p',
      { class: 'small-note' },
      'SHA-256 checks bind file bytes to supplied digests; they do not authenticate the producer or establish semantic correctness. Rendering-derived semantic digests are revision-specific and are not raw file hashes. A trace may have been produced later; see its provenance notes.',
    ),
  );
  const artifacts = new Map();
  for (const artifact of item.provenance || []) {
    if (!artifacts.has(artifact.name) || artifact.role !== 'hash') artifacts.set(artifact.name, artifact);
  }
  for (const artifact of artifacts.values())
    right.append(
      h(
        'div',
        { class: 'artifact-row' },
        h('div', {}, h('strong', {}, artifact.name), h('span', {}, artifact.role)),
        h('code', {}, artifact.sha256),
        h('small', {}, artifact.hashBasis || 'Imported bytes'),
      ),
    );
  if (item.provenance_notes) right.append(h('div', { class: 'callout' }, item.provenance_notes));
  if (item.trace_producer)
    right.append(
      h(
        'details',
        {},
        h('summary', {}, 'Trace producer metadata'),
        h('pre', { class: 'raw' }, json(item.trace_producer)),
      ),
    );
  const section = h(
    'div',
    { class: 'workspace-content' },
    h(
      'div',
      { class: 'content-heading' },
      h(
        'div',
        {},
        h('h2', {}, 'Evidence, with its boundaries intact.'),
        h('p', {}, 'Missing evidence is not a negative result. A saved formula is not a solver transcript.'),
      ),
    ),
    h('div', { class: 'evidence-grid' }, left, right),
  );
  for (const issue of item.issues || []) section.append(h('div', { class: 'callout warn' }, issue));
  if (item.query)
    section.append(
      h(
        'details',
        { open: true },
        h('summary', {}, 'Exact captured query'),
        h('pre', { class: 'raw query-text' }, item.query),
      ),
    );
  if (verdict)
    section.append(
      h(
        'details',
        {},
        h('summary', {}, 'Saved verdict · lossless decoded JSON'),
        h('pre', { class: 'raw' }, json(verdict)),
      ),
    );
  return section;
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text);
    notice('Repository path copied. Open it at the artifact producer revision.');
  } catch {
    notice(`Copy this repository path: ${text}`);
  }
}
async function openFiles(files) {
  try {
    if (files.length > 256 || [...files].reduce((sum, f) => sum + f.size, 0) > 64 * 1024 * 1024)
      throw new Error('Import at most 256 files and 64 MiB at once.');
    if ([...files].some((f) => f.size > 16 * 1024 * 1024))
      throw new Error('An artifact exceeds 16 MiB. Import snapshots and traces, not the full SMT formula.');
    const selected = await Promise.all(
      [...files].map(async (file) => {
        const bytes = new Uint8Array(await file.arrayBuffer());
        return {
          name: file.webkitRelativePath || file.name,
          text: new TextDecoder('utf-8', { fatal: true }).decode(bytes),
          bytes,
        };
      }),
    );
    const result = await loadArtifacts(selected);
    if (!result.cases.length)
      throw new Error(result.issues.join(' · ') || 'No snapshot pairs or recognized cases found.');
    state.cases = result.cases;
    $('#search').value = '';
    setCase(result.cases[0]);
    notice(result.issues.join(' · '));
  } catch (error) {
    notice(`Could not open artifacts: ${error.message}`);
  }
}
async function loadDemo() {
  try {
    const response = await fetch('./demo/manifest.json');
    if (!response.ok) throw new Error(`Example manifest: HTTP ${response.status}`);
    const bytes = new Uint8Array(await response.arrayBuffer());
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    const manifest = parseLosslessJson(text);
    const names = new Set(
      manifest.cases.flatMap((item) =>
        ['before', 'after', 'verdict', 'trace', 'query'].map((key) => item[key]).filter(Boolean),
      ),
    );
    const files = [{ name: 'manifest.json', text, bytes }];
    for (const name of names) {
      // Only the bundled manifest is fetched. Imported manifests never trigger I/O.
      if (!/^[a-zA-Z0-9_.-]+$/.test(name)) throw new Error('Invalid bundled artifact filename');
      const response = await fetch(`./demo/${name}`);
      if (!response.ok) throw new Error(`${name}: HTTP ${response.status}`);
      const bytes = new Uint8Array(await response.arrayBuffer());
      files.push({ name, bytes, text: new TextDecoder('utf-8', { fatal: true }).decode(bytes) });
    }
    const result = await loadArtifacts(files);
    if (!result.cases.length) throw new Error(result.issues.join(' · ') || 'No examples loaded');
    state.cases = result.cases;
    $('#search').value = '';
    setCase(result.cases[0]);
    notice(result.issues.join(' · '));
  } catch (error) {
    notice(
      `Examples could not be loaded: ${error.message}. Serve this directory over localhost; see README.md.`,
    );
    render();
  }
}

$('#import-button').addEventListener('click', () => $('#file-input').click());
$('#file-input').addEventListener('change', (event) => openFiles(event.target.files));
$('#demo-button').addEventListener('click', loadDemo);
$('#search').addEventListener('input', renderCases);
$('#help-button').addEventListener('click', () => $('#help-dialog').showModal());
$('#close-help').addEventListener('click', () => $('#help-dialog').close());
document.addEventListener('keydown', (event) => {
  if (
    event.key === '/' &&
    !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName) &&
    !$('#help-dialog').open
  ) {
    event.preventDefault();
    $('#search').focus();
  }
});
let dragDepth = 0;
document.addEventListener('dragenter', (event) => {
  if (event.dataTransfer.types.includes('Files')) {
    event.preventDefault();
    if (!dragDepth++)
      document.body.append(h('div', { class: 'file-drop' }, 'Drop saved artifacts to explore'));
  }
});
document.addEventListener('dragover', (event) => event.preventDefault());
document.addEventListener('dragleave', () => {
  if (--dragDepth <= 0) {
    dragDepth = 0;
    $('.file-drop')?.remove();
  }
});
document.addEventListener('drop', (event) => {
  event.preventDefault();
  dragDepth = 0;
  $('.file-drop')?.remove();
  openFiles(event.dataTransfer.files);
});
render();
await loadDemo();
