import { h, button, empty, badge, json, title, cell, tableView } from './dom.mjs';
import { outcomeTable } from './trace-view.mjs';
import { explainMismatch, formatCell } from './mismatch.mjs';
import { shortTable } from './graph.mjs';

const sideName = side => side === 'before' ? 'Before' : 'After';
const typed = value => value ? h('span', {title: `Exact payload: ${json(value.value)}`},
  formatCell(value), h('small', {}, ` · ${value.type}`)) : '∅';

function differenceRow(diff) {
  const row = (where, before, after) => h('tr', {}, h('th', {}, where),
    h('td', { class: 'difference-value' }, before), h('td', { class: 'difference-value' }, after));
  if (diff.kind === 'cell') return row(
    h('span', {}, `Position ${diff.position + 1} · ${diff.sourceColumn} ↔ ${diff.targetColumn}`,
      h('small', {class: 'mismatch-slots'}, `Candidate slots ${diff.sourceSlot} / ${diff.targetSlot} (left / right)`)),
    typed(diff.source), typed(diff.target));
  if (diff.kind === 'multiplicity') return row(
    h('span', {}, 'Row ', diff.values.map((value, index) => [index ? ', ' : '', typed(value)])),
    `${diff.sourceCount} copies`, `${diff.targetCount} copies`);
  if (diff.kind === 'row') return row(`Position ${diff.position + 1}`,
    diff.source ? json(diff.source.values) : 'No row', diff.target ? json(diff.target.values) : 'No row');
  if (diff.kind === 'status') return row('Result status', title(diff.sourceStatus), title(diff.targetStatus));
  if (diff.kind === 'order') return row('Row order', 'Same bag', 'Different sequence');
  return row('Enabled outcomes', diff.sourceCount, diff.targetCount);
}

function mismatchPanel(item, revealNode) {
  const trace = item.trace, descriptors = trace.mismatches || [];
  if (!descriptors.length) return empty('No unmatched outcome descriptor recorded.');
  const content = h('div', { class: 'mismatch-detail' });
  let selected = 0, target;
  const show = () => {
    const descriptor = descriptors[selected];
    const result = explainMismatch(trace, descriptor.source, descriptor.outcome, target);
    content.replaceChildren(h('div', { class: 'callout error' }, result.summary || descriptor.reason));
    if (result.targets?.length > 1) content.append(h('label', { class: 'mismatch-select' },
      'Compare with a recorded opposite outcome ',
      h('select', { 'aria-label': 'Opposite outcome', onchange: event => {
        target = event.target.value === '' ? undefined : Number(event.target.value); show();
      } }, h('option', { value: '', selected: target === undefined }, 'Choose an outcome…'),
      result.targets.map(option => h('option', { value: option.index, selected: target === option.index },
        `Outcome ${option.index} · ${option.status}`)))));
    if (result.differences?.length) content.append(h('div', { class: 'table-scroll mismatch-table' },
      h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Where it differs'),
        h('th', {}, sideName(result.source.side)),
        h('th', {}, sideName(result.target?.side || (result.source.side === 'before' ? 'after' : 'before'))))),
      h('tbody', {}, result.differences.map(differenceRow)))));
    if (result.source?.outcome) content.append(h('div', { class: 'outcome-pair' },
      [result.source, result.target].filter(Boolean).map(side => h('div', { class: 'data-card' },
        h('div', { class: 'data-card-header' },
          h('strong', {}, `${sideName(side.side)} · outcome ${side.index}`),
          button('Locate result node', () => revealNode(side.side, item[side.side]?.plan?.root), 'text-button')),
        outcomeTable(side.family, side.outcome)))));
    for (const issue of result.issues || []) content.append(h('p', { class: 'small-note' }, issue));
    content.append(h('p', { class: 'small-note' },
      'The saved descriptor identifies an unmatched result. Rows above explain one recorded comparison; they do not establish a compatible execution schedule or identify a faulty intermediate operator.'));
  };
  show();
  return h('section', { class: 'mismatch-panel' },
    h('div', { class: 'content-heading' }, h('div', {}, h('h2', {}, 'Exactly what differs'),
      h('p', {}, 'At the result boundary · positions and multiplicities, not guessed node matches.')),
      badge(trace.trace?.comparison?.semantics || 'Recorded comparison', 'neutral')),
    descriptors.length > 1 && h('select', { 'aria-label': 'Recorded mismatch', onchange: event => {
      selected = Number(event.target.value); target = undefined; show();
    } }, descriptors.map((descriptor, index) => h('option', { value: index },
      `${sideName(descriptor.source)} outcome ${descriptor.outcome} · unmatched`))),
    content, h('details', {}, h('summary', {}, 'Exact mismatch descriptors'),
      h('pre', { class: 'raw' }, json(descriptors))));
}

export function witnessView(item, revealNode) {
  const trace = item.trace, witness = item.verdict?.witness || trace?.witness;
  const section = h('div', { class: 'workspace-content' },
    h('div', { class: 'callout warn' },
      'Model counterexample · not runtime-confirmed. Bounds and semantic abstractions still apply.'));
  if (!witness) return h('div', { class: 'workspace-content' },
    empty('No counterexample database attached.', 'A proof verdict does not contain a concrete execution.'));
  if (trace?.trace) section.append(mismatchPanel(item, revealNode));
  else section.append(empty('No operator trace attached.',
    item.verdict?.comparison_scope?.startsWith('OPTIMIZER_TRANSFORMATION_')
      ? 'This is a transformation-boundary diagnostic. Its saved database and rule interval are available, but the whole-query witness tracer cannot replay this comparison scope.'
      : 'Generate an inspector witness trace with --verifier-verdict to keep this database fixed.'));
  const database = h('div', { class: 'witness-grid' });
  for (const [name, rows] of Object.entries(witness)) {
    const schema = item.before?.schema?.tables?.find(table => table.name === name);
    const columns = [...new Set([...(schema?.columns.map(column => column.name) || []),
      ...rows.flatMap(row => Object.keys(row))])];
    database.append(h('div', { class: 'data-card' },
      h('div', { class: 'data-card-header' }, h('strong', { title: name }, shortTable(name)),
        h('span', {}, `${rows.length} rows`)), tableView(columns, rows.map(row => columns.map(c => row[c])))));
  }
  section.append(h('details', { class: 'witness-database', open: !trace?.trace },
    h('summary', {}, `Counterexample database · ${Object.keys(witness).length} tables`), database));
  return section;
}
