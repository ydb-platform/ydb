import { h, button, empty, badge, json } from './dom.mjs';
import { formulaFields, formulaIndex, readableTerm } from './formula-model.mjs';
import { shortTable } from './graph.mjs';

const indexes = new WeakMap();
const scopeName = scope => scope.kind === 'stage_task' ? `${scope.stage} / task ${scope.task}` : json(scope);

export function formulaView(document, side, node, snapshot) {
  if (!document) return empty('No operator formulas attached.',
    'Export with kqp_rbo_inspect formulas BEFORE AFTER --rows N, then import the JSON alongside those exact snapshots.');
  if (!indexes.has(document)) indexes.set(document, formulaIndex(document));
  const terms = indexes.get(document);
  const events = document[side]?.operators.filter(event => event.node === node.id) || [];
  if (!events.length) return empty('This node has no separately observed formula.',
    'The kernel may fuse its evaluation into another operator. No standalone semantics are reconstructed here.');
  const section = h('div', { class: 'formula-view' });
  let eventIndex = 0, view = 'readable', current, currentLabel, field, history = [];
  const content = h('div'), controls = h('div', { class: 'formula-controls' });
  const jump = (id, label = `Dependency · ${id}`) => {
    history.push({current, currentLabel}); current = id; currentLabel = label; show();
  };
  const show = () => {
    const term = terms.get(current);
    content.replaceChildren(h('div', { class: 'formula-location' },
      h('strong', {}, currentLabel), badge(term.sort, 'neutral'), h('code', {}, current),
      history.length > 0 && button('← Parent', () => {
        ({current, currentLabel} = history.pop()); show();
      }, 'text-button')));
    if (view === 'readable') {
      content.append(h('pre', { class: 'formula-readable' }, readableTerm(terms, current)),
        h('p', { class: 'small-note' }, 'Readable abbreviation · @tN marks a shared or omitted subtree. Follow dependencies for its exact definition.'));
      const pending = [current], seen = new Set(), hints = new Map();
      while (pending.length && seen.size < 300 && hints.size < 8) {
        const id = pending.pop(); if (seen.has(id)) continue; seen.add(id);
        const term = terms.get(id);
        if (term.op === 'symbol') {
          const declaration = document.declarations.find(item => item.name === term.atom);
          if (typeof declaration?.hint === 'string') {
            let hint = declaration.hint;
            for (const table of snapshot?.schema?.tables || []) hint = hint.replaceAll(table.name, shortTable(table.name));
            hints.set(term.atom, {id, hint});
          }
        }
        if (!['forall', 'exists'].includes(term.op)) pending.push(...term.args.slice(0, 40));
      }
      if (hints.size) content.append(h('div', {class: 'formula-symbols'},
        h('small', {}, 'Global symbol hints · table names abbreviated'),
        [...hints].map(([name, item]) => button([h('code', {}, name), h('span', {}, item.hint)],
          () => jump(item.id), 'formula-symbol'))));
    }
    if (view === 'exact') content.append(h('pre', { class: 'raw' }, json(term)),
      h('p', { class: 'small-note' }, 'Exact typed DAG record, not standalone SMT-LIB. Quantifier arguments bind symbols in their final argument (the body).'));
    content.append(h('div', { class: 'formula-dag' },
      h('div', { class: 'formula-node current' }, h('strong', {}, term.op),
        h('code', {}, term.atom == null ? `${current} : ${term.sort}` : String(term.atom))),
      term.args.length > 0 && h('div', { class: 'formula-arguments' }, term.args.slice(0, 40).map((id, index) =>
        button([h('small', {}, `Argument ${index + 1} · ${id}`), h('code', {}, readableTerm(terms, id, 1, 100))],
          () => jump(id), 'formula-node'))), term.args.length > 40 &&
        h('p', { class: 'small-note' }, `First 40 of ${term.args.length} arguments shown; all references are in Exact DAG.`)));
    const declaration = document.declarations.find(item => item.name === (term.atom ?? term.op)
      || (item.kind === 'product' && (item.constructor === term.op || item.fields.some(field => field.selector === term.op))));
    if (declaration) content.append(h('details', { open: true }, h('summary', {}, 'Symbol meaning / declaration'),
      h('pre', { class: 'raw' }, json(declaration)), declaration.kind === 'definition' && button('Inspect definition body',
        () => jump(declaration.body, `Definition of ${declaration.name}`), 'text-button')));
  };
  const chooseEvent = () => {
    const fields = formulaFields(events[eventIndex].result);
    controls.replaceChildren();
    if (!fields.length) { content.replaceChildren(empty('No symbolic outcomes observed.')); return; }
    const select = h('select', { 'aria-label': 'Formula field', onchange: event => {
      field = fields[Number(event.target.value)]; current = field.ref; currentLabel = field.label; history = []; show();
    } }, fields.map((field, index) => h('option', { value: index }, field.label)));
    field = fields.find(field => field.label.endsWith('· present') && terms.get(field.ref)?.op !== 'bool') || fields[0];
    select.value = String(fields.indexOf(field)); current = field.ref; currentLabel = field.label; history = [];
    controls.append(select);
    const tabs = h('div', { class: 'segmented' });
    const renderTabs = () => tabs.replaceChildren(...['readable', 'dependencies', 'exact'].map(key =>
      button({readable: 'Readable', dependencies: 'Dependencies', exact: 'Exact DAG'}[key], () => {
        view = key; renderTabs(); show();
      }, view === key ? 'active' : '', {'aria-pressed': view === key})));
    renderTabs(); controls.append(tabs); show();
  };
  section.append(h('p', { class: 'small-note' },
    `Actual kernel terms · ${document.row_bound} rows/table · ${document.task_bound} tasks · ${document.semantic_mode ?? 'default scalar model'}`),
    h('label', {}, 'Execution scope ', h('select', { 'aria-label': 'Formula scope', onchange: event => {
      eventIndex = Number(event.target.value); chooseEvent();
    } }, events.map((event, index) => h('option', { value: index }, scopeName(event.scope))))), controls, content);
  chooseEvent();
  section.append(h('details', {}, h('summary', {}, 'Outcome schema, choices and row slots'),
    h('pre', { class: 'raw' }, json(events.map(event => ({scope: event.scope, result: event.result}))))),
    h('details', {}, h('summary', {}, `Global context · ${document.assertions.length} assertions`),
      h('p', { class: 'small-note' }, 'Local terms depend on the complete model constraints. A row payload is meaningful only when its outcome is enabled, has no error, and the row is present.'),
      button('Inspect semantic mismatch', () => jump(document.comparison.counterexample, 'Whole-query semantic mismatch'), 'text-button'),
      document.comparison.soundness_exclusion && button('Inspect soundness exclusion',
        () => jump(document.comparison.soundness_exclusion, 'Whole-query soundness exclusion'), 'text-button'),
      h('select', { 'aria-label': 'Global assertion', onchange: event => {
        if (event.target.value) jump(event.target.value, 'Global model assertion');
      } }, h('option', { value: '' }, 'Choose an assertion…'),
      document.assertions.map((ref, index) => h('option', {value: ref}, `Assertion ${index} · ${ref}`)))));
  return section;
}
