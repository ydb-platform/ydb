import { h, badge, empty, json, tableView } from './dom.mjs';

export function outcomeTable(family, outcome) {
  if (!outcome) return empty('No enabled outcome recorded.');
  if (outcome.status === 'error')
    return h('div', { class: 'callout error' }, 'Query error · row payloads are not successful output.');
  if (outcome.status !== 'success') return empty('Outcome status is not established.');
  const columns = family.columns || [];
  const present = (outcome.rows || []).filter(row => row.present === true);
  return tableView(columns.map(c => c.name),
    present.map(row => columns.map(c => row.values?.find(v => v.column === c.name)?.value)));
}

export function familyCard(family, heading) {
  if (!family) return h('div', { class: 'data-card' }, empty('No outcome family recorded.'));
  const outcomes = family.outcomes || [];
  const content = h('div');
  const show = index => {
    const outcome = outcomes[index];
    content.replaceChildren(outcomeTable(family, outcome));
    if (outcome) content.append(h('div', { class: 'outcome-meta' },
      badge(outcome.sequence ? 'Sequence' : 'Bag', 'neutral'),
      `${outcome.rows.filter(row => row.present).length} present / ${outcome.rows.length} slots`,
      h('details', {}, h('summary', {}, 'Choices, order and exact outcome'),
        h('pre', { class: 'raw' }, json(outcome)))));
  };
  show(0);
  return h('div', { class: 'data-card' },
    h('div', { class: 'data-card-header' }, h('strong', {}, heading),
      h('span', {}, `${outcomes.length} enabled outcomes`)),
    outcomes.length > 1 && h('select', { 'aria-label': `${heading} outcome`,
      onchange: event => show(Number(event.target.value)) },
      outcomes.map((outcome, index) => h('option', { value: index },
        `Outcome ${outcome.index ?? index} · ${outcome.status}`))),
    content);
}
