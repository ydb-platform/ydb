import { h, button, badge, empty, json } from './dom.mjs';

export function localizationView(item, select) {
  const report = item.localization;
  if (!report) return h('div', {class: 'workspace-content'}, empty('No rule investigation attached.',
    'Run kqp_rbo_bisect, then import result.json and the referenced snapshot/verdict JSON files. The explorer never runs optimizer or solver commands.'));
  const records = new Map(report.comparisons.map(record => [record.id, record]));
  const findings = new Map(report.findings.map(finding => [finding.comparison, finding]));
  const row = record => {
    const finding = findings.get(record.id), pair = item.comparisons.find(pair => pair.id === record.id);
    const gap = ['UNKNOWN', 'UNSUPPORTED'].includes(record.verifier.status);
    const event = record.adjacent && report.events.find(event => event.ordinal === record.after);
    return button([
      h('code', {}, `${record.before} → ${record.after}`),
      h('strong', {}, event ? event.name : record.adjacent ? 'Final export / global suffix' : 'Midpoint interval'),
      event && h('small', {}, event.kind === 'ATOMIC_STAGE_COMMIT' ? 'Stage commit'
        : event.kind === 'RULE_APPLICATION' ? 'Rule application' : 'Recorded event'),
      badge(record.verifier.status, finding ? 'error' : gap ? 'warn' : 'neutral'),
      h('small', {}, pair?.verdict ? 'Raw verdict attached' : 'Report summary only'),
      'Inspect →',
    ], () => select(record.id), `rule-row ${finding ? 'failure' : gap ? 'gap' : ''}`);
  };
  const adjacent = report.comparisons.filter(record => record.adjacent).sort((a, b) => a.after - b.after);
  const unresolved = adjacent.filter(record => ['UNKNOWN', 'UNSUPPORTED'].includes(record.verifier.status));
  return h('div', {class: 'workspace-content'},
    h('div', {class: 'content-heading'}, h('div', {}, h('h2', {}, 'Which transformation changed the result?'),
      h('p', {}, `${report.events.length} recorded events · ${report.comparisons.length} comparisons`)),
      badge(`Reported coverage: ${report.completeness}`, report.completeness === 'COMPLETE' ? '' : 'warn')),
    h('div', {class: 'callout'},
      `Original-query observation: ${report.observation_kind || 'unresolved'}. Midpoint-first search checks both halves, including equivalent intervals: two bad edits can cancel. Only adjacent comparisons identify a single event. These are bounded model findings, not runtime-confirmed bugs.`),
    h('h3', {}, `${findings.size} reported non-equivalent steps`),
    h('div', {class: 'rule-list'}, [...findings.keys()].map(id => row(records.get(id)))),
    unresolved.length > 0 && h('details', {open: unresolved.length <= 3}, h('summary', {}, `${unresolved.length} unresolved adjacent checks`),
      h('div', {class: 'rule-list'}, unresolved.map(row))),
    h('details', {}, h('summary', {}, 'All steps in application order'), h('div', {class: 'rule-list'}, adjacent.map(row))),
    h('details', {}, h('summary', {}, 'Midpoint checks in search order'),
      h('div', {class: 'rule-list'}, report.comparisons.filter(record => !record.adjacent).map(row))),
    h('details', {}, h('summary', {}, 'Exact investigation report'), h('pre', {class: 'raw'}, json(report))));
}
