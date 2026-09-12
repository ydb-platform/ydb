import assert from 'node:assert/strict';
import {createHash, webcrypto} from 'node:crypto';
import test from 'node:test';
import {loadArtifacts} from './artifacts.mjs';

globalThis.crypto ??= webcrypto;
const digest = text => createHash('sha256').update(text).digest('hex');
const file = (name, value) => ({name, text: JSON.stringify(value)});
const snapshot = {format: 'ydb-rbo-semantic-snapshot', version: 1,
  plan: {nodes: [{id: 'n', op: 'empty_source'}], root: 'n'}, schema: {tables: []}};
function fixture(rawStatus = 'COUNTEREXAMPLE', pairScope = false) {
  const before = file('run/completion/q.initial.json', snapshot);
  const anchor = {observation_snapshot_sha256: digest(before.text), observation_kind: 'bag'};
  const summary = {status: 'COUNTEREXAMPLE', row_bound: 2, task_bound: 2,
    ...(pairScope ? {comparison_scope: 'OPTIMIZER_TRANSFORMATION_PAIR', ...anchor} : {})};
  const after = file('run/completion/q.final.json', snapshot);
  const verdict = file('run/completion/verdict.json', {...summary, status: rawStatus});
  const reference = item => ({path: item.name.slice(4), sha256: digest(item.text)});
  const report = file('run/result.json', {format: 'ydb-rbo-transformation-localization', version: 1,
    status: 'LOCALIZED_FAILURES', strategy: 'divide-and-conquer', completeness: 'COMPLETE',
    row_bound: 2, task_bound: 2, events: [], events_total: 0, final_verifier: summary, observation_boundary: 0, ...anchor,
    boundaries: [{ordinal: 0, kind: 'INITIAL', artifacts: {snapshot: reference(before)}},
      {ordinal: 1, kind: 'FINAL', artifacts: {snapshot: reference(after)}}],
    comparisons: [{id: '0:1', before: 0, after: 1, adjacent: true, verifier: summary,
      artifacts: {verdict: reference(verdict)}}],
    findings: [{comparison: '0:1', status: 'COUNTEREXAMPLE', region: 'GLOBAL_SUFFIX_AFTER_TRANSFORMATIONS'}], gaps: []});
  return {report, before, after, verdict};
}

test('localization owns hash-bound evidence regardless of chooser order', async () => {
  const {report, before, after, verdict} = fixture('COUNTEREXAMPLE', true);
  for (const selected of [[report, before, after, verdict], [verdict, after, before, report]]) {
    const loaded = await loadArtifacts(selected);
    assert.deepEqual(loaded.issues, []);
    assert.equal(loaded.cases.length, 1); // No duplicate filename-grouped snapshot case.
    const entry = loaded.cases[0], pair = entry.comparisons[0];
    assert.equal(entry.binding, 'localization-report');
    assert.equal(entry.comparisons.length, 1);
    assert.deepEqual(pair.interval, {before: 0, after: 1});
    assert.deepEqual(pair.before, snapshot);
    assert.deepEqual(pair.after, snapshot);
    assert.deepEqual(pair.verdict, pair.reported);
    assert.deepEqual(entry.verdict, pair.verdict);
    for (const [role, item] of [['before', before], ['after', after], ['verdict', verdict], ['localization', report]]) {
      assert.equal(entry.provenance.find(p => p.role === role).sha256, digest(item.text));
    }
    assert.deepEqual(pair.issues, []);
  }
});
test('a changed snapshot byte is detached despite unchanged decoded JSON', async () => {
  const {report, before, after, verdict} = fixture();
  before.text += '\n';
  const loaded = await loadArtifacts([report, before, after, verdict]);
  assert.equal(loaded.cases.length, 1);
  const entry = loaded.cases[0], pair = entry.comparisons[0];
  assert.equal(pair.before, null);
  assert.equal(entry.before, null);
  assert.ok(pair.issues.some(issue => issue.includes('SHA256 mismatch')));
  assert.equal(pair.provenance.find(p => p.role === 'before').sha256, digest(before.text));
  assert.deepEqual(pair.after, snapshot);
});

test('hash-valid raw verdict disagreement is not replaced by the report summary', async () => {
  const {report, before, after, verdict} = fixture('VERIFIED_BOUNDED');
  const entry = (await loadArtifacts([report, before, after, verdict])).cases[0];
  const pair = entry.comparisons[0];
  assert.equal(pair.verdict, null);
  assert.equal(entry.verdict, null);
  assert.equal(pair.reported.status, 'COUNTEREXAMPLE');
  assert.ok(pair.issues.some(issue => issue.includes('raw verdict differs from the report')));
  assert.equal(pair.provenance.find(p => p.role === 'verdict').sha256, digest(verdict.text));
});

test('report-only import exposes missing snapshots and raw verdict evidence', async () => {
  const {report} = fixture();
  const loaded = await loadArtifacts([report]);
  assert.equal(loaded.cases.length, 1);
  const entry = loaded.cases[0], pair = entry.comparisons[0];
  assert.equal(pair.before, null);
  assert.equal(pair.after, null);
  assert.equal(pair.verdict, null);
  assert.equal(pair.reported.status, 'COUNTEREXAMPLE');
  assert.equal(pair.issues.filter(issue => issue.includes('Missing imported file')).length, 3);
  assert.ok(entry.issues.includes('No raw verifier verdict imported'));
});
test('malformed event and finding labels fail visibly instead of inventing a failing step', async () => {
  for (const mutate of [value => {value.events = [null];},
    value => {value.observation_snapshot_sha256 = '0'.repeat(64);},
    value => {value.comparisons[0].verifier.observation_snapshot_sha256 = '0'.repeat(64);},
    value => {value.comparisons[0].verifier.observation_kind = 'sequence';},
    value => {value.findings[0].status = 'VERIFIED_BOUNDED';},
    value => {value.findings[0].comparison = 'missing';}]) {
    const {report} = fixture('COUNTEREXAMPLE', true), value = JSON.parse(report.text);
    mutate(value);
    const loaded = await loadArtifacts([file(report.name, value)]);
    assert.equal(loaded.cases.length, 0);
    assert.ok(loaded.issues.some(issue => issue.includes('Invalid localization')));
  }
});
