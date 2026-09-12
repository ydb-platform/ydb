import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {parseLosslessJson} from './artifacts.mjs';
import {explainMismatch, formatCell} from './mismatch.mjs';

const columns = [{name: 'value', type: 'Int64', nullable: true}];
const row = (value, slot, type = 'Int64') => ({present: true, slot, values: [{column: 'value', type, value}]});
const outcome = (index, values, sequence = false, status = 'success') =>
  ({index, status, sequence, rows: values.map((value, slot) => row(value, slot))});
const family = outcomes => ({columns, outcomes, disabled_outcome_count: 0});
function trace(before, after, semantics = 'bag', source = 'before', index = before[0]?.index) {
  return {format: 'ydb-rbo-concrete-trace', version: 1, status: 'COUNTEREXAMPLE',
    mismatches: [{source, outcome: index, matching_outcomes: []}],
    trace: {comparison: {semantics, before: family(before), after: family(after)}}};
}

test('Decimal display decodes exact markers and scale without changing raw evidence', () => {
  for (const [type, value, expected] of [
    ['Decimal(35,2)', '100000000000000000000000000000000001', 'NaN'],
    ['Decimal(7,2)', '100000000000000000000000000000000000', '+∞'],
    ['Decimal(35,2)', '-100000000000000000000000000000000000', '−∞'],
    ['Decimal(7,2)', -12345, '-123.45'], ['Decimal(3,2)', 1, '0.01'],
    ['Decimal(3,2)', 0, '0.00'], ['Decimal(3,0)', 12, '12'],
    ['Decimal(35,2)', '12345678901234567890123456789012345', '123456789012345678901234567890123.45'],
    ['Decimal(3,2)', null, 'NULL'], ['Int64', '9223372036854775807', '9223372036854775807'],
    ['String', '', ''], ['Bool', false, 'false'],
  ]) {
    const cell = Object.freeze({type, value});
    assert.equal(formatCell(cell), expected);
    assert.equal(cell.value, value);
  }
  assert.equal(formatCell({type: 'Decimal(35,2)', value: 1e35}), 'Unreliable Decimal payload (already rounded)');
  for (const [type, value] of [['Decimal(3,2)', 1000], ['Decimal(3,4)', 1], ['Decimal(36,0)', '1'],
    ['Decimal(3,2)', '01'], ['Decimal(3,2)', '1.2'], ['Decimal(3,2)', '-0']]) {
    assert.equal(formatCell({type, value}), `Raw Decimal payload: ${value}`);
  }
  assert.equal(formatCell({type: 'Decimal(3,2)'}), 'Not recorded');
});

test('only recorded descriptors and enabled recorded indices can select a comparison', () => {
  const data = trace([outcome(3, [1])], [outcome(7, [2]), outcome(9, [3])]);
  assert.equal(explainMismatch(data, 'before', 0).status, 'unavailable'); // Not array index zero.
  const unselected = explainMismatch(data, 'before', 3);
  assert.equal(unselected.status, 'unavailable');
  assert.equal(unselected.target, null);
  assert.deepEqual(unselected.targets, [{index: 7, status: 'success'}, {index: 9, status: 'success'}]);
  assert.equal(explainMismatch(data, 'before', 3, 8).status, 'unavailable');
  const selected = explainMismatch(data, 'before', 3, 9);
  assert.equal(selected.status, 'explained');
  assert.equal(selected.source.outcome, data.trace.comparison.before.outcomes[0]);
  assert.equal(selected.target.outcome, data.trace.comparison.after.outcomes[1]);
  assert.equal(selected.target.family, data.trace.comparison.after);
  data.mismatches = [];
  assert.equal(explainMismatch(data, 'before', 3, 9).status, 'unavailable');
});

test('bag explanation counts duplicate whole rows without inventing cell alignment', () => {
  const data = trace([outcome(0, [null, 5, 5])], [outcome(4, [5, null])]);
  data.trace.comparison.before.outcomes[0].rows.push({slot: 3, present: false});
  const result = explainMismatch(data, 'before', 0);
  assert.equal(result.status, 'explained');
  assert.deepEqual(result.differences, [{kind: 'multiplicity', values: [{column: 'value', type: 'Int64', value: 5}],
    sourceCount: 2, targetCount: 1, sourceSlots: [1, 2], targetSlots: [0]}]);
  const reverse = trace([outcome(0, [1])], [outcome(8, [2])], 'bag', 'after', 8);
  assert.equal(explainMismatch(reverse, 'after', 8).source.side, 'after');
  assert.deepEqual(explainMismatch(reverse, 'after', 8).differences.map(d => d.kind), ['multiplicity', 'multiplicity']);
});

test('sequence differences use recorded order and exact NULL/integer cells, not sorted slots', () => {
  const large = '18446744073709551615';
  const data = trace([outcome(2, [null, large], true)], [outcome(5, [large, null], true)], 'sequence');
  data.trace.comparison.before.outcomes[0].rows[0].slot = 20;
  data.trace.comparison.before.outcomes[0].rows[1].slot = 10;
  const result = explainMismatch(data, 'before', 2);
  assert.equal(result.status, 'explained');
  assert.equal(result.differences[0].kind, 'order');
  assert.deepEqual(result.differences[1], {kind: 'cell', position: 0, sourceSlot: 20, targetSlot: 0,
    columnIndex: 0, sourceColumn: 'value', targetColumn: 'value',
    source: {type: 'Int64', value: null}, target: {type: 'Int64', value: large}});
  const extra = explainMismatch(trace([outcome(0, [1, 2], true)], [outcome(0, [1], true)], 'sequence'), 'before', 0);
  assert.deepEqual(extra.differences, [{kind: 'row', position: 1,
    source: {slot: 1, values: [{column: 'value', type: 'Int64', value: 2}]}, target: null}]);
});

test('query errors ignore candidate payloads and empty languages require their descriptor', () => {
  const data = trace([outcome(0, [1], false, 'error')], [outcome(6, [2])]);
  assert.deepEqual(explainMismatch(data, 'before', 0).differences,
    [{kind: 'status', sourceStatus: 'error', targetStatus: 'success'}]);
  data.trace.comparison.after.outcomes[0].status = 'error';
  assert.equal(explainMismatch(data, 'before', 0).status, 'inconsistent');
  const noTargets = trace([outcome(0, [1])], []);
  assert.deepEqual(explainMismatch(noTargets, 'before', 0).differences,
    [{kind: 'language', sourceCount: 1, targetCount: 0}]);
  const noSource = trace([], [outcome(6, [1])]);
  noSource.mismatches = [{source: 'before', reason: 'no_enabled_outcomes'}];
  assert.equal(explainMismatch(noSource, 'before').status, 'explained');
  noSource.trace.comparison.before.outcomes.push(outcome(1, [1]));
  assert.equal(explainMismatch(noSource, 'before').status, 'inconsistent');
});

test('missing cells, rounded numbers, abstract state and non-normalized boundaries are unavailable', () => {
  const mutations = [
    data => {delete data.trace.comparison.before.outcomes[0].rows[0].values[0].value;},
    data => {data.trace.comparison.before.outcomes[0].rows[0].values[0].value = 9007199254740992;},
    data => {data.trace.comparison.before.outcomes[0].rows[0].values[0].average_state = {sum: 1, count: 2};},
    data => {data.trace.comparison.before.outcomes[0].rows[0].present = 'false';},
    data => {data.trace.before = {boundary: data.trace.comparison.before}; delete data.trace.comparison;},
  ];
  for (const mutate of mutations) {
    const data = trace([outcome(0, [1])], [outcome(1, [2])]);
    mutate(data);
    const result = explainMismatch(data, 'before', 0);
    assert.equal(result.status, 'unavailable');
    assert.deepEqual(result.differences, []);
  }
  const equal = trace([outcome(0, [1, null])], [outcome(1, [null, '1'])]);
  assert.equal(explainMismatch(equal, 'before', 0).status, 'inconsistent');
});

test('same spelling in different scalar types differs; unknown Double semantics are not invented', () => {
  const data = trace([outcome(0, [1], true)], [outcome(1, [1], true)], 'sequence');
  data.trace.comparison.after.columns = [{name: 'renamed', type: 'String', nullable: true}];
  data.trace.comparison.after.outcomes[0].rows = [{slot: 0, present: true, values: [{column: 'renamed', type: 'String', value: '1'}]}];
  const result = explainMismatch(data, 'before', 0);
  assert.equal(result.differences[0].targetColumn, 'renamed');
  assert.deepEqual(result.differences[0].target, {type: 'String', value: '1'});
  data.trace.comparison.after.columns[0].type = 'Double';
  data.trace.comparison.after.outcomes[0].rows[0].values[0].type = 'Double';
  assert.equal(explainMismatch(data, 'before', 0).status, 'unavailable');
});

test('authentic q40 normalized trace explains two exact Decimal differences without mutation', async () => {
  const data = parseLosslessJson(await readFile(new URL('./demo/q40.trace.json', import.meta.url), 'utf8'));
  const original = JSON.stringify(data);
  const result = explainMismatch(data, 'before', 0);
  assert.equal(result.status, 'explained');
  assert.deepEqual(result.differences.map(d => [d.kind, d.position, d.columnIndex]), [['cell', 0, 2], ['cell', 0, 3]]);
  assert.equal(result.differences[0].source.value, '100000000000000000000000000000000001');
  assert.equal(result.differences[0].target.value, '-100000000000000000000000000000000000');
  assert.equal(result.differences[1].source.value, null);
  assert.equal(result.differences[1].target.value, 0);
  assert.deepEqual(result.differences.map(d => [formatCell(d.source), formatCell(d.target)]),
    [['NaN', '−∞'], ['NULL', '0.00']]);
  assert.equal(JSON.stringify(data), original);
});
