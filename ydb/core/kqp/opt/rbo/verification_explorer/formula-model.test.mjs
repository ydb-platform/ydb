import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash, webcrypto} from 'node:crypto';
import {formulaFields, formulaIndex, readableTerm} from './formula-model.mjs';
import {loadArtifacts} from './artifacts.mjs';

function fixture() {
  const family = {columns: [{name: 'x', type: 'Int64', nullable: true}], outcomes: [{
    index: 0, enabled: 't4', error: 't6', decisions: [], choices: [{term: 't0', bound: 2}],
    sequence: false, order: null, rows: [{slot: 0, present: 't4', ordinal: 't0', values: [{
      column: 'x', type: 'Int64', is_null: 't6', value: 't0', decimal_finite_abs_bound: null,
      metadata: [{kind: 'summary', role: 'proof_metadata', terms: {count: 't1'}}],
    }]}],
  }]};
  return {
    format: 'ydb-rbo-operator-formulas', version: 1, status: 'FORMULAS_GENERATED',
    row_bound: 2, task_bound: 2, semantic_mode: null, semantic_modes: {before: null, after: null},
    inputs: {before_sha256: 'a'.repeat(64), after_sha256: 'b'.repeat(64)},
    abstract_integral_average: false, string_literals: [],
    terms: [
      {id: 't0', sort: 'Int', op: 'symbol', args: [], atom: 'x'},
      {id: 't1', sort: 'Int', op: 'int', args: [], atom: '1208925819614629174706177'},
      {id: 't2', sort: 'Bool', op: '<', args: ['t0', 't1']},
      {id: 't3', sort: 'Bool', op: 'forall', args: ['t0', 't2']},
      {id: 't4', sort: 'Bool', op: 'bool', args: [], atom: true},
      {id: 't5', sort: 'Bool', op: 'and', args: ['t3', 't2']},
      {id: 't6', sort: 'Bool', op: 'bool', args: [], atom: false},
      {id: 't7', sort: 'Int', op: 'f', args: ['t0']},
    ],
    declarations: [
      {kind: 'function', name: 'x', hint: 'witness', arguments: [], result: 'Int'},
      {kind: 'definition', name: 'f', hint: 'exact helper', parameters: ['t0'], result: 'Int', body: 't1'},
    ],
    assertions: ['t5'],
    before: {operators: [{node: 'scan', op: 'scan', scope: {kind: 'logical'}, result: family}],
      connections: [], boundary: family, unobserved_nodes: []},
    after: {operators: [], connections: [], boundary: family, unobserved_nodes: []},
    comparison: {semantics: 'bag', before: family, after: family,
      pair_equal: [['t2']], counterexample: 't5', soundness_exclusion: null},
  };
}

test('readable formulas preserve exact atoms, binders, shared references and unknown function names', () => {
  const document = fixture(), terms = formulaIndex(document);
  assert.deepEqual(terms.get('t3').args, ['t0', 't2']);
  assert.equal(terms.get('t1').atom, '1208925819614629174706177');
  assert.equal(readableTerm(terms, 't3'), '∀ x:Int · (x < 1208925819614629174706177)');
  assert.equal(readableTerm(terms, 't7'), 'f(x)');
  assert.deepEqual(terms.get('t5').args, ['t3', 't2']);
  assert.ok(formulaFields(document.before.boundary).some(field => field.label.includes('summary/count') && field.ref === 't1'));
});

test('malformed navigable references and atoms fail closed', () => {
  const result = document => document.before.operators[0].result.outcomes[0];
  const mutations = [
    document => { document.terms[2].args[0] = 'missing'; },
    document => { document.terms[2].args[0] = 't2'; },
    document => { document.assertions = ['missing']; },
    document => { document.declarations[1].body = 'missing'; },
    document => { document.declarations[1].parameters = ['missing']; },
    document => { document.comparison.counterexample = 'missing'; },
    document => { document.comparison.soundness_exclusion = 'missing'; },
    document => { result(document).choices[0].term = 'missing'; },
    document => { result(document).rows[0].ordinal = 'missing'; },
    document => { result(document).rows[0].values[0].value = 'missing'; },
    document => { result(document).rows[0].values[0].metadata[0].terms.count = 'missing'; },
    document => { document.terms[1].atom = 9007199254740992; },
    document => { document.terms[1].atom = '3.14'; },
    document => { document.terms[5].op = 'not'; },
  ];
  for (const [index, mutate] of mutations.entries()) {
    const document = fixture(); mutate(document);
    assert.throws(() => formulaIndex(document), `mutation ${index}`);
  }
});

test('term/edge caps reject oversized graphs and readable abbreviations stay visibly bounded', () => {
  const tooMany = fixture(); tooMany.terms = Array(200001);
  assert.throws(() => formulaIndex(tooMany), /limit/i);
  const fanout = fixture();
  fanout.terms.push({id: 'wide', op: 'and', sort: 'Bool', args: Array(1000001).fill('t4')});
  assert.throws(() => formulaIndex(fanout), /limit/i);
  fanout.terms.at(-1).args.length = 100;
  let terms = formulaIndex(fanout), text = readableTerm(terms, 'wide');
  assert.ok(text.length < 1000); assert.match(text, /@wide/);
  fanout.terms[1].atom = '1'.repeat(10000);
  terms = formulaIndex(fanout); text = readableTerm(terms, 't1');
  assert.ok(text.length < 1000); assert.match(text, /@t1/);
});

test('formula bindings reject changed hashes, bounds, modes and status without changing proof verdicts', async () => {
  globalThis.crypto ??= webcrypto;
  const snapshot = {format: 'ydb-rbo-semantic-snapshot', version: 1, schema: {tables: []},
    plan: {nodes: [{id: 'scan', op: 'empty_source'}], root: 'scan'}, stage_graph: null};
  const file = (name, value) => ({name, text: JSON.stringify(value)});
  const hash = createHash('sha256').update(JSON.stringify(snapshot)).digest('hex');
  for (const change of [null, 'hash', 'bound', 'input-mode', 'effective-mode', 'status']) {
    const document = fixture(); document.inputs = {before_sha256: hash, after_sha256: hash};
    document.before.operators[0].op = 'empty_source';
    if (change === 'hash') document.inputs.before_sha256 = '0'.repeat(64);
    if (change === 'bound') document.row_bound = 3;
    if (change === 'input-mode') document.semantic_modes.before = 'binary64_uf_universal_v1';
    if (change === 'effective-mode') document.semantic_mode = 'binary64_uf_universal_v1';
    if (change === 'status') document.status = 'VERIFIED_BOUNDED';
    const {cases} = await loadArtifacts([file('q.initial.json', snapshot), file('q.final.json', snapshot),
      file('q.formulas.json', document), file('q.verdict.json', {status: 'VERIFIED_BOUNDED', row_bound: 2, task_bound: 2})]);
    assert.equal(Boolean(cases[0].formulas), change === null, change);
    assert.equal(cases[0].verdict.status, 'VERIFIED_BOUNDED', change);
  }
});
