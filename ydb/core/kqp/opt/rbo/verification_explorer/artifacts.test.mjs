import assert from "node:assert/strict";
import {createHash, webcrypto} from "node:crypto";
import test from "node:test";
import {loadArtifacts, parseLosslessJson} from "./artifacts.mjs";

globalThis.crypto ??= webcrypto;
const snapshot = {format: "ydb-rbo-semantic-snapshot", version: 1,
    plan: {nodes: [{id: "n", op: "empty_source"}], root: "n"}, schema: {tables: []}};
const traceFamily = {columns: [{name: "x", type: "Uint64", nullable: false}], outcomes: [{status: "success", sequence: false,
    rows: [{present: true, values: [{column: "x", value: "18446744073709551615"}]}]}]};
const traceBody = {before: {operators: [], boundary: traceFamily}, after: {operators: [], boundary: traceFamily}};
const file = (name, value) => ({name, text: JSON.stringify(value)});
const digest = text => createHash("sha256").update(text).digest("hex");
const manifest = cases => file("manifest.json", {format: "rbo-explorer", version: 1, cases});
const pair = [file("q.initial.json", snapshot), file("q.final.json", snapshot)];

test("JSON numeric tokens are exact, including Decimal carriers and exponent notation", () => {
    const parsed = parseLosslessJson('{"v":[9007199254740991,9007199254740992,-9007199254740993,'
        + '18446744073709551615,100000000000000000000000000000000000,1.2300,1e400,-0],"s":"9007199254740993"}');
    assert.deepEqual(parsed.v, [9007199254740991, "9007199254740992", "-9007199254740993",
        "18446744073709551615", "100000000000000000000000000000000000", "1.2300", "1e400", "-0"]);
    assert.equal(parsed.s, "9007199254740993");
    assert.equal(parseLosslessJson(JSON.stringify('escaped "')), 'escaped "');
    for (const invalid of ['{"a":1,"a":2}', '{"a":1,"\\u0061":2}', "01", "[1,]", "true false", "1.", '"\n"']) {
        assert.throws(() => parseLosslessJson(invalid), SyntaxError, invalid);
    }
    const proto = parseLosslessJson('{"__proto__":{"polluted":true}}');
    assert.equal(Object.getPrototypeOf(proto), Object.prototype);
    assert.equal(Object.hasOwn(proto, "__proto__"), true);
    assert.equal({}.polluted, undefined);
    assert.throws(() => parseLosslessJson("[".repeat(130) + "0" + "]".repeat(130)), /nesting/);
});

test("explicit manifest binds exact files, hashes, raw trace and query without interpreting status", async () => {
    const trace = {format: "ydb-rbo-concrete-trace", version: 1, status: "COUNTEREXAMPLE",
        inputs: {before_semantic_sha256: "not-a-raw-file-digest"}, trace: traceBody};
    const query = {name: "query.yql", text: "SELECT '<script>alert(1)</script>';"};
    const loaded = await loadArtifacts([...pair, query, file("trace.json", trace),
        {name: "verdict.json", text: '{"status":"COUNTEREXAMPLE","witness":{"T":[{"x":18446744073709551615}]}}'},
        manifest([{id: "case", title: "Kept title", kind: "mutation", revision: "abc",
            trace_producer: {revision: "trace-revision"}, provenance_notes: "Explicit mixed-revision evidence", before: pair[0].name,
            after: pair[1].name, query: query.name, trace: "trace.json", verdict: "verdict.json",
            hashes: {[pair[0].name]: digest(pair[0].text)}}])]);
    assert.equal(loaded.cases.length, 1);
    const entry = loaded.cases[0];
    assert.equal(entry.binding, "manifest");
    assert.deepEqual(entry.before, snapshot);
    assert.deepEqual(entry.trace, trace);
    assert.equal(entry.verdict.witness.T[0].x, "18446744073709551615");
    assert.equal(entry.query, query.text);
    assert.equal(entry.kind, "mutation");
    assert.equal(entry.revision, "abc");
    assert.deepEqual(entry.trace_producer, {revision: "trace-revision"});
    assert.equal(entry.provenance_notes, "Explicit mixed-revision evidence");
    assert.equal(entry.provenance.find(p => p.role === "before").sha256, digest(pair[0].text));
    assert.deepEqual(entry.issues, []);
    assert.deepEqual(loaded.issues, []);
});

test("hashes use original bytes, reject mismatches and never substitute bad evidence", async () => {
    const bytes = new Uint8Array([0xef, 0xbb, 0xbf, ...new TextEncoder().encode(pair[0].text)]);
    const loaded = await loadArtifacts([{...pair[0], bytes}, pair[1], manifest([{id: "bad", before: pair[0].name,
        after: pair[1].name, hashes: {[pair[0].name]: "0".repeat(64)}}])]);
    const entry = loaded.cases[0];
    assert.equal(entry.before, null);
    assert.ok(entry.issues.some(issue => issue.includes("SHA256 mismatch")));
    assert.equal(entry.provenance.find(p => p.role === "before").hashBasis, "raw-bytes");
    assert.equal(entry.provenance.find(p => p.role === "before").sha256, createHash("sha256").update(bytes).digest("hex"));
    assert.notEqual(entry.provenance.find(p => p.role === "before").sha256, digest(pair[0].text));
    const inconsistent = await loadArtifacts([{...pair[0], bytes: new TextEncoder().encode("different")}]);
    assert.ok(inconsistent.issues.some(issue => issue.includes("do not decode")));
    const invalidUtf8 = await loadArtifacts([{...pair[0], bytes: new Uint8Array([0xff])}]);
    assert.equal(invalidUtf8.cases.length, 0);
    assert.equal(invalidUtf8.issues.length, 1);
});

test("filename groups retain missing evidence; duplicates, remote references and invalid JSON fail visibly", async () => {
    const loaded = await loadArtifacts([pair[0], file("q.trace.json", {format: "ydb-rbo-concrete-trace", version: 1}),
        pair[1], pair[1], {name: "other.verdict.json", text: '{"status":"SAT","status":"UNSAT"}'},
        manifest([{id: "remote", before: "https://example.invalid/private.json", after: "missing.json"}])]);
    const raw = loaded.cases.find(entry => entry.id === "q");
    assert.deepEqual(raw.before, snapshot);
    assert.equal(raw.after, null);
    assert.equal(raw.verdict, null);
    assert.ok(raw.issues.some(issue => issue.includes("Filename grouping")));
    assert.ok(loaded.issues.some(issue => issue.includes("Ambiguous imported filename")));
    assert.ok(loaded.issues.some(issue => issue.includes("Duplicate object key")));
    const remote = loaded.cases.find(entry => entry.id === "remote");
    assert.ok(remote.issues.some(issue => issue.includes("not a URL")));
    assert.ok(remote.issues.some(issue => issue.includes("Missing imported file")));
});

test("relative manifest references are exact and do not guess a matching basename", async () => {
    const loaded = await loadArtifacts([{...manifest([{id: "relative", before: "a.json", after: "b.json"}]), name: "dir/manifest.json"},
        file("dir/a.json", snapshot), file("elsewhere/b.json", snapshot)]);
    assert.deepEqual(loaded.cases[0].before, snapshot);
    assert.equal(loaded.cases[0].after, null);
});

test("coverage v5 resolves selected report artifacts but never invents raw verdict evidence", async () => {
    const report = {format: "ydb-rbo-benchmark-coverage", version: 5, suite: "TPCDS_YQL", row_bound: 2, task_bound: 2,
        queries: [{query_id: 50, status: "VERIFIED_BOUNDED", verdict: {status: "VERIFIED_BOUNDED"},
            artifacts: {initial_snapshot: "/old/run/q.initial.json", initial_snapshot_sha256: digest(pair[0].text),
                final_snapshot: "/old/run/q.final.json"}}]};
    const loaded = await loadArtifacts([file("coverage.json", report), ...pair]);
    assert.equal(loaded.cases.length, 1);
    const entry = loaded.cases[0];
    assert.deepEqual(entry.before, snapshot);
    assert.equal(entry.verdict, null);
    assert.equal(entry.coverage.status, "VERIFIED_BOUNDED");
    assert.ok(entry.issues.some(issue => issue.includes("summary, not raw witness")));
    const ambiguous = await loadArtifacts([file("coverage.json", report), pair[1],
        {...pair[0], name: "a/q.initial.json"}, {...pair[0], name: "b/q.initial.json"}]);
    assert.equal(ambiguous.cases[0].before, null);
    assert.ok(ambiguous.cases[0].issues.some(issue => issue.includes("Ambiguous imported basename")));
});

test("native bundles preserve one joint verdict and explicit ordered result slots", async () => {
    const bundle = {format: "ydb-rbo-result-bundle", version: 1, observation: "buffered_tuple_or_error",
        results: [{before: "q.initial.json", after: "q.final.json"}, {before: "r.initial.json", after: "r.final.json"}]};
    const loaded = await loadArtifacts([...pair, file("r.initial.json", snapshot), file("r.final.json", snapshot),
        file("batch.bundle.json", bundle), file("batch.verdict.json", {status: "VERIFIED_BOUNDED", comparison_scope: "BUFFERED_RESULT_BUNDLE"})]);
    assert.equal(loaded.cases.length, 1);
    const entry = loaded.cases[0];
    assert.equal(entry.before, null);
    assert.equal(entry.results.length, 2);
    assert.deepEqual(entry.results[1].after, snapshot);
    assert.equal(entry.results[0].verdict, null);
    assert.equal(entry.verdict.comparison_scope, "BUFFERED_RESULT_BUNDLE");
    assert.ok(entry.issues.some(issue => issue.includes("joint result tuple")));
});

test("unsupported versions and repeated manifest ids are not silently accepted", async () => {
    const loaded = await loadArtifacts([file("future.json", {format: "rbo-explorer", version: 2, cases: []}),
        manifest([{id: "same"}, {id: "same"}])]);
    assert.equal(loaded.cases.length, 1);
    assert.ok(loaded.issues.some(issue => issue.includes("unsupported explorer")));
    assert.ok(loaded.issues.some(issue => issue.includes("Duplicate case id")));
});

test("unpaired snapshots retain their raw plan without inventing a boundary role", async () => {
    const loaded = await loadArtifacts([file("snapshot.json", snapshot)]);
    assert.deepEqual(loaded.cases[0].snapshot, snapshot);
    assert.equal(loaded.cases[0].before, null);
    assert.equal(loaded.cases[0].after, null);
    assert.equal(loaded.cases[0].binding, "unpaired");
});

test("coverage owns its bundle independent of selection order", async () => {
    const bundle = file("batch.bundle.json", {format: "ydb-rbo-result-bundle", version: 1,
        observation: "buffered_tuple_or_error", results: [{before: "q.initial.json", after: "q.final.json"}]});
    const report = file("coverage.json", {format: "ydb-rbo-benchmark-coverage", version: 5,
        queries: [{query_id: 14, artifacts: {bundle: bundle.name, bundle_sha256: digest(bundle.text)}}]});
    for (const selected of [[bundle, report, ...pair], [report, ...pair, bundle]]) {
        const loaded = await loadArtifacts(selected);
        assert.equal(loaded.cases.length, 1);
        assert.equal(loaded.cases[0].binding, "coverage-report");
        assert.deepEqual(loaded.cases[0].results[0].before, snapshot);
    }
});

test("malformed display shapes detach snapshots; unknown operators remain inspectable", async () => {
    const withNode = node => ({...snapshot, plan: {...snapshot.plan, nodes: [{id: "n", ...node}]}});
    const mutants = [
        {...snapshot, plan: {}}, {...snapshot, plan: {...snapshot.plan, nodes: [{id: 1, op: "scan"}]}},
        {...snapshot, plan: {...snapshot.plan, nodes: [{id: "n", op: "join"}]}},
        {...snapshot, plan: {...snapshot.plan, nodes: [...snapshot.plan.nodes, ...snapshot.plan.nodes]}},
        {...snapshot, schema: {tables: [{name: "T", columns: {}}]}}, {...snapshot, stage_graph: {}},
        {...snapshot, stage_graph: {root_stage: "s", stages: [{id: "s", nodes: ["missing"], inputs: [], outputs: []}], edges: []}},
        {...snapshot, plan: {...snapshot.plan, nodes: [{id: "n", op: "filter", predicate: {kind: "and", args: 1}}]}},
        withNode({op: "join", kind: "inner", keys: {}}), withNode({op: "join", kind: "inner", keys: [null]}),
        withNode({op: "sort", order: [{column: "x", ascending: "false", nulls_first: true}]}),
        withNode({op: "sort", order: [{column: "x", ascending: true, nulls_first: 0}]}),
        withNode({op: "project", columns: [], ordered: "false"}),
        withNode({op: "aggregate", aggregates: [], keys: {}}), withNode({op: "aggregate", aggregates: [], keys: [1]}),
        ...[{type: 1, nullable: false}, {type: "Int64", nullable: "false"}].map(column =>
            ({...snapshot, schema: {tables: [{name: "T", columns: [{name: "x", ...column}]}]}})),
    ];
    for (const mutant of mutants) {
        const loaded = await loadArtifacts([file("bad.initial.json", mutant)]);
        assert.equal(loaded.cases[0].before, null);
        assert.ok(loaded.cases[0].issues.some(issue => issue.includes("unsupported display shape")));
    }
    const unknown = {...snapshot, plan: {...snapshot.plan, nodes: [{id: "n", op: "future_operator", secret: "raw"}]}};
    assert.deepEqual((await loadArtifacts([file("unknown.initial.json", unknown)])).cases[0].before, unknown);
    const union = {...snapshot, plan: {root: "union", nodes: [...snapshot.plan.nodes,
        {id: "union", op: "union_all", ordered: false, inputs: [{node: "n", columns: []}, {node: "n", columns: []}]}]}};
    assert.deepEqual((await loadArtifacts([file("union.initial.json", union)])).cases[0].before, union);
    const large = {...snapshot, plan: {root: "n0", nodes: Array.from({length: 1001}, (_, i) => ({id: `n${i}`, op: "empty_source"}))}};
    const rejected = (await loadArtifacts([file("large.initial.json", large)])).cases[0];
    assert.equal(rejected.before, null);
    assert.ok(rejected.issues.some(issue => issue.includes("1000-node UI display limit")));
    large.plan.nodes.pop();
    assert.ok((await loadArtifacts([file("large.initial.json", large)])).cases[0].before);
    for (const column of [{name: "x", type: {}, nullable: false}, {name: "x", type: "Int64", nullable: "false"}]) {
        const malformed = structuredClone(traceBody);
        malformed.before.boundary.columns = [column];
        const loaded = await loadArtifacts([file("bad.trace.json", {format: "ydb-rbo-concrete-trace", version: 1, trace: malformed})]);
        assert.equal(loaded.cases[0].trace, null);
        assert.ok(loaded.cases[0].issues.some(issue => issue.includes("trace columns require")));
    }
});

test("trace/verdict metadata or query disagreement detaches the trace without changing verdict", async () => {
    const verdict = {status: "COUNTEREXAMPLE", row_bound: 2, task_bound: 2, witness: {T: [{x: "18446744073709551615", y: null}]}};
    const query = {name: "q.query.yql", text: "SELECT 1;\n"};
    const trace = {format: "ydb-rbo-concrete-trace", version: 1, ...verdict, trace: traceBody,
        inputs: {query_sha256: digest(query.text)}};
    for (const different of [{...trace, row_bound: 3}, {...trace, task_bound: 1}, {...trace, status: "VERIFIED_BOUNDED"},
        {...trace, inputs: {query_sha256: digest("SELECT 2;\n")}},
        {...trace, witness: {T: [{x: "18446744073709551614", y: null}]}}]) {
        const loaded = await loadArtifacts([file("q.verdict.json", verdict), file("q.trace.json", different), query]);
        assert.deepEqual(loaded.cases[0].verdict, verdict);
        assert.equal(loaded.cases[0].trace, null);
        assert.ok(loaded.cases[0].issues.some(issue => issue.includes("Trace detached")));
        assert.ok(loaded.cases[0].provenance.some(p => p.role === "trace"));
    }
    const reordered = {...trace, witness: {T: [{y: null, x: "18446744073709551615"}]}};
    assert.ok((await loadArtifacts([file("q.verdict.json", verdict), file("q.trace.json", reordered), query])).cases[0].trace);
    const malformed = structuredClone(trace);
    malformed.trace.before.boundary.outcomes[0].rows[0].present = "false";
    const loaded = await loadArtifacts([file("q.trace.json", malformed)]);
    assert.equal(loaded.cases[0].trace, null);
    assert.ok(loaded.cases[0].issues.some(issue => issue.includes("presence must be a Boolean")));
});
