// Local evidence adapter, not a verifier. No URLs are fetched or commands run.
// Unsafe integers and decimal/exponent tokens remain exact strings. Supplied
// bytes are authoritative for SHA256; text-only imports hash UTF-8 text bytes.

export function parseLosslessJson(text) {
    let pos = 0;
    const fail = message => { throw new SyntaxError(`${message} at offset ${pos}`); };
    const space = () => { while (/[\t\n\r ]/.test(text[pos] ?? "!")) pos++; };
    function string() {
        const start = pos++;
        while (pos < text.length) {
            if (text[pos] === "\\") pos += 2;
            else if (text[pos++] === '"') return JSON.parse(text.slice(start, pos));
        }
        return fail("Unterminated string");
    }
    function value(depth) {
        if (depth > 128) fail("JSON nesting exceeds 128");
        space();
        if (text[pos] === '"') return string();
        if (text[pos] === "{" || text[pos] === "[") {
            const object = text[pos++] === "{";
            const result = object ? {} : [];
            const end = object ? "}" : "]";
            space();
            if (text[pos] === end) { pos++; return result; }
            for (;;) {
                space();
                let key;
                if (object) {
                    if (text[pos] !== '"') fail("Expected object key");
                    key = string();
                    if (Object.hasOwn(result, key)) fail(`Duplicate object key ${JSON.stringify(key)}`);
                    space();
                    if (text[pos++] !== ":") fail("Expected colon");
                }
                const item = value(depth + 1);
                // Define a data property: imported __proto__ is never a setter.
                if (object) Object.defineProperty(result, key, {value: item, enumerable: true, writable: true});
                else result.push(item);
                space();
                if (text[pos] === end) { pos++; return result; }
                if (text[pos++] !== ",") fail("Expected comma or closing delimiter");
            }
        }
        for (const [token, result] of [["true", true], ["false", false], ["null", null]]) {
            if (text.startsWith(token, pos)) { pos += token.length; return result; }
        }
        const token = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(text.slice(pos))?.[0];
        if (!token) fail("Expected JSON value");
        pos += token.length;
        if (/^-?\d+$/.test(token) && token !== "-0") {
            const integer = BigInt(token);
            if (integer >= -9007199254740991n && integer <= 9007199254740991n) return Number(integer);
        }
        return token;
    }
    const result = value(0);
    space();
    if (pos !== text.length) fail("Unexpected trailing input");
    return result;
}

const object = value => value !== null && typeof value === "object" && !Array.isArray(value);
const formats = {
    snapshot: "ydb-rbo-semantic-snapshot", trace: "ydb-rbo-concrete-trace",
    bundle: "ydb-rbo-result-bundle", coverage: "ydb-rbo-benchmark-coverage",
};
const roles = ["before", "after", "verdict", "trace", "query"];
const rawSuffix = /^(.*)\.(initial\.json|final\.json|verdict\.json|trace\.json|query\.(?:yql|sql))$/;
const rawRole = suffix => suffix.startsWith("initial") ? "before" : suffix.startsWith("final") ? "after"
    : suffix.split(".")[0];

// Validate only the shapes dereferenced by the display. This neither admits
// an operator to the verifier nor verifies its semantics; unknown ops stay raw.
function displayShape(value, role) {
    const check = (condition, message) => { if (!condition) throw new Error(message); };
    const name = value => typeof value === "string" && value.length > 0;
    const named = (items, key, context) => {
        check(Array.isArray(items), `${context} must be an array`);
        const seen = new Set();
        for (const item of items) {
            check(object(item) && name(item[key]) && !seen.has(item[key]), `${context} needs unique nonempty ${key} fields`);
            seen.add(item[key]);
        }
        return seen;
    };
    const references = (items, ids, context) => check(Array.isArray(items)
        && items.every(id => name(id) && ids.has(id)), `${context} contains invalid node references`);
    const strings = items => Array.isArray(items) && items.every(name);
    const columns = (items, context) => {
        named(items, "name", context);
        check(items.every(column => name(column.type) && typeof column.nullable === "boolean"),
            `${context} require string types and Boolean nullable flags`);
    };
    const order = items => check(Array.isArray(items) && items.every(item => object(item) && name(item.column)
        && typeof item.ascending === "boolean" && typeof item.nulls_first === "boolean"),
    "sort order requires named columns and Boolean ascending/nulls_first flags");
    const witness = data => {
        check(object(data), "witness must be a table-to-row-array object");
        for (const rows of Object.values(data)) check(Array.isArray(rows) && rows.every(object), "witness tables require row objects");
    };
    if (role === "verdict" || role === "trace") {
        if (value.witness !== undefined) witness(value.witness);
        if (role === "verdict" || value.trace === undefined) return;
        const family = data => {
            check(object(data), "trace family must be an object");
            columns(data.columns, "trace columns");
            check(Array.isArray(data.outcomes), "trace outcomes must be an array");
            for (const outcome of data.outcomes) {
                check(object(outcome) && ["success", "error"].includes(outcome.status)
                    && typeof outcome.sequence === "boolean" && Array.isArray(outcome.rows), "invalid concrete trace outcome");
                if (outcome.order != null) order(outcome.order);
                for (const row of outcome.rows) {
                    check(object(row) && typeof row.present === "boolean", "trace presence must be a Boolean");
                    if (row.present) named(row.values, "column", "present trace row values");
                }
            }
        };
        check(object(value.trace), "trace body must be an object");
        for (const side of ["before", "after"]) {
            const execution = value.trace[side];
            check(object(execution) && Array.isArray(execution.operators), `trace ${side} needs recorded operators`);
            for (const event of execution.operators) {
                check(object(event) && name(event.node) && object(event.scope), "invalid trace operator event");
                family(event.result);
            }
            family(execution.boundary);
        }
        if (value.trace.comparison !== undefined) {
            check(object(value.trace.comparison), "trace comparison must be an object");
            family(value.trace.comparison.before); family(value.trace.comparison.after);
        }
        if (value.mismatches !== undefined) check(Array.isArray(value.mismatches)
            && value.mismatches.every(object), "trace mismatches must be an array of objects");
        return;
    }
    check(object(value.plan), "snapshot plan must be an object");
    const ids = named(value.plan.nodes, "id", "plan nodes");
    check(ids.size <= 1000, "snapshot exceeds the 1000-node UI display limit (not a verifier admission limit)");
    check(name(value.plan.root) && ids.has(value.plan.root), "plan root must reference a recorded node");
    for (const node of value.plan.nodes) {
        check(name(node.op), "node op must be a nonempty string");
        for (const key of ["input", "left", "right"]) if (node[key] !== undefined) references([node[key]], ids, key);
        if (node.inputs !== undefined) {
            if (node.op === "union_all") {
                check(Array.isArray(node.inputs) && node.inputs.every(input => object(input) && strings(input.columns)),
                    "union inputs require node/column bindings");
                references(node.inputs.map(input => input.node), ids, "union inputs");
            } else references(node.inputs, ids, "node inputs");
        }
        if (node.phase != null) check(typeof node.phase === "string", "node phase must be a string");
        if (node.ordered !== undefined) check(typeof node.ordered === "boolean", "node ordered must be a Boolean");
        if (node.columns !== undefined) named(node.columns, "output", "node columns");
        if (node.op === "scan") check(name(node.table), "scan table must be a nonempty name");
        if (node.op === "join") {
            check(name(node.kind), "join kind must be a nonempty string");
            check(Array.isArray(node.keys) && node.keys.every(key => object(key) && name(key.left) && name(key.right)),
                "join keys must be an array of named left/right pairs");
        }
        if (node.op === "sort") order(node.order);
        if (node.op === "aggregate") {
            check(strings(node.keys), "aggregate keys must be an array of strings");
            check(Array.isArray(node.aggregates) && node.aggregates.every(a => object(a) && name(a.function)), "invalid aggregate display fields");
        }
        const pending = [node.predicate, ...(node.columns || []).map(column => column.expression)];
        while (pending.length) {
            const expr = pending.pop();
            if (!object(expr)) continue;
            if (["and", "or"].includes(expr.kind)) check(Array.isArray(expr.args), "Boolean expression args must be an array");
            for (const child of Object.values(expr)) if (object(child)) pending.push(child);
                else if (Array.isArray(child)) pending.push(...child);
        }
    }
    check(object(value.schema), "snapshot schema must be an object");
    named(value.schema.tables, "name", "schema tables");
    for (const table of value.schema.tables) columns(table.columns, "schema columns");
    if (value.plan.subplans !== undefined) {
        check(Array.isArray(value.plan.subplans), "subplans must be an array");
        for (const subplan of value.plan.subplans) {
            check(object(subplan) && name(subplan.kind), "invalid subplan display fields");
            references([subplan.root], ids, "subplan root");
            references(subplan.consumers || [], ids, "subplan consumers");
        }
    }
    if (value.stage_graph == null) return;
    check(object(value.stage_graph), "stage_graph must be an object or null");
    const graph = value.stage_graph, stages = named(graph.stages, "id", "stages");
    check(stages.size <= 1000, "stage graph exceeds the 1000-node UI display limit (not a verifier admission limit)");
    check(stages.has(graph.root_stage), "root_stage must reference a recorded stage");
    for (const stage of graph.stages) {
        references(stage.nodes, ids, "stage nodes"); references(stage.inputs, ids, "stage inputs");
        check(Array.isArray(stage.outputs), "stage outputs must be an array");
        references(stage.outputs.map(output => output?.node), ids, "stage outputs");
    }
    named(graph.edges, "id", "stage edges");
    for (const edge of graph.edges) {
        check(stages.has(edge.producer) && stages.has(edge.consumer)
            && name(edge.kind), "stage edge must name recorded producer/consumer stages and a kind");
        if (edge.order !== undefined) order(edge.order);
    }
}

function sameJson(left, right) {
    if (left === right) return true;
    if (!left || !right || typeof left !== "object" || typeof right !== "object"
        || Array.isArray(left) !== Array.isArray(right)) return false;
    const keys = Object.keys(left);
    return keys.length === Object.keys(right).length
        && keys.every(key => Object.hasOwn(right, key) && sameJson(left[key], right[key]));
}

function localName(name) {
    if (typeof name !== "string" || !name || /[\0\\]/.test(name) || /^[a-z][\w+.-]*:/i.test(name)
        || name.startsWith("//")) throw new Error("Expected a local imported filename, not a URL");
    const parts = [];
    for (const part of name.split("/")) {
        if (part === "..") {
            if (!parts.length) throw new Error("Filename escapes the imported namespace");
            parts.pop();
        } else if (part && part !== ".") parts.push(part);
    }
    if (!parts.length) throw new Error("Empty imported filename");
    return (name.startsWith("/") ? "/" : "") + parts.join("/");
}

export async function loadArtifacts(files) {
    const issues = [], cases = [], imports = new Map(), used = new Set(), ids = new Set();
    if (!Array.isArray(files)) throw new TypeError("files must be an array");
    for (const file of files) {
        try {
            const name = localName(file.name);
            if (imports.has(name)) {
                imports.set(name, null); // Neither duplicate wins, even when bytes happen to match.
                issues.push(`Ambiguous imported filename: ${name}`);
                continue;
            }
            if (typeof file.text !== "string") throw new Error("text must be a string");
            const bytes = file.bytes === undefined ? new TextEncoder().encode(file.text) : file.bytes;
            if (!(bytes instanceof Uint8Array)) throw new Error("bytes must be a Uint8Array");
            if (file.bytes !== undefined && new TextDecoder("utf-8", {fatal: true}).decode(bytes) !== file.text) {
                throw new Error("Original UTF-8 bytes do not decode to the supplied text");
            }
            const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
            const sha256 = Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
            const item = {name, text: file.text, sha256, hashBasis: file.bytes === undefined ? "utf8-text" : "raw-bytes"};
            try { item.value = name.endsWith(".json") ? parseLosslessJson(file.text.replace(/^\uFEFF/, "")) : null; }
            catch (error) { item.error = `Invalid JSON: ${error.message}`; issues.push(`${name}: ${item.error}`); }
            imports.set(name, item);
        } catch (error) { issues.push(`${String(file?.name)}: ${error.message}`); }
    }
    function evidence(entry, item, role) {
        used.add(item.name);
        if (!entry.provenance.some(p => p.name === item.name && p.role === role)) {
            entry.provenance.push({name: item.name, sha256: item.sha256, hashBasis: item.hashBasis, role});
        }
    }
    function resolve(entry, ref, owner, role, hashes = {}, basename = false) {
        if (ref === undefined || ref === null) return null;
        try {
            const name = localName(ref);
            const relative = ref.startsWith("/") ? name : localName(`${owner.slice(0, owner.lastIndexOf("/") + 1)}${ref}`);
            let key = imports.has(relative) ? relative : name;
            if (!imports.has(key) && basename) {
                const matches = [...imports.keys()].filter(k => k.split("/").at(-1) === name.split("/").at(-1));
                if (matches.length > 1) throw new Error(`Ambiguous imported basename for ${ref}`);
                if (matches.length === 1) {
                    key = matches[0];
                    entry.issues.push(`${role}: resolved report path ${ref} by unique imported basename ${key}`);
                }
            }
            if (!imports.has(key)) throw new Error(`Missing imported file: ${ref}`);
            const item = imports.get(key);
            if (!item) throw new Error(`Ambiguous imported filename: ${key}`);
            evidence(entry, item, role);
            const expected = Object.hasOwn(hashes, ref) ? hashes[ref] : undefined;
            if (expected !== undefined && (typeof expected !== "string" || !/^[\da-f]{64}$/i.test(expected)
                || expected.toLowerCase() !== item.sha256)) throw new Error(`SHA256 mismatch for ${ref}`);
            if (item.error) throw new Error(item.error);
            return item;
        } catch (error) { entry.issues.push(`${role}: ${error.message}`); return null; }
    }
    function attach(entry, role, item) {
        if (!item) return;
        const value = item.value;
        const valid = role === "query" || (object(value) && (role === "verdict" ? typeof value.status === "string"
            : value.version === 1 && value.format === (role === "trace" ? formats.trace : formats.snapshot)));
        if (!valid) entry.issues.push(`${role}: unsupported artifact format in ${item.name}`);
        else {
            try {
                if (role !== "query") displayShape(value, role);
                entry[role] = role === "query" ? item.text : value;
                if (role === "query") entry.querySha256 = item.sha256;
            } catch (error) { entry.issues.push(`${role}: unsupported display shape in ${item.name}: ${error.message}`); }
        }
    }
    function blank(id, title = id, description = "") {
        return {id, title, description, before: null, after: null, verdict: null, trace: null, query: "",
            provenance: [], issues: []};
    }
    function finish(entry) {
        if (ids.has(entry.id)) { issues.push(`Duplicate case id: ${entry.id}`); return; }
        ids.add(entry.id);
        const conflicts = [];
        if (entry.verdict && entry.trace) {
            const mismatchedBounds = ["status", "row_bound", "task_bound"].filter(key => entry.verdict[key] !== undefined
                && entry.trace[key] !== undefined && entry.verdict[key] !== entry.trace[key]);
            if (mismatchedBounds.length) conflicts.push(`different ${mismatchedBounds.join("/")} from the saved verdict`);
            if (entry.verdict.witness !== undefined && entry.trace.witness !== undefined
                && !sameJson(entry.verdict.witness, entry.trace.witness)) conflicts.push("different witness database from the saved verdict");
        }
        const queryHash = entry.trace?.inputs?.query_sha256;
        if (queryHash !== undefined && entry.querySha256 !== undefined && (typeof queryHash !== "string"
            || !/^[\da-f]{64}$/i.test(queryHash) || queryHash.toLowerCase() !== entry.querySha256)) {
            conflicts.push("query SHA256 does not match the attached query bytes");
        }
        if (conflicts.length) {
            entry.issues.push(`Trace detached: ${conflicts.join("; ")}`);
            entry.trace = null;
        }
        if (!entry.results) for (const role of ["before", "after"]) {
            if (!entry[role]) entry.issues.push(`Missing ${role} snapshot evidence`);
        }
        if (!entry.verdict) entry.issues.push("No raw verifier verdict imported");
        cases.push(entry);
    }
    function bind(entry, spec, owner, hashes = {}, basename = false) {
        for (const role of roles) attach(entry, role, resolve(entry, spec[role], owner, role, hashes, basename));
    }
    function bundle(entry, value, owner, hashes = {}) {
        if (value.version !== 1 || value.observation !== "buffered_tuple_or_error"
            || !Array.isArray(value.results) || !value.results.length) {
            entry.issues.push("Unsupported result-bundle manifest; expected nonempty v1 buffered_tuple_or_error");
            return;
        }
        entry.results = value.results.map((slot, index) => {
            const result = blank(`${entry.id}:result:${index}`, `Result ${index}`);
            if (!object(slot) || Object.keys(slot).sort().join(",") !== "after,before") {
                result.issues.push("Unsupported bundle slot; expected before and after filenames");
            } else bind(result, slot, owner, hashes);
            for (const role of ["before", "after"]) if (!result[role]) result.issues.push(`Missing ${role} snapshot evidence`);
            entry.provenance.push(...result.provenance);
            return result;
        });
        entry.observation = "buffered_tuple_or_error";
        entry.issues.push("Bundle verdict applies to the joint result tuple, not to individual result slots");
    }
    // Explicit explorer manifests bind exact filenames; no basename guessing.
    for (const item of imports.values()) {
        if (!item || item.value?.format !== "rbo-explorer") continue;
        used.add(item.name);
        if (item.value.version !== 1 || !Array.isArray(item.value.cases)) {
            issues.push(`${item.name}: unsupported explorer manifest version/shape`); continue;
        }
        for (const spec of item.value.cases) {
            if (!object(spec) || typeof spec.id !== "string" || !spec.id) {
                issues.push(`${item.name}: manifest case needs a nonempty string id`); continue;
            }
            const entry = blank(spec.id, typeof spec.title === "string" ? spec.title : spec.id,
                typeof spec.description === "string" ? spec.description : "");
            entry.binding = "manifest";
            for (const field of ["kind", "revision", "provenance_notes"]) if (typeof spec[field] === "string") entry[field] = spec[field];
            if (object(spec.trace_producer)) entry.trace_producer = spec.trace_producer;
            evidence(entry, item, "manifest");
            const hashes = spec.hashes ?? {};
            if (!object(hashes)) entry.issues.push("Manifest hashes must be a filename-to-SHA256 object");
            else {
                for (const ref of Object.keys(hashes)) resolve(entry, ref, item.name, "hash", hashes);
                bind(entry, spec, item.name, hashes);
                if (spec.results !== undefined) {
                    if (spec.before !== undefined || spec.after !== undefined) entry.issues.push("Bundle results cannot also bind a single pair");
                    else bundle(entry, {version: 1, observation: "buffered_tuple_or_error", results: spec.results}, item.name, hashes);
                }
            }
            finish(entry);
        }
    }
    // Coverage owns its referenced bundle even if the chooser listed that
    // bundle first. Avoid making import ordering create duplicate joint cases.
    const reportsFirst = [...imports.values()].sort((a, b) =>
        Number(b?.value?.format === formats.coverage) - Number(a?.value?.format === formats.coverage));
    for (const item of reportsFirst) {
        if (!item || used.has(item.name) || !object(item.value)) continue;
        const value = item.value;
        if (value.format === formats.bundle) {
            const entry = blank(item.name);
            entry.binding = "bundle-manifest";
            evidence(entry, item, "manifest");
            bundle(entry, value, item.name);
            const stem = item.name.replace(/\.bundle\.json$/, "");
            for (const [role, suffix] of [["verdict", ".verdict.json"], ["trace", ".trace.json"], ["query", ".query.yql"]]) {
                if (imports.has(stem + suffix)) attach(entry, role, resolve(entry, stem + suffix, "", role));
            }
            finish(entry);
        } else if (value.format === formats.coverage) {
            used.add(item.name);
            if (value.version !== 5 || !Array.isArray(value.queries)) {
                issues.push(`${item.name}: unsupported coverage report version/shape`); continue;
            }
            for (const row of value.queries) {
                if (!object(row) || !(typeof row.query_id === "number" || typeof row.query_id === "string")) {
                    issues.push(`${item.name}: coverage row has no query id`); continue;
                }
                const entry = blank(`${item.name}#${row.query_id}`, `${value.suite ?? "Coverage"} q${row.query_id}`);
                entry.binding = "coverage-report";
                entry.coverage = row;
                entry.coverageBounds = {row_bound: value.row_bound, task_bound: value.task_bound};
                evidence(entry, item, "coverage");
                const artifacts = object(row.artifacts) ? row.artifacts : {};
                const mapping = {before: "initial_snapshot", after: "final_snapshot", verdict: "verifier_verdict", query: "query"};
                const spec = {}, hashes = Object.create(null);
                for (const [role, key] of Object.entries(mapping)) {
                    spec[role] = artifacts[key];
                    if (artifacts[key] !== undefined && artifacts[`${key}_sha256`] !== undefined) {
                        hashes[artifacts[key]] = artifacts[`${key}_sha256`];
                    }
                }
                bind(entry, spec, item.name, hashes, true);
                if (artifacts.bundle) {
                    const bundleHashes = {[artifacts.bundle]: artifacts.bundle_sha256};
                    const manifest = resolve(entry, artifacts.bundle, item.name, "manifest", bundleHashes, true);
                    if (manifest?.value?.format === formats.bundle) bundle(entry, manifest.value, manifest.name);
                    else entry.issues.push("Missing or unsupported bundle manifest evidence");
                }
                if (!entry.verdict) entry.issues.push("Coverage status/embedded verdict is a summary, not raw witness evidence");
                finish(entry);
            }
        }
    }
    const groups = new Map();
    for (const item of imports.values()) {
        if (!item || used.has(item.name)) continue;
        const match = rawSuffix.exec(item.name);
        if (!match) {
            const entry = blank(item.name), value = item.value;
            if (value?.format === formats.snapshot && value.version === 1) {
                evidence(entry, item, "snapshot");
                attach(entry, "snapshot", item);
            } else if (value?.format === formats.trace) {
                evidence(entry, item, "trace"); attach(entry, "trace", item);
            } else if (object(value) && typeof value.status === "string" && !value.format) {
                evidence(entry, item, "verdict"); attach(entry, "verdict", item);
            } else {
                if (item.value !== null && !item.error) issues.push(`${item.name}: unbound or unsupported artifact`);
                continue;
            }
            entry.binding = "unpaired";
            entry.issues.push("Unpaired artifact: use an explicit manifest to bind its role and companions");
            finish(entry);
            continue;
        }
        const [, stem, suffix] = match;
        if (!groups.has(stem)) groups.set(stem, blank(stem));
        const entry = groups.get(stem), role = rawRole(suffix);
        if (entry.provenance.some(p => p.role === role)) {
            entry[role] = role === "query" ? "" : null;
            if (role === "query") delete entry.querySha256;
            entry.issues.push(`Ambiguous ${role} files for ${stem}`);
        } else { evidence(entry, item, role); attach(entry, role, item); }
    }
    for (const entry of groups.values()) {
        entry.binding = "filename";
        entry.issues.push("Filename grouping is not cryptographic pair binding; use an explicit explorer manifest");
        finish(entry);
    }
    return {cases, issues};
}
