# RBO Observatory

A standalone, local, read-only explorer of saved verifier evidence. No package
dependencies, command API, telemetry, optimizer execution, or solver execution.
It is a sibling of `verification`, outside its recursive build and trusted core.

## Open

From the repository root:

```bash
python3 ydb/core/kqp/opt/rbo/verification_explorer/serve.py
```

Open **http://localhost:8765** in a modern browser. A local HTTP server is needed
for ES modules and the bundled example files; opening `index.html` as a file is
not supported. The launcher generates both demo formula DAGs from this checkout
before serving static files. Generated data stays in ignored `.generated/`, not
in Git. It never calls a solver; source hashes record the formula producer.
Use `--prepare-only` to generate without serving, `--port 8766` for another port,
or `--bind ::1` for IPv6 loopback. An ordinary static server also works; without
preparation, formulas stay unavailable. Remote access should use an SSH tunnel
to loopback (or HTTPS); browser hashing needs a secure context.

The [two authentic examples](demo/README.md) include a saved q40 model candidate
with a fixed-witness trace, and a staged TPCH q12 bounded proof. They are dated
evidence, not claims about the latest optimizer. q40 is **not runtime-confirmed**.

## Explore

- **Plan comparison:** operator DAGs or stage routing; zoom, fit, focus a node's
  neighborhood, and follow its recorded inputs/consumers. Shared nodes are not
  duplicated. IDs are local to each side; no cross-plan correspondence is inferred.
- **Audit desk:** clicking an operator opens its actual exported formulas:
  readable abbreviation, clickable dependencies, and exact typed DAG records.
  Choose a task/scope and a presence, NULL, payload, error, or choice term.
  Global assertions and declarations remain inspectable; fused nodes without a
  separate kernel observation are explicitly unavailable. Fields, concrete
  model rows, Raw IR and source paths remain separate tabs.
- **Counterexample:** unmatched results first, with differing sequence positions,
  typed cells, bag multiplicities, or query-error status. Decimal NaN/Infinity
  markers are decoded exactly; raw payloads remain available. Multiple opposite
  outcomes require explicit selection. The input database is collapsible.
- **Rule history:** import a [localizer](../verification/tools/README.md)
  `result.json` with its referenced snapshot/verdict files, or use **Open rule
  investigation folder** to preserve paths and skip logs/SMT. Inspect non-equivalent
  adjacent steps, unresolved gaps, or midpoint checks. Missing raw verdicts are
  labeled report summaries, not independently established findings. Every pair
  keeps the original query's bag/sequence observation; incidental physical order
  does not become an extra SQL requirement. Unanchored reports are rejected.
  Pair diagnostics retain canonical SMT and witness databases, but pair-specific
  operator formula export and concrete operator traces are not available yet.
- **Evidence:** recorded verdict, bounds, mode, nonempty-output diagnostics,
  exact query and raw artifact hashes. No runtime confirmation is inferred.

Use `/` to search cases. The interface also supports keyboard node selection,
mobile layouts and reduced-motion preferences.

## Import your evidence

Choose **Open artifacts**, or drop files into the page. Files stay in browser
memory; imported paths are never fetched, opened on disk, or executed.
Display limits: 2,048 files, 64 MiB total, 16 MiB per file, 1,000 nodes per plan.
These are viewer limits, not changes to verifier admission. Do not import giant SMT files.

An explicit manifest is the strongest available file association:

```json
{
  "format": "rbo-explorer",
  "version": 1,
  "cases": [{
    "id": "my-query",
    "title": "My query",
    "revision": "capture-and-verdict-producer-commit",
    "before": "initial.json",
    "after": "final.json",
    "verdict": "verdict.json",
    "trace": "trace.json",
    "formulas": "formulas.json",
    "query": "query.yql",
    "hashes": {"initial.json": "<64 hexadecimal SHA-256 digits>"}
  }]
}
```

Select the manifest **and** its referenced files together. Every supplied hash
is checked against original bytes; hash all referenced artifacts for full byte
binding. Optional `provenance_notes`, `trace_producer` and `formula_producer` preserve mixed producer
revisions. A manifest is not signed and its claims are not authenticated.

Without a manifest, shared stems such as `q.initial.json`, `q.final.json`,
`q.verdict.json`, `q.trace.json`, `q.formulas.json`, and `q.query.yql` are grouped with a visible
weaker-association warning. An arbitrary snapshot filename is displayed as
unpaired, without assigning its boundary role.

Generate your own formulas with the built inspector:

```bash
kqp_rbo_inspect formulas q.initial.json q.final.json --rows 2 > q.formulas.json
```

Formula inputs must match both attached snapshots' raw-byte SHA256, bounds and
semantic modes. They are regenerated model terms, not the historical solver's
transcript or a derivation of its saved verdict. Formula imports are limited to
200,000 terms and one million dependency references; shortened views say so.

Version-5 benchmark reports can bind their referenced imported files. Native
version-1 result bundles retain one joint case with a result-slot selector;
their verdict is never reinterpreted as a proof of each slot independently.
Current inspector traces do not cover joint bundles. Missing, ambiguous,
hash-mismatched or structurally malformed evidence stays visibly unavailable.

## Trust boundary and deliberate limits

`artifacts.mjs` decodes JSON without rounding unsafe integers or decimal numeric
tokens, checks display shapes and file associations, and returns plain view
data. Numeric lexemes outside safe integer representation stay strings; typed
cells retain their type fields. “Raw IR” is lossless decoded JSON, not a byte-
identical pretty-print. Every imported label is rendered as text, never HTML.

`graph.mjs` lays out topology. Formula, witness and rule-history views are small
separate modules; `app.mjs` coordinates them and `style.css` owns appearance.
None implements SQL evaluation or changes a verifier result.
The verifier must not import this app. Display validation is not semantic
admission, and the viewer is not an independent proof checker.

The canonical SMT is not the actual solver portfolio transcript. Row-lineage
links, proof derivations and successful-example traces are not invented.
Readable formulas abbreviate the exported AST; they do not simplify its logic.
Quantifiers retain lexical binding; term references are not global substitutions.
Rendering-derived semantic hashes are revision-specific and are
not interchangeable with raw-file SHA-256. Trace generation can require a
separate solver run on the saved witness; opening the explorer never does.

## Check

The small dependency-free tests cover precision, evidence binding, malformed
inputs, joint bundles and graph topology:

```bash
node --test ydb/core/kqp/opt/rbo/verification_explorer/*.test.mjs
```

Browser smoke checklist: open both examples; select a Join; follow formula
dependencies and return; inspect Model rows and Raw IR; switch to Stages;
inspect q40's NaN/−Infinity and NULL/0 differences; open Evidence and Rule history;
import the manifest and its files; verify a wrong hash and mixed trace are
visible; check a narrow viewport. Visual screenshots are not committed as a
second large regression corpus.
