# RBO Observatory

A standalone, local, read-only explorer of saved verifier evidence. No package
dependencies, backend, telemetry, optimizer execution, or solver execution.
It is a sibling of `verification`, outside its recursive build and trusted core.

## Open

From the repository root:

```bash
python3 -m http.server 8765 --bind 127.0.0.1 \
  --directory ydb/core/kqp/opt/rbo/verification_explorer
```

Open **http://localhost:8765** in a modern browser. A local HTTP server is needed
for ES modules and the bundled example files; opening `index.html` as a file is
not supported. Nothing needs to be installed for the app itself.

The [two authentic examples](demo/README.md) include a saved q40 model candidate
with a fixed-witness trace, and a staged TPCH q12 bounded proof. They are dated
evidence, not claims about the latest optimizer. q40 is **not runtime-confirmed**.

## Explore

- **Plan comparison:** operator DAGs or stage routing; zoom, fit, focus a node's
  neighborhood, and follow its recorded inputs/consumers. Shared nodes are not
  duplicated. IDs are local to each side; no cross-plan correspondence is inferred.
- **Audit desk:** compact fields, exact decoded operator JSON, saved per-task
  model rows, and copyable repository source locations. Source text is not
  bundled: open the indicated file at the relevant producer revision.
- **Counterexample:** the saved database, enabled outcomes, NULLs, query errors,
  choices, ordering and unmatched root outcomes. An error's row payloads are not
  presented as successful output. Independent outcome selections do not imply a
  compatible global schedule.
- **Evidence:** recorded verdict, bounds, mode, nonempty-output diagnostics,
  exact query and raw artifact hashes. No runtime confirmation is inferred.

Use `/` to search cases. The interface also supports keyboard node selection,
mobile layouts and reduced-motion preferences.

## Import your evidence

Choose **Open artifacts**, or drop files into the page. Files stay in browser
memory; imported paths are never fetched, opened on disk, or executed.
Display limits: 256 files, 64 MiB total, 16 MiB per file, 1,000 nodes per plan.
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
    "query": "query.yql",
    "hashes": {"initial.json": "<64 hexadecimal SHA-256 digits>"}
  }]
}
```

Select the manifest **and** its referenced files together. Every supplied hash
is checked against original bytes; hash all referenced artifacts for full byte
binding. Optional `provenance_notes` and `trace_producer` preserve mixed producer
revisions. A manifest is not signed and its claims are not authenticated.

Without a manifest, shared stems such as `q.initial.json`, `q.final.json`,
`q.verdict.json`, `q.trace.json`, and `q.query.yql` are grouped with a visible
weaker-association warning. An arbitrary snapshot filename is displayed as
unpaired, without assigning its boundary role.

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

`graph.mjs` lays out recorded topology. `app.mjs` renders views; `style.css`
owns appearance. None implements SQL evaluation or changes a verifier result.
The verifier must not import this app. Display validation is not semantic
admission, and the viewer is not an independent proof checker.

The canonical SMT is not the actual solver portfolio transcript. Symbolic
per-node predicates, row-lineage links, exact proof derivations and successful-
example traces are not exported by the current inspector; this app does not
invent them. Rendering-derived semantic hashes are revision-specific and are
not interchangeable with raw-file SHA-256. Trace generation can require a
separate solver run on the saved witness; opening the explorer never does.

## Check

The small dependency-free tests cover precision, evidence binding, malformed
inputs, joint bundles and graph topology:

```bash
node --test ydb/core/kqp/opt/rbo/verification_explorer/*.test.mjs
```

Browser smoke checklist: open both examples; select a Join; inspect Model rows
and Raw IR; switch to Stages; inspect the q40 output mismatch; open Evidence;
import the manifest and its files; verify a wrong hash and mixed trace are
visible; check a narrow viewport. Visual screenshots are not committed as a
second large regression corpus.
