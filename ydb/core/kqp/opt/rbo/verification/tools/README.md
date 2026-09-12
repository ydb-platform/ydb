# Transformation localizer

`kqp_rbo_bisect` is diagnostic machinery outside the verifier kernel.
`protocol.py` owns capture/verifier I/O, `bisect.py` schedules comparisons,
and `cli.py` handles arguments and exit codes.

```bash
kqp_rbo_bisect \
  --verifier /path/to/kqp_rbo_verify --solver /path/to/z3 \
  --artifacts /tmp/new-localization-directory \
  -- /path/to/kqp_rbo_prefix_capture \
     --schema /path/to/tpcds.sql --query /path/to/q96.yql \
     --benchmark-column-store
```

## What it checks

The ordinary Initial/Final check runs first. By default, only a final
`COUNTEREXAMPLE` or `SCHEMA_MISMATCH` starts localization. `VERIFIED_BOUNDED`,
`UNKNOWN`, and `UNSUPPORTED` finals stop without blaming intermediate states.
Use `--all-steps` to explicitly audit those states too, including changes that
cancel before the final boundary.

The default `--strategy divide-and-conquer` compares initial→middle and
middle→final, then recursively checks both halves. **An equivalent interval
is still split.** Equivalence is not monotone; pruning would miss canceling
changes. Captures and comparisons are cached, and each event's immediately
preceding/following snapshots are checked once. For N events this costs N
prefix captures and up to 2N diagnostic comparisons, including the final suffix,
in addition to the ordinary final check. This is midpoint-first scheduling,
not logarithmic binary search.

Only a non-equivalent **adjacent pair** enters `findings`. It identifies an
observed change at that captured boundary, not a confirmed runtime bug.
An `ATOMIC_STAGE_COMMIT` is attributed to the whole stage, never an internal
rule. The last prefix→final comparison is a separate global-suffix finding.
Unsupported exports/checks and timeouts remain gaps; they never implicate a
rule. Other intervals continue to be checked.

Pair checks use `--diagnostic-transformation-pair`, with the explicit
`OPTIMIZER_TRANSFORMATION_PAIR` scope and the original initial snapshot as
`--diagnostic-observation-snapshot`. Its bag/sequence contract applies to
every pair: incidental physical ordering cannot create a finding for an
unordered query, and an ordered query cannot silently become a bag check.
The anchor is evaluated separately to derive that contract; none of its
constraints enter the pair obligation. This adds construction work, not an
extra solver query. An already incompatible prefix schema is a context gap,
not another finding against every subsequent rule.
Both compared snapshots may be logical or staged; staged execution is
evaluated on either side, not stripped off.
Independent execution choices are namespaced per side while physical source
and hash-routing contracts remain shared. Normal Initial/Final admission is
unchanged. Returned verdicts must match the requested row bound and the
supported two-task contract. Results are model diagnostics, not runtime confirmation.

`--strategy sequential` preserves the previous initial→prefix scan: it stops
at the first supported failure, reporting a conservative interval across any
gaps. It uses `OPTIMIZER_TRANSFORMATION_PREFIX` scope and does not accept
`--all-steps`. It does not claim to find every non-equivalent adjacent step.

## Saved result

All snapshots, capture manifests, raw process streams, raw verdicts, exact
canonical SMT-LIB, and `result.json` remain in the new artifact directory.
An exhaustive investigation emits `format: "ydb-rbo-transformation-localization"`,
`version: 1`, with these fields:

| Field | Meaning |
| --- | --- |
| `events` | Ordered committed events, with ordinal, kind, stage, and name. |
| `boundaries` | Ordinal 0 initial, 1..N prefixes, N+1 final; capture status and artifact references. |
| `observation_boundary` | Always 0: the original query fixes bag versus sequence observation for every diagnostic pair. |
| `observation_kind`, `observation_snapshot_sha256` | Confirmed `bag`/`sequence` contract and the exact initial bytes. Kind is null if no pair reached observation derivation. |
| `comparisons` | Midpoint-first checks with ID `left:right`, endpoint ordinals, `adjacent`, raw `verifier`, and artifact references. |
| `findings` | Failing adjacent comparison ID plus its event or global-suffix region. |
| `gaps` | Every unresolved comparison ID, endpoints, status, and adjacency. |
| `completeness` | `COMPLETE` when every adjacent check is conclusive; otherwise `GAPS`. A coarse timeout need not leave an adjacent gap. |
| `events_attempted`, `events_checked` | All event boundaries visited; adjacent event pairs actually sent to the verifier, respectively. Export gaps are not counted as checked. |

Artifact references are `{path, sha256}`, relative to `result.json`'s directory.
Boundaries reference `snapshot`, `capture`, `command`, `stdout`, and `stderr` when present;
comparisons reference `verdict`, `formula`, `command`, `stdout`, and `stderr` when present.
An exporter gap has no fabricated snapshot or verdict file. Pair identity is
bound through the endpoint ordinals and hashed boundary snapshots. Findings
and gaps refer back to the comparison's exact verdict; witness data is not
duplicated into each finding.

`LOCALIZED_FAILURES` means at least one observed adjacent failure, possibly
with gaps; inspect `completeness`. `LOCALIZATION_INCOMPLETE` means gaps without
an adjacent finding. `STEPS_VERIFIED_BOUNDED` means every adjacent check was
bounded-verified; it does not replace `final_verifier`. Exit codes are 1 for
findings, 0 for bounded-verified steps/final, and 2 for inconclusive/error results.
Without an investigation, final-gate and legacy sequential results retain
their existing summary format.

## Capture contract

The driver appends these arguments to each capture invocation:

```text
--rbo-transformation-prefix-ordinal N --rbo-transformation-prefix-output DIRECTORY
```

It first requests `max_events + 1` to capture optimizer completion and the
full event stream. Reaching that ordinal is an error: increase `--max-events`.
Each subsequent request returns the complete committed prefix through N:

```json
{
  "protocol": "ydb-rbo-transformation-prefix-capture-v2",
  "requested_ordinal": 1,
  "status": "PREFIX_CAPTURED",
  "initial_snapshot": "initial.json",
  "prefix_snapshot": "prefix.json",
  "events": [
    {"ordinal": 1, "kind": "RULE_APPLICATION", "stage": "rewrite", "name": "PushFilter"}
  ]
}
```

`OPTIMIZER_COMPLETE` replaces `prefix_snapshot` with `final_snapshot` and has
fewer events than requested. `PREFIX_UNSUPPORTED` or `FINAL_UNSUPPORTED`
instead carries a nonempty `unsupported_reason`. No missing snapshot is
invented. Paths must remain inside the capture directory. Every rerun must
match the completion's event prefix and initial snapshot SHA-256; divergence
aborts localization. Each cached boundary's SHA is pinned on capture, checked
on reuse and report emission, and verified unchanged across verifier execution.
Event-prefix matching guards reruns; it is not a proof of arbitrary optimizer
determinism or a claim that all boundaries came from one execution.

`kqp_rbo_prefix_capture` creates an isolated in-process YDB, executes the schema,
and prepares the query once with new RBO enabled and fallback disabled.
`--benchmark-column-store` applies the dashboard's schema rewrite and query
prelude. Inputs must be nonempty regular files; output directories must be
new or empty. `capture.json` is written last as the commit marker.

The stream records committed `TRuleBasedStage` applications as
`RULE_APPLICATION`, and each mutating non-rule stage as `ATOMIC_STAGE_COMMIT`.
Constant folding and final hash propagation are included; internal CBO
decisions remain one event at the enclosing CBO commit boundary.
