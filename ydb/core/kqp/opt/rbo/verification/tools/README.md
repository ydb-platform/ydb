# Sequential rule-prefix localizer

`kqp_rbo_bisect` is diagnostic machinery outside the verifier kernel. Despite
the historical name, it never uses binary search: equivalence is not monotone
across rule applications.

The implementation has three acyclic audit boundaries: `protocol.py` validates
capture/verifier I/O, `bisect.py` contains the sequential state machine, and
`cli.py` contains argument and exit-code handling.

The localizer invokes the capture command once per ordinal, appending:

```text
--rbo-rule-prefix-ordinal N --rbo-rule-prefix-output DIRECTORY
```

The driver first requests `max_applications + 1`. A normal optimizer completion
contains fewer applications and returns the final snapshot:

```json
{
  "protocol": "ydb-rbo-rule-prefix-capture-v1",
  "requested_ordinal": 10001,
  "status": "OPTIMIZER_COMPLETE",
  "initial_snapshot": "initial.json",
  "final_snapshot": "final.json",
  "applications": [
    {"ordinal": 1, "stage": "stage name", "rule": "rule name"},
    {"ordinal": 2, "stage": "stage name", "rule": "rule name"}
  ]
}
```

The ordinary initial-to-final verifier runs before any prefix diagnostics. A
verified final plan ends the investigation without reporting harmless or
temporarily unexportable intermediate states as optimizer bugs. An unsupported
or unknown final check is reported honestly. Only a final counterexample or
schema mismatch starts a sequential scan of ordinals `1..N`.

A captured prefix uses the same full application list and adds its snapshot:

```json
{
  "protocol": "ydb-rbo-rule-prefix-capture-v1",
  "requested_ordinal": 2,
  "status": "PREFIX_CAPTURED",
  "initial_snapshot": "initial.json",
  "prefix_snapshot": "prefix.json",
  "applications": [
    {"ordinal": 1, "stage": "stage name", "rule": "rule name"},
    {"ordinal": 2, "stage": "stage name", "rule": "rule name"}
  ]
}
```

Snapshot paths are relative to the capture directory. Every run must list its
whole committed application prefix. The driver compares that list and the
SHA-256 identity of the initial snapshot across reruns, failing if optimizer
behavior is unstable.

Some CBO and temporary operator prefixes cannot be exported. The capture
command represents that directly, without inventing a snapshot:

```json
{
  "protocol": "ydb-rbo-rule-prefix-capture-v1",
  "requested_ordinal": 2,
  "status": "PREFIX_UNSUPPORTED",
  "initial_snapshot": "initial.json",
  "unsupported_reason": "stable exporter diagnostic",
  "applications": [
    {"ordinal": 1, "stage": "stage name", "rule": "rule name"},
    {"ordinal": 2, "stage": "stage name", "rule": "rule name"}
  ]
}
```

`FINAL_UNSUPPORTED` has the same fields when final export fails and contains
fewer applications than the sentinel request. These exporter gaps, plus
verifier `UNSUPPORTED` and `UNKNOWN` results, are recorded and scanning
continues. A failing prefix is exact only immediately after a verified prefix;
otherwise the result gives the conservative interval from the last verified
ordinal plus one to the first observed failing prefix. Trailing gaps before a
failing final boundary likewise prevent blaming the global suffix.

Capture output, verifier output, exact SMT-LIB, snapshots, manifests, gaps, and
`result.json` remain under the new artifact directory.

Example shape:

```bash
kqp_rbo_bisect \
  --verifier /path/to/kqp_rbo_verify \
  --solver /path/to/z3 \
  --artifacts /tmp/q96-rule-prefixes \
  -- /path/to/kqp_rbo_prefix_capture \
     --schema /path/to/tpcds.sql \
     --query /path/to/q96.yql \
     --benchmark-column-store
```

`kqp_rbo_prefix_capture` creates an isolated in-process YDB, executes the schema,
and prepares the query exactly once with the new RBO enabled and fallback
disabled. `--benchmark-column-store` applies the same column-store schema
rewrite and query prelude as the TPCH/TPC-DS coverage dashboard. Input files
must be nonempty regular files. The artifact directory must be new or empty;
the command refuses to overwrite any artifact and writes `capture.json` last as
the commit marker.

The capture command currently numbers committed `TRuleBasedStage` applications.
Constant folding and final hash-function propagation are mutating atomic stages,
not rules, and are not yet represented in that sequence. A final failure after
hash propagation is conservatively a global suffix failure. Constant folding
can occur between numbered applications, so an apparent first failing rule
prefix must currently be read as an interval that may include the preceding
constant-fold stage. The next protocol revision will add explicit transformation
event kinds and atomic-stage checkpoints before exact blame is claimed.
