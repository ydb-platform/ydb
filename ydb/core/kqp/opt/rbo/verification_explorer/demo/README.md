# Authentic saved examples

These files are copied evidence, not hand-written or mutated plans. The
manifest hashes the exact imported bytes, including the captured snapshots'
absence of a final newline. No SMT formulas, runtime-confirmation claims, or
invented execution traces are included.

## Provenance and limits

Both snapshot pairs, queries, and verifier verdicts came from the 2026-09-06
real-host census at `6abc462dc40cb232268e994b7a7222940c4ed363`, with two row
slots per table and two tasks. Their original artifact registry is the local
`/tmp/rbo-proof-depth-1FnuVM/repaired-census-receipt.json`, SHA256
`6c29f6856fc01a146ed0605c8bdc9df164a7468fb16ef5e65db54c3cd55f9ec7`.

- **TPC-DS 40:** `tpcds-0/testing_out_stuff/tpcds_q40.*` in that census.
  The initial/final plans contain 17/16 nodes. The saved `COUNTEREXAMPLE`
  remains a **model candidate**, not a runtime-confirmed optimizer defect.
  Opaque Decimal zero/default wrappers are a suspected abstraction mismatch;
  see the verifier's `FINDINGS.md` for the distinction.
- **TPC-H 12:** `tpch/testing_out_stuff/tpch_q12.*` in that census.
  These 10/9-node plans have a saved `VERIFIED_BOUNDED` verdict. It covers the
  admitted model at the recorded bounds, before physical lowering. No failing
  execution trace exists for this proof, and none is fabricated here.

The q40 trace was generated on 2026-09-06 with the current inspector at
`d337c21e4b4e8fe57cb2b4c15db66795cab2e0fa` plus its renderer-fidelity update
(`inspector/plan.py` SHA256
`9150998e3693ecc7c8808d115cab9ac820c7d696e348aca6a5bce16f45009f5f`).
The solver SHA256 was
`b497e052b101371eea902fb76c643808b6776a7746e67a804c4d0a13ef43e1fc`.
The trace's decoded database equals the saved verdict's witness exactly;
the inspector may choose internal model valuations on that fixed database.
Its input digests bind the corrected inspector rendering and exact query,
not a runtime execution or an arbitrary pretty-printing of the JSON files.

## Reproduce the fixed-database walkthrough

After building `verification/inspect_bin` and `contrib/tools/z3`, run from the
repository root:

```bash
demo=ydb/core/kqp/opt/rbo/verification_explorer/demo
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect witness \
  "$demo/q40.before.json" "$demo/q40.after.json" \
  --rows 2 --timeout-ms 60000 --solver contrib/tools/z3/z3 \
  --verifier-verdict "$demo/q40.verdict.json" --query "$demo/q40.query.yql"
```

Exit code 1 means a model counterexample trace, not a tool failure. A newer
model or solver may produce a different trace or fail to reproduce the saved
candidate; preserve the original files and record that result separately.
