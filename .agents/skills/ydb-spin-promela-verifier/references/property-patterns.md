# Property patterns

Use local assertions for immediate invariants, for example:

```promela
assert(!(owner_a && owner_b));
```

Name LTL claims so each run selects one property:

```promela
ltl safe_unique_owner { [] (!(owner_a && owner_b)) }
ltl live_request_completes { [] (pending -> <> (!pending || cancelled)) }
```

Bind request identity or epoch into the model when one completion must not satisfy
another request. `[] <> progress` requires infinitely repeated progress and is
usually too strong for a finite workload that terminates successfully.

For a manual safety run, compile with `-DSAFETY -DNOCLAIM`. For inline LTL, generate
all claims with `spin -a`, compile without those flags, and select the name with
`pan -a -N claim_name`. `spin -N` selects a never-claim file, not an inline claim.
Weak process fairness uses `pan -a -f` and sufficient `-DNFAIR` bookkeeping; it
must be justified by the intended environment. See
[Pan options](https://spinroot.com/spin/Man/Pan.html).

Replay an actual counterexample before changing the model or property. An
impossible environment needs a documented assumption; a weaker property needs a
contract-based reason. A completed bounded search proves only the modeled bounds.
