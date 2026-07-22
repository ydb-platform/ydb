# Decimal `SUM` runtime witness

This isolated manual target checks whether `SUM(Decimal(35,0))` depends on
column-table partitioning. It puts exactly the same complete rows in one- and
two-partition tables and runs them through both the new RBO with fallback
disabled and the legacy/YQL optimizer.

Run it directly:

```bash
./ya make --build relwithdebinfo -tA --test-tag ya:manual \
  ydb/core/kqp/opt/rbo/verification/runtime_ut
```

The current expected outcome is a deliberate `CONFIRMED_MISMATCH` failure. A
representative observation is:

```text
new RBO {one=M, two=inf, DqPhyHashCombine=2}
legacy  {one=M, two=inf, DqPhyHashCombine=0}
```

Here `M = 99999999999999999999999999999999999`. The harness first checks the
physical partition counts, exact raw rows, and consistency-hash routing. The
final assertion asks only for partition invariance; it does not choose a
preferred overflow policy. The result demonstrates a shared
distributed Decimal aggregation/runtime problem, not a bug attributable only
to the new RBO. The target is manual and is not recursed by the aggregate
verification test target while the witness is intentionally failing.

The bounded semantic verifier deliberately does not attempt this proof.
Decimal aggregate addition saturates and is non-associative when finite
overflow is possible, so it requires the sum of absolute finite-input bounds
to stay strictly below `10^35`. This witness exceeds that headroom; the
verifier therefore fails closed instead of certifying an unsound equivalence.
