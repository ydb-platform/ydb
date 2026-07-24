# Manual real-YDB runtime diagnostics

This target keeps paired legacy/new-RBO runtime diagnostics separate from the
bounded semantic verifier. Run one diagnostic by test name; running the whole
manual target still fails deliberately on the unresolved Decimal witness
described below.

## Shared-IU String `IN`

The minimized q56/q60 shape uses two `item` rows with the same `i_item_id` and
one matching `store_sales` row. CBO is explicitly disabled. Before the fix,
legacy execution returned `("same", 10)` while new RBO returned no rows.

`TPushFilterIntoJoinRule` had treated IU-set membership as side identity. When
an IU occurred on both `LeftSemi` inputs, the rule could turn an equality
available entirely on the left into an additional semi-join key. The
finding-only reproduction is commit `6a2c3acb29b`; the exclusive-ownership fix
and direct rule regression are commit `98176b0b48c`. The normal nonmanual
runtime regression is commit `4f73b38aaaf`.

Run the paired regression directly:

```bash
./ya make --build relwithdebinfo -tA --test-tag ya:manual \
  ydb/core/kqp/opt/rbo/verification/runtime_ut \
  -F '*StringInRuntimeDiagnostic*'
```

The expected result after the fix is one passing test: both optimizers return
the same expected row.

## Decimal `SUM`

This isolated manual target checks whether `SUM(Decimal(35,0))` depends on
column-table partitioning. It puts exactly the same complete rows in one- and
two-partition tables and runs them through both the new RBO with fallback
disabled and the legacy/YQL optimizer.

Run the full manual target:

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
