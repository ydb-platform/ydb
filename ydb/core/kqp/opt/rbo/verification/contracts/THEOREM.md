# Bounded theorem

By default (no `semantic_mode`), for a strictly accepted initial snapshot `I`,
final snapshot `F`, and row bound `N`, the verifier constructs:

```text
catalog constraints
and one shared symbolic database with at most N present rows per table
and not Equal(Eval(I), Eval(F))
```

`Equal` compares the complete modeled result languages. It distinguishes
success from query error, compares successful unordered results as bags,
preserves observable order, correlates choices created by shared DAG nodes, and
allows independent choices where executions are independent. Root names,
column order, types, and nullability are checked before the formula is built.

If the pinned solver returns `UNSAT`, `VERIFIED_BOUNDED` means:

> For every database represented by the declared row bound and catalog
> constraints, both modeled execution languages are nonempty and contain the
> same observable outcomes within the fixed task semantics.

This is mutual language equality, not equality of every pair of executions:
two independent legal choices may produce different results in the same language.

A v1 `buffered_tuple_or_error` result bundle applies this comparison to the
**joint** ordered tuple of result slots, not to separate marginal proofs. All
slots share one database, routing and scalar-function identities; root-local
choices have independent scopes. Each successful slot retains its own bag or
sequence semantics, while an error in any slot produces one query error and
suppresses the whole tuple. Complete execution pairs compare their slots directly,
before the language quantifiers bind the whole joint choice vector. Streaming partial results, effects and
cross-result relational bindings are outside this manifest contract.

`semantic_mode: "binary64_uf_universal_v1"` selects a different, sufficient
obligation: both languages must be nonempty and **every enabled initial/final
schedule pair must agree**. Binary64 arithmetic uses shared primitive
uninterpreted functions with explicit state transitions, and visitation/flush
schedules conservatively include runtime executions. This stronger check avoids
mistaking equality of two enlarged abstract languages for runtime equivalence.
It can be inconclusive even for equivalent plans. If any snapshot requests this
mode, both sides and every result slot use it; verdicts record the mode.

In this mode, `UNSAT` proves bounded agreement under the
[primitive and scheduling assumptions](EXTERNAL_ASSUMPTIONS.md).
Semantic `SAT` is always `UNKNOWN`, without a concrete counterexample or witness;
it can reflect abstraction or schedule dependence. The integral-AVG
count-at-most-two exclusion below remains mandatory. Omitting the mode preserves
the default comparison and its existing admission rules.

The canonical formula is retained as one grouped mismatch assertion. The
ordinary solver schedule first gives that check at most three quarters of the
one global deadline. If it returns `UNKNOWN`, the verifier replaces only that
assertion with an exact distributive cover. In default mode this is: no enabled
left language, no enabled right language, then one guarded unmatched-result
predicate for each normalized left and right outcome. The explicit binary64
mode instead uses the two language-absence branches and one differing enabled
schedule-pair branch per normalized pair.

In default mode there is one exact branch-first exception. When both result families are
ordered singletons with no decisions or choices, have position-compatible
schemas, carry the same nonempty positional null-safe unique key, and publish
the same positional order covering that key, `relation.py` may attach a
preferred keyed cover. It contains the possible language-absence and
asymmetric-error cases, bidirectional per-live-row key absence, and non-key
payload mismatch from the side with fewer live slots. Under the trusted
uniqueness certificate, bidirectional key inclusion gives the same set with
one row per key; one complete payload direction and the shared total order therefore
give exact sequence equality. The solver skips the canonical probe only for
this cover. Ineligible or over-budget shapes retain the ordinary schedule.

In either schedule, `VERIFIED_BOUNDED` requires the canonical assertion to be
`UNSAT` or every selected exact branch to be `UNSAT` before the same deadline.
Any branch `SAT` settles satisfiability immediately; the mode-specific rules
still determine whether it is a candidate or `UNKNOWN`. An unresolved or
untried branch prevents a proof. Where model extraction is allowed, it reruns
the exact winning assertion without resetting the deadline. Mandatory
proof-domain exclusions run before both schedules.
The first branch `UNKNOWN` is retained if the remaining deadline later
expires; a deadline message is synthesized only when no earlier unknown exists.

The critical construction invariant is:

```text
canonical mismatch = OR(general exact solver branches)
preferred admission premises =>
    canonical mismatch = OR(preferred keyed solver branches)
```

These equalities concern the selected mode's obligation. `relation.py`
assembles the canonical and general forms by local Boolean
distribution. The preferred equivalence additionally relies on the admitted
null-safe uniqueness and key-covering total order. Solver-backed representative
regressions prove both equalities, including sparse nullable composite keys,
and pin branch order. A future edit to either representation requires reviewing
the construction itself, not merely rerunning one workload. `--emit-smt`
deliberately writes the canonical monolithic formula for stable inspection. It
is the selected proof obligation, not a transcript of the internal portfolio, so solving
that one file can have different performance or return `UNKNOWN` where the
portfolio succeeds.

Private `DecimalSumState` composition changes the construction cost of one
directly linked split Decimal SUM, not the theorem. An admitted intermediate
state records original-input presence, the three Decimal-special predicates,
the exact finite total, and its proven finite bound beside the authoritative
materialized scalar. Combining guarded states is exactly equivalent to
flattening their guarded original input bags: presence and special predicates
compose by disjunction and finite totals by addition. Finishing then applies
the existing NaN/opposing-infinity/single-infinity/finite precedence once.
The combined finite bound must stay strictly below the maximum-precision
accumulator limit. Any missing certificate, malformed reconstruction, lineage
near miss, or insufficient headroom retains the established scalar path or
fails closed. No ghost state enters result equality or the snapshot wire.

Some admitted operations require separate model-domain proofs. The raw
assertion is their disjunction with the semantic mismatch:

```text
semantic mismatch
or any reachable model-domain exclusion
```

The solver checks the exclusion disjunct first. It must be `UNSAT`; `SAT` or
`UNKNOWN` returns `UNKNOWN` before classifying a semantic mismatch. No database
constraint assumes an exclusion away. Standalone `SAT` of the raw disjunction
is not a counterexample: it does not identify which disjunct fired.

For a checked String `Unwrap` projection marked `require_total`, the exclusion
is an enabled, successful input execution containing a present row whose source
column is NULL. It is observed at the actual Project input, including stage-edge
overrides, and existentially binds all carried choices. Discharging it makes
eager evaluation harmless regardless of downstream demand. The unmarked form
retains its existing physical-storage and result-root/private-keyed-LeftSemi
error-demand contract. Neither form is admitted inside a relational subplan.

Cardinality-certified integral `AVG` excludes reachable successful non-NULL
completed states with count greater than two. In default mode it uses one shared
`(count,min,max) -> result` uninterpreted function. It is exact for an
unordered non-NULL `Int64` multiset of size at most two, because the summary
uniquely identifies that multiset, but it over-approximates binary64 equality
between different summaries. Consequently semantic `SAT` is also
`UNKNOWN` pending exact binary64 replay. Only model-domain `UNSAT` followed by
semantic `UNSAT` can produce `VERIFIED_BOUNDED`. This extra abstraction rule
applies when the passive AVG function is used, not merely because another
operation has a totality obligation.

The command-line default is two row slots per table. Stage execution has a
fixed bound of two tasks; a modeled stage may use one or two. Explicit type,
value, expression, relation, choice, and construction ceilings are also part of
the accepted subset. Crossing a ceiling returns `UNSUPPORTED`; it does not
silently weaken the formula.

This is not unbounded SQL equivalence. It does not cover unsupported semantics,
inputs with more than `N` rows per table, execution with more than two tasks,
`ConvertToPhysical`, the execution engine, optimizer optimality, or error
codes/text beyond the modeled success/error distinction. `FORMULA_EMITTED` and
`UNKNOWN` are not proofs. A satisfiable semantic-mismatch assertion is normally
a symbolic candidate and may need real-YDB replay because admitted opaque
values can over-approximate runtime behavior. Raw model-domain disjunctions and
explicit binary64 mode are stricter: standalone `SAT` is not classified as a
counterexample.
