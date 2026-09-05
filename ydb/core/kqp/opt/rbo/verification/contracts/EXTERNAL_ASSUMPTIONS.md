# External runtime assumptions

The production optimizer claim additionally relies on facts not established by
the SMT obligation itself:

- the host invokes the initial hook after `TOpRoot` construction and parent
  computation but before the first new-RBO stage, and invokes the final hook
  after the last stage/property recomputation but before physical generation;
- the shared captured catalog and exported initial/final roots are the plans
  actually present at those boundaries, and instrumentation does not alter
  optimization;
- each accepted exporter encoding and Python semantic rule agrees with the
  corresponding YQL, RBO, KQP task-construction, and runtime behavior;
- each accepted concrete String or Utf8 literal denotes exactly its recorded
  strict-UTF-8 byte sequence, and runtime equality across those two families is
  raw byte equality without normalization;
- the accepted nullable Unicode-uppercase bridge identifies the deterministic
  built-in `Unicode.ToUpper`; `SafeCast` from String to Utf8 deterministically
  returns NULL for invalid bytes and otherwise supplies that UDF with the
  decoded value, so equal raw String inputs have equal combined nullable
  results;
- each accepted compiled-LIKE program is exactly the deterministic,
  case-sensitive YQL/RE2 `PatternFromLike` plus `Match` operation represented
  by its audited pattern/options fingerprint; the generic and pushed
  `KqpOlapApply` spellings have the same NULL-to-false behavior, and pushed
  outer NOT has the explicit Boolean semantics retained in the snapshot;
- each accepted pushed Boolean coalesce evaluates its `Optional<Bool>` left
  operand once, returns its present payload, and otherwise returns the recorded
  non-null Boolean literal; the compact `is-null OR value` / `not-null AND
  value` encoding is exact for the corresponding true/false fallback;
- each accepted checked nullable-String projection returns the direct source
  payload for a present value and raises an observable query error for a
  present NULL; a main result-root Project is demanded, and the accepted
  private keyed `left_semi` RHS is evaluated as the eager/build side even when
  the left input is empty;
- for an accepted compact ordered singleton, a `sequence` relation without an
  ordinal vector is in the exact fixed runtime row order represented by its
  tuple; each enumerated StageGraph Merge outcome establishes one fixed global
  order before `Limit`, while unordered gather does not preserve `sequence`,
  and runtime `Limit(1)` returns the first present row in that order;
- each accepted `outer_bind` represents one fresh correlated scalar invocation
  with no hidden row-selection, ordering, error, or nondeterministic choice
  semantics beyond the explicitly modeled root;
- each accepted relational `EXISTS` descriptor represents one Boolean presence
  test over the recorded inner root; its ordered dependency values come from
  one outer row, its retained predicate has the strict comparison/NULL behavior
  described above, and it has no hidden row selection, ordering, error,
  coercion, correlation, or fanout beyond the admitted form;
- each accepted dynamic-`IN` descriptor represents one uncorrelated
  existential membership test over the recorded lookup/result columns; for an
  independently nullable fixed-width-integral or Date pair, the binding occurs
  only as a direct positive top-level Filter conjunct where FALSE and UNKNOWN
  both reject the outer row; Date values obey the recorded bounded domain, and
  there is no hidden coercion, correlation, cardinality-error, or fanout
  semantics in the `IN` operator itself;
- each scalar binding consumed inside an accepted dynamic-`IN` root is exactly
  the recorded uncorrelated scalar plan, is demanded at the recorded immediate
  unary consumer, and has no hidden dependency, invocation, choice, error, or
  cardinality semantics beyond the ordinary scalar-subplan model;
- each leaf `IN` binding consumed inside an accepted dynamic-`IN` root is
  exactly the recorded second uncorrelated membership test, has no hidden
  subplan dependency or correlation, and preserves the same NULL, Filter-truth,
  caching, error, and row-pair semantics at that nested consumer;
- each accepted nullable-annotated Date item in a raw static-`SqlIn` tuple is
  the recorded direct literal `SafeCast`, MiniKQL parsing proves the runtime
  value present, and replacing it with the emitted non-null Date literal is
  exact;
- each accepted literal-only String `Concat` evaluates by ordered byte
  concatenation, has no hidden failure within the audited type, metadata,
  source-size, and allocation bounds, and therefore equals the emitted
  canonical literal;
- each catalog bound carried into a restricted stored-String `Concat` is a true
  runtime upper bound: Datashard enforces 16 MiB per value and validated Arrow
  `BinaryType` storage bounds one Olap cell by `INT32_MAX`; MiniKQL concatenates
  the audited leaves in fingerprint order, returns that deterministic byte
  string when every intermediate sum fits `UINT32_MAX`, and raises an observable
  deterministic query error when a sum exceeds it, with no modeled failure
  depending on stage, task, evaluation count, or ambient allocation pressure;
- every row counted by an accepted unstaged checked-Concat corridor is demanded
  when its selector is nonbinding at the declared row bound, and a checked
  Project at the staged result root is demanded for every present result row;
- each accepted `opaque_double` denotes the recorded deterministic q83
  average/deviation expression over exactly three direct nullable Int64
  arguments, its fingerprint preserves the complete reviewed callable,
  literal, type, and ordered-use identity, and transporting the result as a
  non-key payload does not inspect or alter its runtime Double value;
- each certified `ExpandMultiDistinct` physical
  `Nothing(Optional<Tuple<Decimal(35,s),Uint64>>)` pad denotes an absent
  logical Decimal AVG lane, identity `UnionAll` and payload-only StageGraph
  transport do not inspect or alter that state, and the matching Final AVG
  consumes the intermediate `(sum,count)` states with count-weighted semantics;
- each accepted directly linked intermediate/final Decimal `SUM` ignores NULL
  original inputs, materializes exactly the visible intermediate scalar
  represented by the private state, and applies the same special-aware
  aggregate operation to non-NULL partial scalars at the final phase. A
  non-nullable empty intermediate materializes present zero; nullable empty
  input remains NULL. Within the verifier-checked finite headroom, flattening
  selected original bags and composing their partial states are therefore
  observationally identical;
- each accepted integral `AVG` trait denotes exactly the recorded
  `Optional<Int64> -> Optional<Double>` runtime aggregate, intermediate
  `(count,min,max)` is an exact summary of its original non-NULL inputs and
  composes exactly across the directly linked final phase, and non-NULL count
  at most two makes that summary sufficient to identify the unordered input
  multiset;
- each accepted `integral_avg_rank_v1` Sort or Merge key is the recorded
  completed integral AVG carried through only the certified direct dataflow,
  and runtime binary64 ordering of those results is a total preorder whose
  equivalence classes can be embedded in the verifier's integer rank;
- each accepted fixed-width integral `MIN`/`MAX` trait uses the recorded
  signed/unsigned comparison for its exact input/output type, ignores NULL
  inputs, and combines intermediate values with the same associative extremum
  operation;
- the producer observer sees every successful completed integral-AVG result
  before any parent transformation, and the completed certificate has no
  runtime value or downstream transport semantics beyond constructing the
  model-domain exclusion;
- each accepted repeated-source Map rename copies the same runtime input value
  into every declared distinct output, suppresses the original source once,
  and preserves the recorded Map-element order without hidden computation;
- each accepted side-explicit JoinKey names the actual left and right runtime
  values, each admitted shared-IU semi/anti join exposes only its selected
  side, and StageGraph occurrences with equal IU spellings remain distinct
  runtime streams;
- source placement and HashShuffle routing assign every present row to exactly
  one modeled task, so opposite task facts are exhaustive and mutually
  exclusive; a Broadcast gather occurs before runtime replication, while
  HashShuffle gathers producer tasks and then routes each reconstructed
  present row once, preserving the modeled connection multiplicity;
- each accepted `YqlAggWin(sum|avg)` spelling evaluates over the rows visible
  in its current runtime task, groups the complete ordered nullable partition
  tuple with SQL `IS NOT DISTINCT FROM`, and ignores NULL Decimal inputs. SUM
  returns NULL for an all-NULL partition and otherwise uses the exact Decimal
  aggregate addition. AVG additionally carries exact widened Decimal SUM and
  `Uint64` count state, returns NULL for count zero, and for positive count uses
  runtime Decimal division with nearest/even rounding plus same-scale
  narrowing;
- each accepted q49 `YqlWin(rank)` spelling evaluates its complete unpartitioned
  task input using the recorded ascending/null-first Decimal key, ordinary
  Decimal equality for peers, SQL rank gaps, and one independent unstable-sort
  ordinal family per leaf; raw Decimal order is `-Inf < finite < +Inf < NaN`,
  separate NaN rows need not be peers, `CalcOverWindow` publishes no output
  sequence, and the final TopSort supplies the observed order;
- each accepted q51 `YqlAggWin(sum|max)` spelling evaluates over exactly the
  rows visible in its current task; SUM partitions required Int64 item values
  and outer MAX partitions nullable Int64 item values, both by `IS NOT DISTINCT
  FROM`; each orders nullable Date ascending with NULLs first and aggregates the
  ROWS prefix from unbounded preceding through the current row;
  each of the four leaves has an independent legal order for equal Date peers,
  SUM and MAX ignore NULL inputs, SUM uses exact Decimal addition within the
  verifier-derived finite-headroom bound, MAX uses raw Decimal order including
  specials and consumes the recorded distinct typed input columns, and no
  Project publishes the window's internal sequence;
- each accepted Decimal `Abs` propagates NULL and applies the MiniKQL Decimal
  builtin's raw signed-`TInt128` rule: `SafeNeg` exactly for a negative code,
  leaving positive infinity and the positive NaN sentinel unchanged;
- runtime HashV2 routes equal nullable keys, including two NULL keys, to the
  same task; a nonparallel `UnionAll` consumer has one task, and no later
  physical rule erases or bypasses the explicit Aggregate-to-window stage
  boundary installed by `70ab3d3631c`;
- each exported catalog unique key denotes the runtime at-most-one constraint
  over present base rows, and an accepted direct scan plus task routing
  preserves each source cell exactly while only strengthening its presence
  guard;
- each accepted direct Uint64 `Just` is always present at runtime and the
  synthetic typed-NULL branch preserves its exact static Optional schema;
- each accepted scalar-final Uint64 `unwrap` denotes the physical builder's
  coalesce-to-zero result and therefore has a non-null effective output for
  empty, all-NULL, and populated inputs;
- each accepted scalar or grouped fixed-width integer or nullable Decimal
  count-distinct uses the runtime raw aggregate equality of its exact input
  type, including self-equality for the Decimal NaN code, ignores absent rows
  and NULL values, partitions rows by the modeled null-safe grouping-key
  equality, and returns one non-null Uint64 count without overflow within the
  declared relation-row bound;
- each accepted `yql-datetime-year-v1` shape denotes the same complete
  Date-to-Timestamp cast and deterministic, total Split/GetYear operation on
  the bound Date payload;
- each accepted Date-`Unwrap` SafeCast spelling converts Int32 zero to a
  present Date zero, exactly like the accepted `Just(Date(0))` spelling, so
  the normalized missing branch is exact and the runtime error path is
  unreachable;
- each accepted `cast_decimal` `source_type` names the actual runtime source,
  weak integral-to-Decimal `SafeCast` propagates source NULL but saturates
  present overflow to signed infinity, and same-scale non-decreasing-precision
  Decimal `SafeCast` preserves every finite and special encoded value; the six
  accepted q49 `Decimal(35,2)`-to-`Decimal(15,4)` casts multiply finite raw
  coefficients by 100, saturate at the target precision boundary, preserve
  specials, and propagate NULL;
- every propagated Decimal integral-arithmetic bound covers all finite runtime
  outputs under the recorded integer type domain; NULL and Decimal specials do
  not create an additional finite result outside that bound;
- opaque fingerprints identify the same runtime function exactly when
  intended, and every admitted opaque expression is deterministic, total, and
  safe to model as an uninterpreted function;
- symbolic Merge input ordinals describe each producer's runtime sequence:
  strict ordinal order must be preserved for two present rows, while equal
  ordinals impose no relative order;
- the fixed two-task routing, hashing, connection, ordering, multiplicity, and
  error semantics agree with the runtime for the admitted StageGraph subset;
- the pinned Z3 executable correctly decides the emitted SMT-LIB formula, and
  the process and output parser return its result without corruption.

Changing a capture point, runtime semantic rule, supported operator field,
opaque positive list, hash/task rule, or solver version therefore requires a
trust-boundary review even when the Python API is unchanged.
