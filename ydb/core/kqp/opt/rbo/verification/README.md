# New RBO equivalence verifier

This directory contains the standalone bounded-equivalence checker described in
[PLAN.md](PLAN.md). It compares two versioned semantic snapshots and asks Z3 for
a bounded input database on which their result bags or ordered sequences differ.
The reproducible 121-query dashboard contract and current unsupported inventory
are recorded in [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md).

The current implementation contains the M1 logical kernel, the M2 C++ boundary
hooks, the supported M3 StageGraph routing slice, and the aggregate, Limit,
ordered Sort/TopSort/Merge, pushed OLAP-filter, restricted static `IN`, exact
Decimal comparison/arithmetic/ordering/SUM, and benchmark-dashboard parts of
M4.
Separate normalized-plan, concrete-counterexample inspection, and isolated
real-YDB replay tools are also implemented. A real-host transformation-prefix capture
command and sequential localizer are implemented outside the verifier kernel.
Committed rule applications and mutating non-rule stages share one explicit
transformation-event stream. Solver-backed tests use the pinned, standalone Z3
target under `contrib/tools/z3`; it is not linked into `ydbd`.
The 2026-07-22 complete formula-only dashboard reaches TPCH q3 and TPC-DS q3,
q48, q52, q55, q71, q88, q93, and q96: 9/121 workload queries. Focused solver
runs return `VERIFIED_BOUNDED` for TPCH q3 and TPC-DS q3, q52, q55, q93, and
q96. TPC-DS q48 has formula-construction coverage only, q71 reached the external
solver process deadline, and q88 is `UNKNOWN`; no optimizer correctness bug has been
confirmed. These results all retain the two-row-per-table/two-task and
pre-physical-boundary qualifications described below.
`CaptureSemanticSnapshotCatalogV1` records the initial query-level catalog once,
and `ExportSemanticSnapshotV1`
deterministically lowers supported RBO operators without doing file I/O. An
optional sink on `TKqlTransformContext` receives the initial snapshot before the
first RBO stage and the final snapshot immediately before physical generation.
`CreateKqpHost` accepts the same sink as immutable instrumentation configuration
and copies it into every per-query transform context created by the host.
Supported final plans include exact stage membership and Map, HashShuffle,
Broadcast, serial or parallel UnionAll, and ordered Merge connections.

Stage execution uses at most two bounded source/shuffle tasks. Source-row task
placement and hash routing are shared symbolic functions, so a two-task source
covers both partitions instead of fixing rows to one convenient task. Map
preserves the producer count, serial UnionAll requires one consumer task,
parallel UnionAll uses the runtime's cross-input round-robin offset, and a
broadcast-only stage uses one task just as KQP task construction does. Invalid
connection combinations are rejected. Merge has one consumer task, validates
that every producer stream has an order compatible with the edge order, and
represents every sorted interleaving that preserves each producer sequence. It
uses explicit interleavings for small inputs and bounded symbolic ordinals once
enumeration would cross the ordinary outcome cap. The final stage is gathered
before root column projection.

Rows carry structural occurrence provenance and facts about symbolic source or
hash routing. When a non-Merge gather sees task copies of the same occurrence
with contradictory routing facts, those guards are mutually exclusive and the copies
can be coalesced exactly, including conditional task-local values. Broadcast
copies have no such proof and retain their bag multiplicity; distinct or unknown
occurrences also remain separate. This keeps routed StageGraphs compact without
silently deduplicating SQL rows.

The exporter and decoder both validate the StageGraph independently: plan nodes
partition into stages, each stage has one logical sink, every cross-stage child
occurrence has one edge, and producer output indices are a bijection with
outgoing edge occurrences. Shuffle-elimination/co-partitioning assumptions are
rejected until the snapshot can substantiate them.

Version one preserves exact supported YQL scalar identities (`Bool`, signed and
unsigned integer widths, `String`, `Utf8`, `Date`, and canonical parameterized
`Decimal(p,s)`) even when several identities use the same SMT domain. Decimal
identity requires `1 <= p <= 35` and `0 <= s <= p`, with no alternate spelling.
Integer identities use SMT integer carriers with exact signed or unsigned
8/16/32/64-bit domains on literals, source cells, and non-null opaque results.
Date is likewise exact: numeric literals, source cells, and non-null opaque
results are constrained to unsigned day values in `[0, 49673)`.

Decimal uses the exact YDB scaled-integer representation. A legal non-null
`Decimal(p,s)` value is a finite code `c` with `-10^p < c < 10^p`, negative or
positive infinity, or NaN. The snapshot literal is explicitly tagged; finite
codes use a canonical signed-integer string, while specials have no `scaled`
field:

```json
{"kind":"literal","type":"Decimal(7,2)","value":{"kind":"finite","scaled":"1234"}}
{"kind":"literal","type":"Decimal(7,2)","value":{"kind":"neg_inf"}}
```

The other special tags are `pos_inf` and `nan`. Source cells and non-null opaque
Decimal results receive the same typed-domain constraint.

Same-type integer `+`, `-`, and `*` are structural scalar nodes. Both operands
and the result must have exactly the same signed or unsigned width, and result
nullability must be the OR of operand nullability; otherwise the expression
remains opaque. Evaluation is strict on NULL and wraps modulo the result width,
using the signed two's-complement representative for `Int8` through `Int64`.
This narrow rule was added when TPC-DS q88 exposed a spurious witness between
source arithmetic constants and optimizer-folded literals.
Source and opaque integer range constraints also prevent out-of-width witnesses
from observing a difference between a direct integer use and its wrapped
arithmetic identity.

Static `SqlIn` is exact for a deliberately narrow shape: a direct raw tuple or
`AsList` with 1..512 recursively supported, non-null items of one scalar type.
That type is identical to the lookup type, or is an integer type using the same
lossless common-type gate as ordinary equality. It evaluates as the SQL
three-valued OR of those equalities, so only a nullable lookup can make the
Boolean result nullable. `ansi`, `warnNoAnsi`, `isCompact`, and
`nullsProcessed` normalize to the same node under this gate. Dynamic, empty,
oversized, nullable, heterogeneous-item, lossy or non-integer mixed-type,
`tableSource`, malformed-option, and unknown-option forms fail closed.

Scalar syntax outside the explicit Boolean, equality, ordering, integer
arithmetic, and restricted static-membership core is represented by a shared
typed uninterpreted function when
the C++ exporter can positively audit it as deterministic and total. The
current reviewed opaque families are scalar comparisons; `Just`, `Exists`,
`Coalesce`, and `If`; `SafeCast`; and non-failing `Convert`. YQL has no complete
generic totality or determinism flag, so all other callables fail closed rather
than relying on a denylist. This includes UDF and PG calls, division, strict
casts, `Unwrap`, runtime-dependent generators, free variables, and unsafe AST
metadata.

A complete cast of a non-null integer constant to a non-null Decimal is handled
before opaque fallback: the exporter evaluates the YDB cast and emits the
resulting tagged Decimal literal. General casts, nullable cast shapes, and
non-integer Decimal cast sources remain unsupported.

The explicit comparison core accepts unequal integer widths only when YQL's
common integer type represents both operands without wrapping: equal signedness,
or a signed type wider than the unsigned type. This covers the canonical
`Int64`-column/`Int32`-literal benchmark form while mixed-width cases that would
bitcast or wrap still fail closed. Date comparison requires Date on both sides.

Ordinary Decimal `=`, `<`, `<=`, `>`, and `>=` are strict on NULL; NaN makes
every ordinary comparison false, and infinities participate in the YDB order.
Decimal/Decimal and Decimal/integer operands use the same scale alignment as
YDB `DataCompare`: integers first receive their declared decimal width, scales
are raised, precision is capped at 35, and narrowing or scale-up saturates to an
infinity exactly where the YDB Decimal conversion does. Alignments that would
require an invalid zero-precision Decimal fail closed. Null-safe Decimal
equality is admitted only for exactly identical Decimal types and compares the
encoded non-null values, including NaN; its usual both-NULL/one-NULL behavior is
unchanged.

Decimal Sort, TopSort, and Merge deliberately use a different comparison.
MiniKQL orders the raw signed 128-bit codes, giving the total non-null order
`-Inf < finite values < +Inf < NaN`; descending reverses it. NaN therefore ties
with NaN instead of making the ordering predicate false. Every order item keeps
one exact canonical `Decimal(p,s)` identity. Separate tuple keys may have
different Decimal identities, but one key is never scale-aligned as if it were
a scalar `DataCompare` expression. Explicit `nulls_first` remains orthogonal to
that non-null order at the selected pre-physical boundary.

Decimal arithmetic is deliberately canonical and narrow. The exporter accepts
only binary `+` and `-` with both operands and the result having one exact
canonical `Decimal(p,s)` type, or binary `DecimalMul` whose left operand and
result have that exact Decimal type and whose right operand is either the same
Decimal type or a signed/unsigned 8/16/32/64-bit integer. Result nullability
must be exactly the OR of operand nullability, and the subtree must still pass
the reviewed closed-world scalar checks. These callables normalize to explicit
`add`, `sub`, and `mul` snapshot nodes. An integer is accepted only on the right
of `DecimalMul`; YQL canonicalizes the supported reversed SQL spelling before
this boundary.

Evaluation matches `NDecimal` scaled-integer behavior and is strict on NULL.
Addition and subtraction preserve the common scale. Same-type Decimal
multiplication rescales the coefficient product by `10^s` with round-to-nearest,
ties-to-even for both signs; an integer right operand multiplies the coefficient
without rescaling and therefore preserves the Decimal scale. NaN propagation,
indeterminate opposite-infinity addition/same-infinity subtraction,
infinity-times-zero, signed infinities, and finite precision overflow are
explicit. Finite overflow saturates to the appropriate infinity before an
in-band NaN code can be mistaken for a calculated NaN.

The arithmetic kernel is checked against an independent rational reference on
every legal finite code and all specials for precisions up to two. Adversarial
cases cover both signs of ties-to-even, every integer width, infinity-times-zero,
precision overflow, and a finite product numerically colliding with the NaN
code. C++ exporter tests audit the admitted and rejected signatures. Separate
solver tests send unchanged `add`/`sub`/`mul` through the normal logical-to-
StageGraph obligation and require `VERIFIED_BOUNDED`; mutations between those
operations must produce concrete counterexamples.

Opaque identity is an inspectable `yql-opaque-v1` canonical string, not a hash.
It preserves exact callable/atom bytes, normalized atom flags, formatted types,
child order, constants, settings, and repeated arguments. Input IU names are
replaced by first-use ordinals and emitted separately as the ordered `args`, so
the same expression remains one UF across optimizer renames while swapped or
repeated values retain their meaning. Positions, allocations, and DAG sharing
do not affect identity. The representation fails closed above 256 expanded
nodes, depth 64, or 64 KiB.

The chosen final boundary is immediately before `ConvertToPhysical`. Therefore
the verifier does not prove physical lowering, task construction, or execution.
In particular, the current lowering does not visibly preserve
`TSortElement::NullsFirst`; explicit NULL ordering remains a replay case until
that contract is clarified.

Sort order is an exact, non-empty sequence of `(column, ascending,
nulls_first)` entries. Small inputs are represented by explicit permutations.
For larger inputs, the bounded evaluator gives each candidate row an integer
ordinal: present-row ordinals are in range and pairwise distinct, and strict key
comparisons imply the corresponding ordinal order. Tied keys remain free in
either order, so the symbolic encoding denotes the same complete sequence set
without factorial expansion. A non-null Sort `limit` is TopSort and applies an
exact prefix by compressed ordinal rank after sorting. Sort and Limit phases
(`undefined`, `intermediate`, and `final`) are preserved but are not otherwise
semantic.
Project nodes carry the exact `TOpMap::Ordered` Boolean. Both `ordered: true`
and `ordered: false` currently preserve an input sequence and compatible order
metadata. This matches RBO's streaming WideMap lowering; retaining the exact
flag makes a future semantic distinction an explicit verifier change.

Limit on an ordered input takes the exact `offset:offset+count` slice of the
compressed present-row sequence. If the initial root is ordered, equivalence is
sequence equality; an unordered initial root retains bag equality. The Merge
encoding is exact within the declared row/task/family bounds: it rejects an
unordered or differently ordered producer and represents all sorted,
producer-order-preserving interleavings. Symbolic Merge ordinals preserve the
relative input ordinals within each producer as well as the output sort order.

Ordinal variables are bounded by the fixed candidate-row vector. When result
languages are compared, one side's choices describe a candidate sequence and
the other side's choices are existentially quantified inside the membership
test; the reverse direction is checked as well. The SMT renderer shares repeated
DAG terms through hygienic, dependency-ordered `let` bindings separately inside
each quantifier scope, never hoisting an expression past a binder. These are
exact finite encodings and rendering transformations, not unbounded ordering
proofs or semantic approximations.

`String` and `Utf8` remain equality-only uninterpreted atoms. Date is an exact
bounded integer-day type with literals, equality, ordinary ordering, Sort, and
Merge. Decimal has exact literals, its legal typed domain, and the comparison
and arithmetic semantics above, plus exact raw-code Sort/TopSort/Merge ordering.
Decimal division, general casts, static `IN`, and aggregate functions other
than the bounded `sum` below remain unsupported. String and Utf8 sort keys
therefore return `UNSUPPORTED` during strict validation instead of inventing an
ordering.

The aggregate subset covers grouped and scalar `count`, integer `sum`, and
Decimal `sum`, including NULL grouping and inputs and optimizer-generated
intermediate/final phases. Signed inputs widen to `Int64`, unsigned inputs widen
to `Uint64`, and both integer sums use the runtime's exact 64-bit modular
overflow. `sum(Decimal(p,s))` widens every input, partial state, and result to
`Decimal(35,s)` and preserves YDB's NaN/infinity algebra.

Decimal `AggrAdd` saturates each intermediate result and is not associative when
finite overflow is possible. The verifier therefore carries a conservative
absolute finite-code bound through partial states and admits a Decimal sum only
when the total bound is strictly below `10^35`. Within that headroom every row
order and distributed parenthesization has the same exact result; otherwise the
query fails closed. A column-storage source is split into symbolic source tasks
before a pushed intermediate aggregate executes.
The optimizer's canonical `COUNT(*)` extractor is represented by the exact
zero-child, typed `Void` expression and evaluated as one non-null unit value;
it may pass through routing and relational operators, but every path must
terminate in a non-distinct, non-unwrapped `count`. Inspecting, dropping, or
exposing that unit fails closed. `Void` is not a catalog, literal, NULL, or
opaque-result type.
`distinct`, `DistinctAll`, `unwrap`, min/max, average, and variance currently
return `UNSUPPORTED` rather than use an approximation.
Intermediate aggregation models the pre-physical logical state per task and
key; memory-pressure batching performed later by a physical hash combiner is
outside the snapshot boundary.

An unordered `Limit` is not modeled as an arbitrary fixed vector prefix. For
each input outcome, the kernel enumerates every guarded-row mask whose size is
`min(count, max(input_size - offset, 0))`. Plans are equivalent only when their
sets of enabled output bags mutually include one another. Choice identities are
carried through the DAG, so two uses of one Limit node share a choice, while
stage-task instances choose independently. Count and offset are restricted to
non-null `Uint64` literals in v1; parameterized or otherwise computed limits
fail closed. Phase is preserved as `undefined`, `intermediate`, or `final`, but
does not itself change runtime semantics. Distinct Limit observers downstream
of one shared unordered plan stream fail closed until their latent-order
correlation is modeled. Ordered Limit is deterministic; Aggregate, Join, and
UnionAll start new unordered streams.

```json
{
  "id": "n2",
  "op": "limit",
  "input": "n1",
  "count": {"kind": "literal", "type": "Uint64", "value": 10},
  "offset": null,
  "phase": "final"
}
```

The current exporter emits `pushed_limit` on every scan, either `null` or the
same exact literal shape. Its absence in a legacy-v1 snapshot means `null`,
because the earlier v1 exporter rejected every pushed read limit. A non-null
pushed limit is valid only on a column-storage source and executes independently
on each source task after symbolic row partitioning.

The exporter also emits `predicate` on every scan, with legacy absence meaning
`null`. A non-null predicate is decoded from the executed `OlapFilterLambda`
rather than `OriginalPredicate` statistics metadata. Version one accepts only a
one-argument chain of `KqpOlapFilter` operations ending at that exact argument;
its scalar subset is physical columns, supported literals, Boolean
AND/OR/NOT, equality, lossless integer or same-type Date ordering, and filter-boundary
`Coalesce(predicate, false)`. Projection wrappers, range reads, malformed type
descriptors, and unknown operations fail closed. The predicate filters raw scan
rows before symbolic source partitioning and any per-task pushed limit.

Every explicit outcome family is capped at 256 alternatives, including
unordered-Limit choices, small enumerated sequence choices, Cartesian products,
and gathers. Sort, Merge, and latent sequences switch to bounded symbolic
ordinals before a large factorial expansion. Cross-plan bag or sequence equality
is capped at 4096 explicit outcome pairs. Exceeding either remaining audit bound
returns `UNSUPPORTED` rather than approximating.

Aggregate nodes preserve ordered keys and traits, output type/nullability, and
phase explicitly:

```json
{
  "id": "n1",
  "op": "aggregate",
  "input": "n0",
  "keys": ["t.k"],
  "aggregates": [{
    "input": "t.v",
    "function": "count",
    "output": "cnt",
    "type": "Uint64",
    "nullable": false,
    "distinct": false,
    "unwrap": false
  }],
  "phase": "intermediate",
  "distinct_all": false
}
```

## StageGraph v1 shape

The strict decoder rejects unknown fields and missing required fields (with the
documented legacy-v1 scan exception above). The abbreviated shape is:

```json
{
  "root_stage": "s2",
  "stages": [{
    "id": "s0",
    "nodes": ["n0"],
    "inputs": [],
    "outputs": [{"index": 0, "node": "n0"}],
    "source_storage": "row"
  }],
  "edges": [{
    "id": "e0",
    "producer": "s0",
    "consumer": "s2",
    "occurrence": 0,
    "producer_output": 0,
    "consumer_input": 0,
    "kind": "hash_shuffle",
    "keys": ["a.k"],
    "hash_function": "HashV1",
    "use_spilling": true
  }],
  "assumptions": []
}
```

Map and Broadcast add no variant fields. UnionAll adds `parallel`. Merge adds
`order`, whose elements contain `column`, `ascending`, and `nulls_first`.
`source_storage` is `row`, `column`, or `null`. Version one requires an empty
`assumptions` array.

## Development setup

The Python kernel has no package dependencies. Its CLI accepts an explicit
Z3-compatible executable when solving a formula. Tests run through `ya` instead
resolve the pinned Z3 4.16.0 target under `contrib/tools/z3`; no ambient solver
installation or `RBO_Z3` setting can affect those hermetic test runs.

```bash
python3 -m unittest discover -s ydb/core/kqp/opt/rbo/verification/ut
```

Run its tests with:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/ut 2>&1 | tail
```

The `ya` test target always builds and runs the solver integration tests. The
raw `unittest` command above still supports lightweight development outside the
`ya` environment: it skips solver cases unless `RBO_Z3` names an external
compatible executable.

The real-host integration tests construct a new-RBO `IKqpHost` and send each
captured initial/final pair through the normal CLI. One test covers an explicit
`LIMIT 1` and its split intermediate/final form. The ordered test covers
`ORDER BY A DESC, B ASC LIMIT 1`: it checks the initial Sort+Limit, final
per-task intermediate TopSort, exact Merge metadata (including NULL placement),
and final Limit. A column-store test compares an initial logical Filter with the
final pushed OLAP predicate and requires the normal bounded proof. The benchmark
test loads the exact `TPCDS_YQL` schema and q96 source used by the new-RBO suite,
checks its split `COUNT(*)`, pushed predicates, four-table join, and StageGraph,
and proves the two-row/two-task obligation with a query-specific 60-second
solver budget. Nullable `String IN ('first', 'second')` and
`Int64 IN (Int32...)` expressions exercise both static-membership type gates
through the real host and prove the normal obligation. A native Decimal
column filter likewise checks exact tagged literals and comparison predicates
at both real-host boundaries and proves its normal two-row/two-task obligation.
A Decimal arithmetic query checks `+`, `-`, same-type multiplication, integer-
right multiplication, and YQL's normalization of the reversed SQL spelling at
both boundaries, then proves the two-row/two-task obligation. All equivalent
real-host fixtures require `VERIFIED_BOUNDED` from the hermetic solver. A
Decimal aggregate query checks the `Decimal(7,2)` to `Decimal(35,2)` widening,
undefined/intermediate/final `sum` phases, and serial partial-state UnionAll,
then proves its two-row/two-task obligation. A
separate Decimal ordered query covers logical Sort+Limit and the transformed
per-task TopSort+Merge+final-Limit path. That proof ends before physical
lowering, consistent with the boundary caveat above.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/integration_ut 2>&1 | tail
```

Build the Ya-owned CLI with:

```bash
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/inspect_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/prefix_capture/bin
./ya make --build relwithdebinfo contrib/tools/z3
```

## CLI

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier before.json after.json \
  --rows 2 --timeout-ms 10000 --solver /path/to/z3
```

The command prints a JSON verdict. A `COUNTEREXAMPLE` verdict contains only the
present base-table rows; opaque-function and symbolic-routing interpretations
and the unmatched unordered-Limit choice are deliberately not treated as a
stable witness format, so concrete replay is the confirmation boundary. Every
bounded verdict reports both `row_bound` and the fixed `task_bound` of two. A
`SCHEMA_MISMATCH` verdict is a direct correctness failure and does not depend on
either bound. Use `--emit-smt formula.smt2` without `--solver` to inspect the
exact proof obligation. If satisfiability succeeds but model extraction returns
unknown, the result remains `COUNTEREXAMPLE` with a reason and no witness.

Normal verification requires a logical initial snapshot and a final snapshot
with a complete StageGraph. The separate localization path may compare that
same logical initial snapshot with a captured logical or complete staged
transformation prefix:

```bash
PYTHONPATH=ydb/core/kqp/opt/rbo/verification \
python3 -m rbo_verifier initial.json prefix.json \
  --diagnostic-transformation-prefix --rows 2 --solver /path/to/z3
```

This mode uses the same formula kernel but an explicitly different boundary
contract. Every result, including errors, carries
`"comparison_scope":"OPTIMIZER_TRANSFORMATION_PREFIX"`; it is evidence about one
optimizer prefix, not a whole start-to-finish verdict. A standalone sequential
localizer drives this mode because equivalence is not monotonic across rule
applications or atomic stage commits and therefore cannot be bisected soundly
with binary search.
`kqp_rbo_prefix_capture` supplies the real-host capture side of that protocol;
see [tools/README.md](tools/README.md) for its strict artifact contract and the
complete localizer invocation.

## Plan and counterexample inspection

The separate `kqp_rbo_inspect` executable renders every modeled snapshot field
as deterministic line-oriented text:

```bash
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect \
  plan final.snapshot.json
```

Its `witness` command rebuilds and solves the normal start-to-finish obligation,
then prints the candidate database and concrete result family at every logical
operator, stage task, connection input, and compared root boundary:

```bash
ydb/core/kqp/opt/rbo/verification/inspect_bin/kqp_rbo_inspect \
  witness initial.snapshot.json final.snapshot.json \
  --query exact-query.yql \
  --solver /path/to/z3 --rows 2 --timeout-ms 10000
```

The inspector observes immutable SMT terms during normal evaluation. Only after
the obligation is complete does it add definitional aliases for model
extraction. Base rows, routing-dependent rows, opaque scalar results,
nondeterministic outcomes, and the exact root mismatch are requested together
from one SAT model. All enabled outcomes are printed; absent rows omit their
meaningless payloads. Trace extraction fails closed above 100,000 unique terms.
Enabling the read-only observers without aliases is regression-tested to leave
the normal SMT-LIB obligation byte-for-byte unchanged. Every trace carries
SHA-256 digests of the complete normalized before/after snapshots; supplying
`--query` also binds the exact query bytes and is mandatory for real replay.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/inspect_ut 2>&1 | tail
```

## Real-YDB counterexample replay

`kqp_rbo_replay` consumes the inspector's version-one concrete trace and runs
the exact witness against two isolated YDB targets. The baseline target must
have the legacy optimizer enabled; the candidate must have the new RBO enabled
with fallback disabled. For parity with the benchmark host, use identical
settings except for `enable_new_rbo`:

```yaml
table_service_config:
  enable_new_rbo: true # false on the baseline target
  enable_fallback_to_yql_optimizer: false
  allow_olap_data_query: true
  default_lang_ver: 202602
  default_cost_based_optimization_level: 2
  backport_mode: All
query_service_config:
  script_result_rows_limit: 0
  script_result_size_limit: 0
```

The query file must contain the exact text used for snapshot capture. In
particular, TPC-DS replay includes the benchmark's `$to_decimal`,
`$to_decimal_max_precision`, and `$round` compatibility definitions.

```bash
ydb/core/kqp/opt/rbo/verification/replay_bin/kqp_rbo_replay \
  initial.snapshot.json final.snapshot.json concrete-trace.json query.yql \
  --ydb /path/to/ydb \
  --baseline-endpoint grpc://baseline-host:2136 \
  --baseline-database /Root/baseline \
  --candidate-endpoint grpc://candidate-host:2136 \
  --candidate-database /Root/candidate
```

Before connecting, the tool strictly validates both snapshots, the trace and
witness shape, primary keys, source integer/Date/Decimal ranges, table identity
encoding, storage inference, exact backtick-quoted source paths, result schema,
and observable determinism. Legal Decimal special codes are accepted and
rendered for BulkUpsert as `-inf`, `inf`, and `nan`; invalid or out-of-precision
finite codes still fail closed. The tool accepts exactly one top-level result
query; the current TPC-DS sources q14, q23, and q39 contain two result queries
and fail closed until replay gains an explicit multi-result contract. It returns
`INCONCLUSIVE_NONDETERMINISM` when the bounded model admits more than one
distinct result for either side.

Each target receives a fresh `_rbo_replay_<128-bit-id>` namespace containing
reduced two-partition tables. Rows are loaded with the CLI BulkUpsert import
path, not an SQL write that would exercise the optimizer under test. Explain is
then run through QueryService. The current new-RBO statistics shape is required
on the candidate, the legacy/absent shape on the baseline, and every candidate
CBO tree must be optimized; this also detects transparent fallback to the old
optimizer. Results use `json-base64-array`, preserve binary strings and
duplicates, and are compared as a sequence or bag according to the initial
snapshot contract.

`REAL_RESULT_DIVERGENCE` means the two real executions differed, while
`NOT_REPRODUCED` means this concrete realization did not differ; neither result
claims unbounded equivalence. The first is strong evidence of an optimizer
correctness problem, but it is not attributed to the supplied symbolic trace
without reproducing that exact final StageGraph. The reduced catalog does not
reproduce indexes, statistics, or every physical table setting, and the
external CLI cannot recapture the final semantic snapshot, so output explicitly
records `trace_plan_reproduced: false`. Column-store sources use two hash
partitions. Row-store synthesis currently fails closed unless the leading
primary-key column is `Uint32` or `Uint64`, the types for which YDB supports
auditable two-way `UNIFORM_PARTITIONS` creation.

Replay namespaces are deliberately retained for diagnosis and are printed in
the JSON result. The tool never deletes an existing or generated YDB object.
Its audit boundary is split by responsibility: `case.py` validates the
certificate and witness, `materialize.py` renders the isolated catalog and
read-only query, `observation.py` decodes real results, and `runner.py` contains
the external mutation boundary.

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/replay_ut 2>&1 | tail
```
