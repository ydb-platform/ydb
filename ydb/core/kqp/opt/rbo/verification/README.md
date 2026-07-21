# New RBO equivalence verifier

This directory contains the standalone bounded-equivalence checker described in
[PLAN.md](PLAN.md). It compares two versioned semantic snapshots and asks Z3 for
a bounded input database on which their result bags or ordered sequences differ.
The reproducible 121-query dashboard contract and current unsupported inventory
are recorded in [BENCHMARK_COVERAGE.md](BENCHMARK_COVERAGE.md).

The current implementation contains the M1 logical kernel, the M2 C++ boundary
hooks, the supported M3 StageGraph routing slice, and the aggregate, Limit,
ordered Sort/TopSort/Merge, pushed OLAP-filter, restricted static `IN`, and
benchmark-dashboard parts of M4. Separate normalized-plan,
concrete-counterexample inspection, and isolated real-YDB replay tools are also
implemented. A real-host transformation-prefix capture
command and sequential localizer are implemented outside the verifier kernel.
Committed rule applications and mutating non-rule stages share one explicit
transformation-event stream. Hermetic solver packaging remains a future milestone.
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
enumerates the sorted interleavings that preserve every producer sequence. The
final stage is gathered before root column projection.

The exporter and decoder both validate the StageGraph independently: plan nodes
partition into stages, each stage has one logical sink, every cross-stage child
occurrence has one edge, and producer output indices are a bijection with
outgoing edge occurrences. Shuffle-elimination/co-partitioning assumptions are
rejected until the snapshot can substantiate them.

Version one preserves exact supported YQL scalar identities (`Bool`, signed and
unsigned integer widths, `String`, `Utf8`, `Date`, and canonical parameterized
`Decimal(p,s)`) even when several identities use the same SMT domain. Decimal
identity requires `1 <= p <= 35` and `0 <= s <= p`, with no alternate spelling.
Integer and Decimal slots currently use unbounded SMT carriers rather than
source-domain range constraints. This may produce an out-of-range or
symbolic-atom `COUNTEREXAMPLE`, which replay can reject, but cannot turn a real
bounded counterexample into `VERIFIED_BOUNDED`. Date is exact: numeric literals,
source cells, and non-null opaque results are constrained to unsigned day values
in `[0, 49673)`.

Same-type integer `+`, `-`, and `*` are structural scalar nodes. Both operands
and the result must have exactly the same signed or unsigned width, and result
nullability must be the OR of operand nullability; otherwise the expression
remains opaque. Evaluation is strict on NULL and wraps modulo the result width,
using the signed two's-complement representative for `Int8` through `Int64`.
This narrow rule was added when TPC-DS q88 exposed a spurious witness between
source arithmetic constants and optimizer-folded literals.

Static `SqlIn` is exact for a deliberately narrow shape: a direct raw tuple or
`AsList` with 1..512 recursively supported, non-null items whose scalar type is
identical to the lookup type. It evaluates as the SQL three-valued OR of
ordinary equalities, so only a nullable lookup can make the Boolean result
nullable. `ansi`, `warnNoAnsi`, `isCompact`, and `nullsProcessed` normalize to
the same node under this gate. Dynamic, empty, oversized, nullable, mixed-type,
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

The explicit comparison core accepts unequal integer widths only when YQL's
common integer type represents both operands without wrapping: equal signedness,
or a signed type wider than the unsigned type. This covers the canonical
`Int64`-column/`Int32`-literal benchmark form while mixed-width cases that would
bitcast or wrap still fail closed. Date comparison requires Date on both sides.

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
nulls_first)` entries. The bounded evaluator enumerates every permutation of the
input row slots and enables exactly the lexicographically sorted ones, retaining
all legal tie orders. A non-null Sort `limit` is TopSort and applies an exact
ordered prefix after sorting. Sort and Limit phases (`undefined`,
`intermediate`, and `final`) are preserved but are not otherwise semantic.
Project nodes carry the exact `TOpMap::Ordered` Boolean. Both `ordered: true`
and `ordered: false` currently preserve an input sequence and compatible order
metadata. This matches RBO's streaming WideMap lowering; retaining the exact
flag makes a future semantic distinction an explicit verifier change.

Limit on an ordered input takes the exact `offset:offset+count` slice of the
compressed present-row sequence. If the initial root is ordered, equivalence is
sequence equality; an unordered initial root retains bag equality. The Merge
encoding is exact within the declared row/task/family bounds: it rejects an
unordered or differently ordered producer and enumerates all sorted,
producer-order-preserving interleavings.

`String`, `Utf8`, and Decimal remain equality-only uninterpreted atoms. Date is
an exact bounded integer-day type with literals, equality, ordinary ordering,
Sort, and Merge. String, Utf8, and Decimal still have no modeled YDB ordering,
so using one in Sort or Merge order metadata returns `UNSUPPORTED` during
strict validation instead of inventing an ordering.

The aggregate subset covers grouped and scalar `count` and integer `sum`, NULL
grouping and inputs, and optimizer-generated intermediate/final phases. Signed
inputs widen to `Int64`, unsigned inputs widen to `Uint64`, and both sums use
the runtime's exact 64-bit modular overflow. A column-storage source is split
into symbolic source tasks before a pushed intermediate aggregate executes.
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
unordered-Limit choices, Sort permutations, Merge interleavings, latent
sequence expansion, Cartesian products, and gathers. Cross-plan bag or sequence
equality is capped at 4096 outcome pairs. Exceeding either audit bound returns
`UNSUPPORTED` rather than approximating.

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

The Python code has no package dependencies. Pass an explicit Z3-compatible
solver executable when asking it to solve a formula. During local development,
that can be a system or isolated-development Z3 installation; hermetic `ya`
integration will use a separately vendored binary.

```bash
python3 -m unittest discover -s ydb/core/kqp/opt/rbo/verification/ut
```

Run its tests with:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/ut 2>&1 | tail
```

Solver integration tests are enabled when an explicit solver binary is
available. Formula construction and parsing tests do not depend on Z3.

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
solver budget. A nullable-String `IN ('first', 'second')` test exercises the
restricted static-membership path through the real host and constructs the
normal obligation. All tests always require strict decoding and SMT construction. Set
`RBO_Z3` to additionally require `VERIFIED_BOUNDED`; M2b will replace that
opt-in path with a hermetic solver dependency.

```bash
RBO_Z3=/path/to/z3 ./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/verification/integration_ut 2>&1 | tail
```

Build the Ya-owned CLI with:

```bash
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/inspect_bin
./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/verification/prefix_capture/bin
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
and observable determinism. It accepts exactly one top-level result query; the
current TPC-DS sources q14, q23, and q39 contain two result queries and fail
closed until replay gains an explicit multi-result contract. It returns
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
