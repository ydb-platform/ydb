# Native SQL frontend and RBO expressions

New RBO has one scalar representation: typed native expressions in a query-owned
arena. SQL translation, YQL scalar graphs, `CompileExpr`, and YQL graph type
annotation do not participate in native binding or logical optimization. The
existing catalog/session services, physical compiler, and runtime remain in use.

```text
SQL
  -> fast parser / independently generated shared-grammar parser
  -> compact syntax + asynchronous catalog resolution
  -> native name binding, types, scalar DAG and relations
  -> checked TCompiledProgram
  -> native execution preparation: shared producers and ordered effects
  -> owning TNativeQuery: RBO roots, ordered outputs, parameters, execution metadata
  -> existing RBO rules, CBO and stage assignment using native expressions
  -> explicit physical emission: backend types, lambdas, tables and runtime operators
  -> existing physical compiler and execution
```

## Boundaries and ownership

`ParseNative`, `DiscoverNative`, and `BindNative` separate syntax from asynchronous
catalog I/O. Discovery owns the syntax and binding settings, resolves global
references and pragmas once, and identifies the required tables. Binding reuses
that immutable resolution after metadata arrives; catalog schemas and local
column bindings remain fresh for each compilation. `CompileNative` binds directly
for callers with a synchronous catalog, without a table-discovery pass.
`TCatalogSnapshot` owns the decoded schema and its lexical column-name index.
The host creates one snapshot for each loaded table version; binding and RBO
share it. Catalog order still determines visible columns, while the lexical
index determines source IDs and supports native type lookup. Translation does
not copy schemas or build another column-type map. New compilations resolve
fresh catalog versions through the existing metadata service.
`TTableAddress` keeps cluster and path separate through discovery, catalog lookup,
binding and effect ordering. Central source resolution applies path pragmas and
configured cluster prefixes once; explicitly qualified tables bypass CTE lookup.
The host validates the complete table set against its execution cluster before
metadata I/O, and the RBO handoff checks that boundary before creating operators.
The fast lexer borrows unchanged text during parsing and owns decoded escape
values until parsing ends. The returned syntax owns all retained strings and
remains valid after the SQL input and parser are released.
Global references resolve to declaration IDs, including aliases of named queries;
source and IN uses share that resolution. Table discovery follows those bindings
before catalog I/O. Scalar typing and local column resolution consume the owning
syntax, never revisit the CST or reparse individual pragmas.

The syntax contract is broader than execution admission. Source-local `ANY`,
sampling, generic/schema/watermark hints, SQL `EXPLAIN`, and window modifiers
retain their structured values even where execution is unsupported. `ANY` inputs
bind and type normally, retaining separate left/right flags on their join. The
RBO handoff rejects these flags before constructing operators; native duplicate
selection is not implemented. Sampling and unsupported hints, explain modes and
window modifiers are rejected at binding before catalog I/O. Named-window references
remain references in the AST and resolve within their query at binding; `OVER w`
and `OVER (w)` retain distinct syntax. Type syntax is decoded from the CST in one
place for schema hints, declarations and casts.
Repeated `DECLARE` statements retain the first parameter type; the anonymous
`$_` spelling cannot declare a parameter. SDK-supplied types must match that first declaration.
Pragma arguments are decoded once into typed syntax, retaining flag, assignment
and list forms. Numbers, Booleans, names and bindings remain distinct from
strings. The central pragma handler validates supported value kinds, warning
actions and issue-code patterns; dynamic values remain explicitly unsupported.
Since native compilation emits
errors and currently has no warning stream, disabling warnings is supported;
restoring or promoting warnings remains explicitly unsupported.
Only checked compilation can construct `TCompiledProgram`; conversion consumes it
once. A raw binder result cannot enter RBO.

Embedding callers may provide immutable `TExternalBindings` with native scalar
values and callable bodies. `TScalarBinding` validates each body's explicit
formal inputs once. Binding resolves external variables in the ordinary lexical
scope, imports values on first use, and substitutes callable arguments through
the native DAG importer with local type checking. Imported values and types
belong to the resulting query; the caller's arena need not survive compilation.
Unused values are not imported, and ordinary SQL has no external binding map.
This interface accepts no backend nodes or unresolved runtime parameters.

Named struct values use the same native arena as tuples and lists. Their
interned shapes retain canonical field names and types through imports,
rewrites and folding. Both parsers preserve named constructor operands and
identifier separators, so binding never splits quoted field text. Scope
resolution identifies the base column before member typing and correlation
capture; operator translation receives ordinary typed scalar expressions.
Selecting a field from a constructed struct returns that field's expression,
without evaluating discarded fields. Runtime records retain typed member
access, including optional records and nullable fields. Backend struct types,
`AsStruct` and `Member` nodes are created only at terminal emission.
Shapes also cache ordering capability. Scalar-subquery construction keeps the
existing `min`/`some` choice using that native property, including records and
collections containing records, without consulting backend type annotations.

`CompileRowExpression` parses catalog scalar SQL through the same expression
grammar and binder against an explicit row schema. It checks and folds native
expressions without query bindings or table access. Generated write columns
share these immutable expressions within a compilation; catalog expression text
is not compiled through the legacy SQL or YQL frontend.

Filter binding uses `PrepareFilterPredicate` to expose Boolean structure where
only TRUE versus a rejected row matters. Scalar result roots retain their NULL
semantics. Preparation preserves evaluation order: for example, the left operand
of AND keeps its FALSE-versus-NULL distinction because that controls evaluation
of the right operand. Nullable membership bounds become visible to the existing
range rules without a YQL scalar rewrite or a change to range-selection policy.
Legacy IN keeps lookup-null propagation as one native `InResult` operation,
shared by fixed collections and subqueries. It evaluates the lookup before its
match and preserves the distinction between a NULL lookup and NULL collection
items. The range adapter exposes a direct scalar-column membership only in a
positive predicate context; scalar results and membership under NOT retain the
original operation. General IF expressions retain their control flow. Only
`IF(condition, true, false/NULL)` reduces to its condition at a filter boundary.
Filters require `Bool` or `Bool?`; a present optional wrapper around a required
Boolean can be removed here. Constant folding tracks conservative totality in
its existing traversal, so Boolean identities and known null checks can simplify
without dropping checked errors or runtime calls.
That traversal also shares equivalent repeatable scalar nodes before relational
rules inspect the DAG, replacing the scalar identity previously supplied by YQL
normalization. Identity includes native types, exact literal bits, bindings and
operand order. Original roots remain immutable; volatile calls and runtime
assertions retain separate identities. JSON/path evaluation stays in the runtime.
`BETWEEN` binds to native comparisons with SQL three-valued logic. Its symmetric
form checks both bound orders; its negated form uses the opposite comparisons,
preserving the existing SQL behavior for NaN. `IN COMPACT` uses native membership
evaluation, with the same NULL semantics as `IN`.

`PrepareExecution` owns statement dependencies and shared producers. A single
result without named producers needs no scheduling graph. Other programs retain
source-order read/write dependencies and materialize shared producers at first
use; reads after writes cannot be moved ahead of those writes.
Shared index-view producers also use an execution input: their storage expansion
may contain a lookup that the existing RBO cannot duplicate, and data queries
cannot assume channel spilling. This conservatively includes covering views;
single-use views need no materialization.

`ConvertNativeToRbo` constructs ordinary RBO operators directly. Its result,
`TNativeQuery`, owns the roots, native parameter schemas, ordered output names,
and `TQueryExecutionMetadata`. Native queries do not build synthetic
`KiDataQueryBlock` graphs. The legacy compiler imports the same execution metadata
from its own query blocks.
Column and relation IDs index dense translation tables; construction does not
allocate hash entries for those IDs.
Projected IN/EXISTS values are constructed at this handoff using the same mark-join
builder as the existing filter rule. The rule retains its filter-only matching
policy. Native OLAP capability checks live separately from physical emission.
Computed ON equalities become input maps and join keys during construction.
That preparation is private to the frontend handoff; the existing expression
extraction rule still matches only filters over joins.
Regular-expression storage admission retains the existing scalar-apply and
`OptEnableOlapPushdownRegexp` gates. Native capability checks and the admission
adapter are compared with the established collector independently of emission.

Explicit index views retain the main-table identity through binding and execution
preparation. The catalog admits ready synchronous row indexes on demand.
`PrepareNativeIndexReads` uses existing local column-demand analysis to construct
an index read, plus a nullable-primary-key lookup only when columns are missing.
A whole adjacent filter covered by the index is constructed before that lookup;
range extraction remains the existing RBO rule. No predicates are split here.
Authorization and write ordering refer to the main table. Ordinary queries do
not run this preparation. Async/specialized indexes and `VIEW PRIMARY KEY` remain
unsupported.

`TExpression` holds an arena and a node ID. There is no YQL expression constructor,
legacy lambda arm, implicit export, or mutable backend annotation. Rewrites append
native nodes, share existing subgraphs, and use explicit column-ID substitutions.
Scalar factories take native types and plan properties, with no backend expression
context argument.
Copying a plan rebinds expressions, captures, read predicates and OLAP projections.

`TNativeSchema` describes rows and aggregate states. `ComputeNativeTypes` checks
operators and rebinds scalars using native types. AVG/statistical states use
arena-owned tuple shapes; nested optional types preserve each optional layer.
`TRboTable` holds catalog identity and the immutable native catalog snapshot. Read ranges,
lookup point prefixes, OLAP predicates, sink settings and stream lookup key types remain native
through stage assignment.
Range bounds may be literals, parameters or repeatable scalar expressions without
row dependencies. Range and lookup-point validity conditions are native required
Booleans. They reject NULL comparison values and unordered NaN bounds; nullable
values are unwrapped only in the valid branch. Lookup points are deduplicated at
runtime, including equal values from different computed expressions. Open upper bounds for Float,
Double and Decimal stop before the NaN storage key. Physical emission constructs
typed conditional lists directly. It does not use YQL range expansion helpers.
Copying and expression visitors preserve these conditions with their bounds.
Range construction deduplicates intervals selected by identical consumed source
terms while retaining the full residual predicate. Lookup-prefix metadata follows
the existing configuration: all eligible indexes contribute prefixes when enabled;
otherwise only the selected read does.
`kqp_native_range_bounds` adapts scalar bounds to the storage key's exact type.
Total integer conversions use domain containment; narrowing conversions use a
safe cast and guarded endpoints, including strict comparisons at integer limits.
Lossless integer-to-floating equality uses an exact round trip. Promotions that
merge distinct keys retain scalar filtering: a single converted lookup point
would lose matching rows near Float/Double precision limits. This is an explicit
correctness exception to pre-migration range admission, covered by predicate and
filter execution comparisons. The rule still owns key-prefix and index selection.

Physical builders cross the boundary through `ExportNativeType`,
`ExportOperatorType`, `ExportIUType`, `LowerTable`, and `LowerExpressionLambda`.
`LowerExecutionType` supplies fully typed backend type syntax for runtime values.
Each export produces fresh objects in the backend context. Scalar lowering emits
fully typed terminal operations; direct MiniKQL tests require neither annotation
nor a semantic rewrite pass. Physical relational builders annotate the emitted graph and apply a bounded,
owned lowering pass for execution callables. They do not invoke YQL peepholes.
Tuple aggregate results use their native result schema instead of assuming that
every tuple is an AVG accumulator.
Collection type import and export preserve shared type DAGs. Import uses an
iterative traversal and visits each shared node once; ordinary scalar and
optional-scalar imports need no traversal map.

Physical stage preparation constructs wide channel adapters, then removes
redundant row/flow/block adapters in one DAG traversal. Field projections retain
shared producers; literals and optional wrappers have explicit block emission.
Sort/top kernel selection follows the existing block mode and Arrow type support.
AUTO block channels propagate in stage order, with a consistent layout for all
consumers and the existing restriction on multiple HashV1 shuffle inputs. Only
changed stages require annotation again. The final execution lowering expands
macros such as scalar broadcasting into executable calls. The historical
`EnableNewRBOPhysicalStagePeephole` flag controls this owned layout preparation;
neither setting invokes a YQL peephole pipeline.

The migration preserves the existing rule schedule, join enumeration, cost
formulas, stage assignment, shuffle policy and runtime algorithms. Expression
construction and inspection use the native arena. Computed ON keys are exposed
as ordinary maps and join keys before outer-join NULL padding. Native selectivity
retains the existing estimator's policies, including its capped OR heuristic and
fallback estimates, rather than changing cost decisions as part of migration.

## Admission and fallback

`EnableNewRBO` selects the native frontend. `NativeFrontendMode` additionally
controls admission:

| Mode | Behavior |
| --- | --- |
| `NATIVE_LEGACY` (default) | Legacy entry when new RBO is disabled; native automatic admission when enabled. |
| `NATIVE_AUTO` | Opt into native admission, including when `EnableNewRBO` is initially disabled. |
| `NATIVE_REQUIRED` | Compile natively or report an error. |

Automatic admission delegates **the whole query to the legacy optimizer** only
for a structured `TUnsupportedFeature`. It reports `[native_frontend_fallback]`,
`compiler=legacy-yql`, feature, position and reason. Invalid supported syntax,
type errors, resource limits, internal errors and RBO failures do not silently
retry. The old `EnableFallbackToYqlOptimizer` flag does not enable error retries;
use `NATIVE_REQUIRED` to forbid unsupported-query fallback. There is no
YQL-to-new-RBO import pipeline.

Native data, scan and generic query preparation bypass the YQL AST cache. Internal
ANALYZE requests and internal metadata maintenance retain their legacy entry. Native explain ASTs contain
`native frontend: native-read-v1`, parser selection and the native relational
plan. The CLI reports admission decisions but cannot execute the legacy compiler.

## Parsing

The default `fast` route uses a handwritten subset and invokes independently
generated ANTLR C++ parsing for unhandled syntax or syntax errors. The generated
parser shares `SQLv1Antlr4.g.in`, not YQL parser wrappers, protobuf trees,
translation or type libraries. Error-recovered trees are never compiled.

`--parser grammar` forces generated parsing; `--parser handwritten` disables
parser dispatch. `--parser verify` compares complete syntax fingerprints, including
precedence, clauses and types. It is a development check and may reject syntax
outside the handwritten subset. Binding and resource-limit errors do not retry
parsing. Correspondence tests cover the 164-query corpus, keyword contexts,
operator combinations, malformed mutations and lexer modes.

Backticks quote identifiers. Double quotes delimit strings by default and
identifiers in ANSI lexer mode, which also changes escaping and nested comments. Leading syntax/parser/ANSI
directives are recognized; directive text inside strings has no effect.
Both parsers share byte, octal and Unicode escape decoding for quoted strings,
identifiers and pragma values. ANSI strings retain backslashes literally;
multiline literals decode doubled `@@` delimiters without interpreting escapes.

## Execution profile

`frontend.cpp` checks `native-read-v1`; `--support` returns the machine-readable
manifest. Grammar recognition is broader than execution support. Arena support
alone does not establish SQL compatibility.

Supported relational forms include SELECT/FROM-first SELECT, ordinary tables and supported system views,
projections and unique-name stars, CTEs/query bindings, simple scalar lambdas,
ON/USING joins, grouping/HAVING, GROUPING SETS/ROLLUP/CUBE, windows, DISTINCT,
UNION/INTERSECT/EXCEPT including ALL, sorting/limits and multiple results. Named queries also serve as
single-column IN inputs in ordinary query expressions.
Scalar/EXISTS/single-column IN subqueries support correlation within the checked
backend profile. Scalar subqueries preserve NULL for no row and fail for more
than one row. Scan queries require one result.
USING keys bind on both inputs and become native equality predicates. Their merged
unqualified values use ordinary coalesce maps; qualified keys and qualified stars
retain each input's identity and outer-join NULL padding. Semi/only joins expose
only the retained input. No new optimizer rule is involved.

INSERT/UPSERT/REPLACE accept VALUES and SELECT inputs. UPDATE and DELETE accept
predicates or explicit input rows; RETURNING retains its projection order.
Effects use the existing table sink and transaction boundaries. Omitted literal
and sequence defaults are effect metadata, expanded once through the existing
sequencer connection during emission. UPSERT retains stored values when a column
was omitted; explicit values, including NULL, take precedence over defaults.
Stored generated columns are evaluated after sequence and literal defaults,
using the same prepared row for the sink and RETURNING. INSERT/REPLACE supply
omitted nullable dependencies as NULL. UPSERT/UPDATE can generate values when all
changed dependencies are available in the input; cases requiring existing stored
dependencies remain explicitly unsupported. Explicit GENERATED ALWAYS values,
self/cross-generated dependencies and non-deterministic expressions are rejected.
INSERT OR REVERT shares its input across primary/unique-key duplicate checks,
storage lookups and the write. A scalar conflict count gates the whole statement;
the ordinary insert sink retains write-time constraint checks. Its conflict
preparation constructs existing operators before optimization and adds no RBO rule.
Defaults and generated columns in INSERT OR REVERT remain unsupported until
their prepared values can be shared before these checks.
Imported effects tests cover mixed reads/writes, indexes,
shared producers and interactive transactions, alongside populated runtime checks.

Named sources outside correlated contexts retain one producer with distinct
bindings at each use. Existing RBO replication, copying and spilling rules decide
how to execute it. OLAP predicates stay native through optimization; physical
emission uses storage operations or the existing optional scalar-apply kernel.
Storage admission preserves source order around checked expressions, so predicates
cannot move across a checked term and suppress its errors.

Aggregates include COUNT/COUNT_IF, SUM, AVG, MIN/MAX, MIN_BY/MAX_BY, SOME and sample standard deviation.
DISTINCT groups original values before accumulator conversion. Native partial
states preserve Decimal AVG and nullable interval SUM. Window support includes
inline and named WINDOW definitions, aggregates/ranking and ROWS/RANGE frames with
checked literal offsets. Inherited windows are excluded.

Scalar types include Boolean, signed/unsigned integers, Float/Double, String/Utf8,
Date/Datetime/Timestamp/Interval, Decimal, Json/JsonDocument and NULL. Explicit
nested optional casts, Just and Unwrap preserve outer absence versus inner NULL;
general arithmetic and comparison on nested optionals are not admitted.
Bitwise operations require unsigned integers and promote to the widest operand.
Shifts consume a required Uint8 count, reinterpret signed inputs as unsigned,
and return zero when the count reaches the input width. Null-safe equality has
a required Boolean result; matching scalar types equate NaNs, while mixed types
retain ordinary comparison semantics. These rules are native construction and
execution contracts, with direct runtime and legacy SQL parity checks.
Extended Date32/Datetime64/Timestamp64/Interval64 and Uuid are native types.
`RandomUuid(dependencies)` and `RandomNumber(dependencies)` retain their runtime
dependencies and cannot fold. `YQL::RandomUuid()` and `YQL::RandomNumber()` are
accepted as SQL spellings for query-scoped native inputs; each emits one internal
parameter and exposes no user parameter.
String `s`/`u` suffixes override `UnicodeLiterals` / `DisableUnicodeLiterals`.
`ClassicDivision='false'` converts the left operand of division to Double;
`CheckedOps='true'` returns NULL for integer overflow and invalid common-type
conversions. Expression options, including IN and window defaults, are captured
at lexical definitions, so later pragmas do not change deferred queries/lambdas.
Checked arithmetic is constructed from native casts and Decimal(20,0)
operations; emission needs no checked-operation macro or YQL peephole.
`BITCAST` wraps Bool, integer and temporal payloads into required integral target
types while preserving input optionality. Native tuple casts support required
tuples with total field conversions. Nested tuples and lists can cross result
boundaries and carry aggregate payloads; shared collection shapes are interned.
Integer promotion, checked casts, decimal
rounding/exponents, Unicode case conversion, selected calendar operations,
JSON_VALUE, LIKE/ILIKE patterns, underscores and escapes have runtime differential
tests. Complex LIKE patterns remain atomic native predicates through RBO so
selectivity and storage admission retain the existing regex policy. Their
runtime matcher is constructed only during emission. Static direct, inline
`Udf(String::...)`, and bound callable forms resolve
to the same checked native string functions. Arbitrary UDF resolution is excluded.
`Re2::Match(pattern)` and `Re2::Grep(pattern[, NULL])` bind configured native
matchers, including aliases and lexical captures. Their Boolean operations
retain input and pattern dependencies through copying and folding. NULL inputs
return false; patterns compile only in the runtime after emission. Non-default
RE2 option structs remain unsupported.

Scalar lambdas support local bindings, higher-order arguments and returned
closures. Name resolution assigns declaration identities before binding;
query-owned binding frames preserve captured values and lexical shadowing.
Calls expand into native scalar expressions during binding, with bounded depth
and node counts. Closures and binding frames never cross the RBO handoff.

`AS_TABLE` accepts declared or supplied `List<Struct<scalar fields>>` parameters,
including nullable fields and empty structs. Source field names survive aliases,
pruning and copying. Separately, scalar List parameters and fixed tuple/list
constructors support IN membership. Dynamic list membership uses checked key
conversion and explicit ANSI/legacy NULL semantics; it emits typed dictionary
operations rather than a `SqlIn` macro. Declared row-source fields currently use
flat scalar types; native struct expressions can be projected as values.
Integral `ListFromRange`, flat scalar list casts, `ListLength`, `ListHasItems` and String/Utf8 `ListConcat`
are native scalar operations. List casts use the scalar conversion rules and
discard failed required elements, while optional items retain NULL. Optional
containers preserve an absent input. Constant folding bounds list materialization before
allocation; runtime ranges remain lazy. Emission constructs typed runtime
operators without the YQL annotator or scalar peepholes.
Scalar `MinOf`/`MaxOf` and `Least`/`Greatest` use native common types, propagate
NULL and retain the scalar runtime's NaN handling. Extended temporal operands
are outside this scalar min/max profile. `Starts_With` and `Ends_With` share
the ordinary native string predicate operations.
Fixed tuple comparisons expand into native scalar comparisons with SQL NULL
semantics and lexicographic ordering.

Aggregate `FILTER (WHERE predicate)` binds in the aggregate's input scope and
becomes a native conditional nullable input before materialization. The same
operation serves ordinary and window aggregates; the optimizer still receives
the existing aggregate operators. False or NULL predicates skip the argument,
including throwing expressions. Filter identity participates in aggregate and
window input sharing. `Math::Abs` converts its argument to Double and preserves
NULL, matching the runtime function's scalar signature.

Set operands align by name and infer common types across the chain. Missing
fields become NULL. Set ALL operations preserve duplicate counts. Output order
follows the existing common-prefix contract; comparisons with the legacy
frontend account for its different star-column order by name.

Important exclusions remain explicit:

- DDL, USE, external/temporary tables, async/specialized index views,
  table hints/sampling and unrecognized settings.
- PostgreSQL values and conversions, including `FromPg` and `ToPg`, are
  intentionally outside this pipeline's scope.
- Batch writes, virtual generated columns, generated writes requiring stored
  dependency lookup, and INSERT OR REVERT with defaults or generated columns;
  dynamic Int64 LIMIT/OFFSET and general row-valued scalar expressions.
- Named USING keys, tuple IN subqueries, duplicate non-key star names,
  and FULL JOIN residual predicates other than column equality keys.
- Subqueries in ON, GROUP BY, direct ORDER BY, LIMIT/OFFSET, scalar bindings or
  lambda bodies; ungrouped outer references after aggregation.
- Correlated windows and correlated INTERSECT ALL/EXCEPT ALL; inherited and
  DISTINCT windows, NULL treatment and unsupported frame forms.
- Arbitrary UDFs, volatile functions other than the UUID forms above, callable
  result columns and general containers.
  Dynamic collection dictionary keys have a narrower checked scalar profile.
- Bare-NULL arithmetic, unsupported mixed Decimal operations, floating Decimal
  casts, Float-to-32/64-bit-integer casts and unverified temporal conversions.
- Unsupported JSON paths or handlers. Calendar intermediates cannot cross
  relational boundaries.

Known backend limitations are admission checks, not a second scalar interpreter.
Unsupported queries retain the legacy optimizer's behavior through whole-query
fallback. See the manifest and negative tests for precise restrictions.

## Validation and measurement

Validation is split by boundary:

- Standalone parser, binder, type, expression evaluator and corpus tests.
- Native RBO invariant tests for column IDs, sharing/captures, copying, schemas,
  and unchanged expression/type allocation counters during logical optimization.
- Direct MiniKQL scalar execution, including dynamic parameters, NULL, overflow,
  Decimal and collection semantics, without scalar annotation or compatibility
  rewriting.
- Host tests for complete compilation, populated results, ordered names/types,
  fallback, prepared parameters, caches, row/column storage and physical options.
- Original SQL regression tests admitted in native-required mode, retaining their
  SQL and result expectations.

The selectivity oracle compares 7,832 predicate roots under three statistics
configurations (23,496 comparisons). A separate 20,000-node shared Boolean DAG
checks bounded traversal without expanding the graph into a tree.

All 22 TPC-H and 99 TPC-DS benchmark queries have native-required full-compilation
checks with fallback disabled and the existing CBO invariants. Execution fixtures
cover all 121 queries. All 22 TPC-H and 54 TPC-DS fixtures have populated result
checks; the remaining 45 TPC-DS queries execute and compare every result, but
some produce empty or NULL-only results and need stronger data before claiming
populated coverage. Q4 retains the original contradictory predicates.

`KqpNativeHost::NativeCompilationBenchmark` measures complete compiler CPU and
wall time on eight queries with AST/compilation caches disabled, one warm-up and
rotating order. Current modes are legacy, native automatic and native required.
Historical YQL+new-RBO comparisons require the preserved pre-removal binary;
current admission modes are not separate compiler architectures. Request wall
time also includes execution on empty tables and is reported separately.
Standalone CLI `--bench N` includes parsing, binding, native checks and destruction;
it excludes asynchronous catalog I/O, RBO and backend compilation.
`--bench-explain N` measures native plan formatting and destruction separately,
after an untimed compile and formatting warmup.
`--bench-discovery N` compares the owning discovery handoff with independent
table discovery and binding in the same binary. It checks native plan equality,
then alternates both paths each iteration, including parsing and destruction.
This isolates discovery reuse from binary layout and host scheduling effects;
whole-compiler timing still requires the host benchmarks.

The `710eddcc4fa` compilation checkpoint covers 6,992 queries with native-required
admission and fallback disabled: 6,813 1C, 22 TPC-H, 99 TPC-DS, 43 ClickBench and
two additional 1C samples, plus the 13 original TPC-H performance-fixture queries
with their preamble and Double/String column-store schema. That fixture also
passes separately through the original Table API. This establishes compilation coverage, not complete
effects parity or negligible frontend cost. The `e1fc80d8039` validation included
257 passing standalone tests. JOIN USING additionally has runtime value/type comparisons
against explicit legacy ON/coalesce forms in scalar/block and nullable/required
configurations, plus a handoff check with zero YQL expression/type allocations.
The imported indexed-delete test had expectations explicitly marked as a legacy
rescan bug; its native port instead checks one shared snapshot and the exact
surviving rows and index contents in both index modes.

The full warmed comparison of native revision `9c5cb3fbadd` against
`d11af71c6d026b84c3e72650813372f3ffa979c2` completed five untraced warmup passes
and three traced measured passes per query. All 20,937 measured native
compilations succeeded, with no fallback. The pre-migration compiler failed
347 1C queries and four ClickBench queries; these remain visible in coverage
reports and are excluded from speed ratios.

| Workload | Queries successful in both versions | Geometric mean native/before compiler time | Median frontend/RBO time |
| --- | ---: | ---: | ---: |
| 1C | 6,466 | 0.212 | 0.665 |
| TPC-H | 22 | 0.503 | 0.253 |
| TPC-DS | 99 | 0.536 | 0.246 |
| ClickBench | 39 | 0.476 | 0.921 |
| 1C medium | 1 | 0.574 | 0.210 |
| 1C tiny | 1 | 0.275 | 0.361 |

Speed ratios use per-query medians on the common successful cohort. Frontend
time includes parsing, table discovery, binding/scalar typing and translation
to operators. Operator typing is already inside the RBO span. These results
show a compiler speedup but do **not** establish negligible frontend overhead,
particularly on 1C and ClickBench. The binaries, traces, hashes, commands and reports are retained locally
under `/tmp/yqlv2-goal/full-warmed-candidate3`; the reproducible driver is in
[`tools/bench_compilation`](tools/bench_compilation/README.md).

The matching untraced control completed three warmup and three measured passes
per query. All 20,937 measured native requests passed; every native batch also
completed its warmups successfully. The same 351 distinct baseline failures
remain excluded from the common successful cohort. Geometric mean request-time
ratios (native/before) were 0.225 for 1C, 0.473 for TPC-H, 0.682 for TPC-DS,
0.414 for ClickBench, 0.612 for medium and 0.313 for tiny. Request duration and
the traced compiler span are different measurements; keep both when assessing
the improvement. Control logs, commands and per-request observations are in
`/tmp/yqlv2-goal/full-warmed-candidate3-untraced`. These measurements describe
the frozen `9c5cb3fbadd` binary, not subsequent source changes.

### Earlier read-path validation, 2026-11-10

Before the effects and full-corpus expansion, the following suites passed:

| Scope | Passing test cases |
| --- | ---: |
| Standalone parser, binder, compiler and scalar arena | 119 |
| Native RBO, copying, decorrelation, IDs, lookup joins and OLAP | 220 |
| Existing SQL read regressions | 226 |
| Host compilation, execution, routing, parameters and caches | 68 |

Those read and host runs covered their candidate semantic changes, including tuple
comparisons, floating remainder and storage filtering. A subsequent API-only
cleanup removed unused backend-context arguments from scalar factories. All
targets were rebuilt and the 220 RBO/OLAP tests passed again; repeated runs are
not added to the counts above. The eight-query benchmark below used that rebuilt binary.
The TPC execution coverage and populated-data limitations are described above.

### Earlier eight-query compiler measurements, 2026-11-10

Two before/after batches compared the preserved YQL+new-RBO binary from
2026-11-09 with that native-required candidate. Each binary ran eight queries
in three modes for nine measured rounds, after warm-up: 216 samples per binary
per batch, 864 samples overall. The ratios below compare per-query median
compiler counters and use a geometric mean across queries. Cache hits and native
fallback were forbidden; every nontrivial CBO tree had to be optimized.

The host reports AMD EPYC 9654 and 128 CPUs. The first batch used normal
scheduling. An independent worktree build introduced timing variation, so a
second batch gave both binaries the same CPU affinity, CPUs 24–31. The machine
was shared in both batches. Legacy-compiler CPU counters provide a control;
their ratio is baseline divided by the candidate, just like the speedup columns.

| Batch | Compiler CPU speedup | Compiler wall-time speedup | Legacy CPU control ratio |
| --- | ---: | ---: | ---: |
| Normal scheduling | 1.344x | 1.362x | 0.996 |
| Fixed CPU affinity | 1.503x | 1.522x | 1.027 |

Per-query CPU results, retaining both batches:

| Query | Normal-scheduling speedup | Fixed-affinity YQL+RBO, ms | Fixed-affinity native, ms | Fixed-affinity speedup |
| --- | ---: | ---: | ---: | ---: |
| TPC-H Q1 | 1.64x | 81.2 | 46.6 | 1.74x |
| TPC-H Q5 | 1.28x | 154.5 | 104.9 | 1.47x |
| TPC-H Q9 | 1.41x | 154.5 | 123.7 | 1.25x |
| TPC-H Q21 | 1.19x | 202.4 | 139.3 | 1.45x |
| TPC-DS Q17 | 1.42x | 318.6 | 185.3 | 1.72x |
| TPC-DS Q28 | 0.95x | 403.6 | 334.0 | 1.21x |
| TPC-DS Q51 | 1.62x | 171.5 | 96.6 | 1.78x |
| TPC-DS Q52 | 1.36x | 97.1 | 64.2 | 1.51x |

The measured aggregate reduction in compilation CPU is 25.6–33.5%. Q28's initial
4.9% slowdown did not reproduce with fixed affinity, where its legacy control
ratio was 1.001. Per-query gains depend on workload and scheduling; these shared
host measurements support an aggregate improvement on this workload. Compiler
wall counters exclude the empty-table execution included in request timings.

## Build and run

Use the project compiler balancer and normal relwithdebinfo build:

```bash
~/projects/clang-balancer/balance ./ya make --build relwithdebinfo \
  ydb/core/kqp/opt/rbo/frontend/ut ydb/core/kqp/ut/rbo

~/projects/clang-balancer/balance ./ya make --build relwithdebinfo -tA \
  ydb/core/kqp/opt/rbo/frontend/ut 2>&1 | tail

~/projects/clang-balancer/balance ./ya make --build relwithdebinfo ydb/core/kqp/opt/rbo/frontend/cli
printf '%s\n' 'SELECT id, value + 1 FROM t WHERE value > 0' | \
  ydb/core/kqp/opt/rbo/frontend/cli/kqp_native_frontend \
  --table t 'id:Uint64,value:Int32?'
```

Do not edit sources during compilation. Full host suites may exceed the default
shard timeout; direct binary selectors use `+Suite` or `+Suite::Test`. The
compilation benchmark accepts `--test-param NATIVE_COMPILATION_BENCH_ITERATIONS=9`
without `--fork-tests`, whose child invocation does not forward test parameters.
`tools/probe_corpus.py` reports per-query parser/binder routing; it is not an
execution test. `tools/bench_parsers.py` compares native parser routes on an idle
machine.
