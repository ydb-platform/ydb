# Semantic reconciliation of the YQL documentation

Baseline: `55de5036c7957e2d0f9185393450e15de33513d1` in GitHub and
`7784bbca6037cad8b069a421abba6cf46d3e70bb` in Arcadia. RU and EN were compared
independently. The source copy at that baseline is shared; consumer availability
must not be inferred from library membership alone.

## Scope and accepted changes

The earlier mechanical extraction is removed. Twelve addition files serve six
articles in each locale. All twelve upstream hooks are additive and use `YDB`.
No upstream paragraph is replaced or hidden by these hooks.

- WITH: accept the current common hint list and INLINE example. Drop old local
  INFER_SCHEMA, FORCE_INFER_SCHEMA, DIRECT_READ, XLOCK and IGNORETYPEV3 descriptions
  and examples. Keep external-file parameters, partition projection, topic
  watermarks and the topic example in one YDB section after the common examples.
- GROUP BY: keep HOP/HoppingWindow, table/topic examples and watermark integration
  in one addition. Accept the common grouping rules, NaN note and updated alias
  scope description. Drop the extra local introductory list, which repeats the
  common grouping explanation and the retained HOP section.
- FLATTEN: keep the YDB container-column storage restriction as a note. Accept the
  common semantics, including the newer FLATTEN OPTIONAL and FLATTEN COLUMNS text.
- AS_TABLE: retain write examples with explicit or inferred column lists. Accept
  the common Optional/Stream input forms and the Optional<List<Struct>> explanation.
- Containers and Optional: keep storage constraints and NOT NULL examples. Common
  container and NULL semantics are not redefined. Optional storage/DDL text is
  grouped at the end rather than replacing the common introductory paragraphs.
- JSON functions: use the common literal-JsonPath requirement and PASSING section.
  Drop the repetitive local prohibition and extra parameter example; the shared
  literal requirement already excludes query parameters in that position.
- String/Unicode and other imported library pages: discard signature wrapping,
  redundant examples and local rewording. Accept common additions such as
  String::AsciiContainsIgnoreCase, Re2::IsValidRegexp and Math::Expm1/Log1p.
  Preserve the incoming String::SplitToList anchor explicitly.
- WINDOW: accept current common RANGE documentation and execution-complexity text.
  Links to the moved window/group-by/join pages are layout adaptations.
- Common release references link to immutable source files at the reviewed GitHub
  revision. They are not presented as YDB server release numbers.

## Why some articles remain local

A `keep` entry is an explicit migration exclusion, not evidence of a different YQL
implementation. In particular, useful generic corrections should ultimately move
upstream rather than become YDB-only conditional paragraphs.

The KQP tests in `ydb/core/kqp/ut/query/kqp_query_ut.cpp` around lines 3716-3824
explicitly cover DISCARD returning no results in Query Service, ignored DISCARD in
Table/Scan API, and execution of Ensure. They were read as evidence, not rerun here.
The combined DISCARD/INTO RESULT claim and correlated-subquery availability remain
unresolved. The PR does not silently choose between contradictory documentation.

PostgreSQL conversion tables contain a concrete common-source formatting defect:
TzDate32/TzDatetime64/TzTimestamp64 have four cells in a three-column table, losing
pgtext in rendering. The corrected local article and topg include remain local.
Random wording, Digest Init signatures and the DateTime introduction are also
candidates for common corrections, not justification for permanent consumer forks.

The inventory below records all mapped files, including unpublished or identical
ones. Unmapped YDB pages (DDL, topics, indexes, navigation, etc.) are preserved.
Hash guards make updates behind exclusions visible to CI; exclusions must not be
silently refreshed merely to unblock a build.

Current scope: 67 imported files, 12 articles with additions, 70 retained files, counting RU and EN separately.

## Retained-file decisions

| Common path | Locales | Reason |
| --- | --- | --- |
| `_includes/topg.md` | ru | Local correction removes a fourth cell from three-column PostgreSQL conversion rows (TzDate32/TzDatetime64/TzTimestamp64). Import would silently lose pgtext cells. Common documentation needs the same correction, not an YDB conditional. |
| `builtins/aggregation.md` | ru, en | YDB OLAP restriction on AGGREGATE_BY plus provider-specific histogram and SessionWindow visibility. Generic COUNT_IF examples overlap common documentation. |
| `builtins/basic.md` | ru, en | Unresolved Random execution-phase wording; ToBytes(123) example is corrected locally; provider-specific FileContent/ParseFile/TablePath and codegen visibility. Do not replace these with an assumed shared behavior. |
| `builtins/dict.md` | ru, en | Examples using container-valued stored columns are hidden in YDB. Reconcile examples with table variables before importing. |
| `builtins/index.md` | ru, en | YDB availability filters for library groups. |
| `builtins/list.md` | ru, en | Examples using container-valued stored columns are hidden in YDB. Shared list operations are not a YDB-specific implementation. |
| `builtins/types.md` | ru, en | Codegen and container-column example visibility. Retain until examples are provider-neutral. |
| `index.md` | ru, en | YDB landing page and navigation. |
| `recipes/index.md` | ru, en | YDB navigation and visibility of recipe pages. |
| `syntax/action.md` | ru, en | Provider-specific EVALUATE and metaprogram execution visibility; common-library availability alone does not prove YDB API support. |
| `syntax/discard.md` | ru, en | Query Service since 26.2 executes DISCARD; Table and Scan API ignore it for compatibility. Confirmed by KQP Discard tests. Also reconcile common INTO RESULT prohibition. |
| `syntax/export_import.md` | ru, en | Common examples require provider-specific SUBQUERY/file pragmas; diagnostic flags expose unpublished destinations. |
| `syntax/expressions.md` | ru, en | YDB fulltext-index LIKE/ILIKE example must survive; other differences hide provider-only ParseFile/EVALUATE/PROCESS. Needs independent reconciliation of availability. |
| `syntax/index.md` | ru, en | YDB statements and DDL navigation, not a common language article. |
| `syntax/insert_into.md` | ru, en | YDB primary-key conflicts, PRECONDITION_FAILED, RETURNING, external sinks and row/column-table transaction constraints. |
| `syntax/into_result.md` | ru, en | Common text prohibits combination with DISCARD; YDB page omits this prohibition. Do not silently choose either claim without a combined-query test. |
| `syntax/lexer.md` | ru, en | Useful generic query-parameter compilation restrictions absent upstream. Candidate for common documentation, not a YDB-only hook; retained rather than discarded. |
| `syntax/not_yet_supported.md` | ru, en | YDB correlated-subquery and INTERSECT/EXCEPT restrictions versus removed upstream restrictions. Preserve until KQP support is verified. |
| `syntax/process.md` | ru, en | YDB PROCESS command naming/availability differs from MapReduce provider documentation. |
| `syntax/select/concat.md` | ru, en | Provider-specific folder traversal, table ranges and examples. No inference of YDB support from common syntax. |
| `syntax/select/from.md` | ru, en | Provider-specific CONCAT/table discovery and USE references. Old YT folder prose should be removed only together with provider-neutral replacement. |
| `syntax/select/index.md` | ru, en | YDB execution order, SQL examples and navigation to separate SELECT articles. |
| `syntax/select/sample.md` | ru, en | YDB OLAP limitation and TABLESAMPLE feature gate; old YT sampling pragmas need separate provider reconciliation. |
| `syntax/select/where.md` | ru, en | Correlated-subquery restriction and link to YDB alternatives. Must be reconciled with provider support, not just upstream deletion. |
| `syntax/subquery.md` | ru, en | Provider/metaprogram visibility and SUBQUERY name resolution. Requires provider-neutral examples. |
| `syntax/use.md` | ru, en | Cluster/service naming is consumer configuration, not language semantics. |
| `types/index.md` | ru, en | YDB type catalog; common Linear/Tagged navigation includes unpublished pages. |
| `types/primitive.md` | ru, en | YDB primary/index-key constraints, 8 MB storage limit and extended type/Interval examples. Storage limits must remain distinct from language-level type ranges. |
| `types/special.md` | ru, en | Callable S-expression explanation is hidden for YDB. Requires provider-neutral replacement. |
| `udf/list/datetime.md` | ru, en | Generic introductory/resource guidance and interval literal restrictions are useful; upstream has newer Init/Init64 APIs and YQL-version changelog references. Reconcile in common docs rather than conditional replacement. |
| `udf/list/digest.md` | ru, en | Named Init seed arguments are documented locally but absent upstream signatures. This is a common-library documentation correction, not YDB-specific behavior. |
| `udf/list/histogram.md` | ru, en | Links to provider-specific web visualization. Preserve until a consumer-neutral description is agreed. |
| `udf/list/index.md` | ru, en | YDB module catalog includes Knn and Roaring; availability is not inferred from library membership. |
| `udf/list/pcre.md` | ru, en | Recommendation about switching regex engines is hidden for YDB; API compatibility guidance requires confirmation. |
| `udf/list/postgres.md` | ru | Local correction removes a fourth cell from three-column PostgreSQL conversion rows (TzDate32/TzDatetime64/TzTimestamp64). Import would silently lose pgtext cells. Common documentation needs the same correction, not an YDB conditional. |
| `udf/list/yson.md` | ru, en | Consumer recipe visibility and pragma anchors. Preserve until the recipe catalog is reconciled. |

## Full mapping

| Locale | Common path | YDB path | Decision |
| --- | --- | --- | --- |
| ru | `_includes/cast_examples.md` | `_includes/cast_examples.md` | import |
| ru | `_includes/decimal_args.md` | `_includes/decimal_args.md` | import |
| ru | `_includes/frompg.md` | `_includes/frompg.md` | import |
| ru | `_includes/topg.md` | `_includes/topg.md` | keep |
| ru | `builtins/aggregation.md` | `builtins/aggregation.md` | keep |
| ru | `builtins/basic.md` | `builtins/basic.md` | keep |
| ru | `builtins/codegen.md` | `builtins/codegen.md` | import |
| ru | `builtins/dict.md` | `builtins/dict.md` | keep |
| ru | `builtins/index.md` | `builtins/index.md` | keep |
| ru | `builtins/json.md` | `builtins/json.md` | import |
| ru | `builtins/list.md` | `builtins/list.md` | keep |
| ru | `builtins/struct.md` | `builtins/struct.md` | import |
| ru | `builtins/types.md` | `builtins/types.md` | keep |
| ru | `builtins/window.md` | `builtins/window.md` | import |
| ru | `index.md` | `index.md` | keep |
| ru | `recipes/accessing-json.md` | `recipes/accessing-json.md` | import |
| ru | `recipes/index.md` | `recipes/index.md` | keep |
| ru | `recipes/modifying-json.md` | `recipes/modifying-json.md` | import |
| ru | `syntax/action.md` | `syntax/action.md` | keep |
| ru | `syntax/commit.md` | `syntax/commit.md` | import |
| ru | `syntax/declare.md` | `syntax/declare.md` | import |
| ru | `syntax/discard.md` | `syntax/discard.md` | keep |
| ru | `syntax/export_import.md` | `syntax/export_import.md` | keep |
| ru | `syntax/export_import.md` | ru, en | Common examples require provider-specific SUBQUERY/file pragmas; diagnostic flags expose unpublished destinations. |
| `syntax/expressions.md` | `syntax/expressions.md` | keep |
| ru | `syntax/flatten.md` | `syntax/select/flatten.md` | include |
| ru | `syntax/group_by.md` | `syntax/select/group-by.md` | include |
| ru | `syntax/index.md` | `syntax/index.md` | keep |
| ru | `syntax/insert_into.md` | `syntax/insert_into.md` | keep |
| ru | `syntax/into_result.md` | `syntax/into_result.md` | keep |
| ru | `syntax/join.md` | `syntax/select/join.md` | import |
| ru | `syntax/lexer.md` | `syntax/lexer.md` | keep |
| ru | `syntax/not_yet_supported.md` | `syntax/not_yet_supported.md` | keep |
| ru | `syntax/process.md` | `syntax/process.md` | keep |
| ru | `syntax/reduce.md` | `syntax/reduce.md` | import |
| ru | `syntax/select/assume_order_by.md` | `syntax/select/assume_order_by.md` | import |
| ru | `syntax/select/concat.md` | `syntax/select/concat.md` | keep |
| ru | `syntax/select/distinct.md` | `syntax/select/distinct.md` | import |
| ru | `syntax/select/from.md` | `syntax/select/from.md` | keep |
| ru | `syntax/select/from_as_table.md` | `syntax/select/from_as_table.md` | include |
| ru | `syntax/select/from_select.md` | `syntax/select/from_select.md` | import |
| ru | `syntax/select/index.md` | `syntax/select/index.md` | keep |
| ru | `syntax/select/limit_offset.md` | `syntax/select/limit_offset.md` | import |
| ru | `syntax/select/order_by.md` | `syntax/select/order_by.md` | import |
| ru | `syntax/select/sample.md` | `syntax/select/sample.md` | keep |
| ru | `syntax/select/unique_distinct_hints.md` | `syntax/select/unique_distinct_hints.md` | import |
| ru | `syntax/select/where.md` | `syntax/select/where.md` | keep |
| ru | `syntax/select/with.md` | `syntax/select/with.md` | include |
| ru | `syntax/select/without.md` | `syntax/select/without.md` | import |
| ru | `syntax/subquery.md` | `syntax/subquery.md` | keep |
| ru | `syntax/use.md` | `syntax/use.md` | keep |
| ru | `syntax/values.md` | `syntax/values.md` | import |
| ru | `syntax/window.md` | `syntax/select/window.md` | import |
| ru | `types/cast.md` | `types/cast.md` | import |
| ru | `types/containers.md` | `types/containers.md` | include |
| ru | `types/index.md` | `types/index.md` | keep |
| ru | `types/json.md` | `types/json.md` | import |
| ru | `types/optional.md` | `types/optional.md` | include |
| ru | `types/primitive.md` | `types/primitive.md` | keep |
| ru | `types/special.md` | `types/special.md` | keep |
| ru | `types/type_string.md` | `types/type_string.md` | import |
| ru | `types/yson.md` | `types/yson.md` | import |
| ru | `udf/list/datetime.md` | `udf/list/datetime.md` | keep |
| ru | `udf/list/digest.md` | `udf/list/digest.md` | keep |
| ru | `udf/list/histogram.md` | `udf/list/histogram.md` | keep |
| ru | `udf/list/hyperscan.md` | `udf/list/hyperscan.md` | import |
| ru | `udf/list/index.md` | `udf/list/index.md` | keep |
| ru | `udf/list/ip.md` | `udf/list/ip.md` | import |
| ru | `udf/list/math.md` | `udf/list/math.md` | import |
| ru | `udf/list/pcre.md` | `udf/list/pcre.md` | keep |
| ru | `udf/list/pire.md` | `udf/list/pire.md` | import |
| ru | `udf/list/postgres.md` | `udf/list/postgres.md` | keep |
| ru | `udf/list/re2.md` | `udf/list/re2.md` | import |
| ru | `udf/list/string.md` | `udf/list/string.md` | import |
| ru | `udf/list/unicode.md` | `udf/list/unicode.md` | import |
| ru | `udf/list/url.md` | `udf/list/url.md` | import |
| ru | `udf/list/yson.md` | `udf/list/yson.md` | keep |
| en | `_includes/cast_examples.md` | `_includes/cast_examples.md` | import |
| en | `_includes/decimal_args.md` | `_includes/decimal_args.md` | import |
| en | `builtins/aggregation.md` | `builtins/aggregation.md` | keep |
| en | `builtins/basic.md` | `builtins/basic.md` | keep |
| en | `builtins/codegen.md` | `builtins/codegen.md` | import |
| en | `builtins/dict.md` | `builtins/dict.md` | keep |
| en | `builtins/index.md` | `builtins/index.md` | keep |
| en | `builtins/json.md` | `builtins/json.md` | import |
| en | `builtins/list.md` | `builtins/list.md` | keep |
| en | `builtins/struct.md` | `builtins/struct.md` | import |
| en | `builtins/types.md` | `builtins/types.md` | keep |
| en | `builtins/window.md` | `builtins/window.md` | import |
| en | `index.md` | `index.md` | keep |
| en | `recipes/accessing-json.md` | `recipes/accessing-json.md` | import |
| en | `recipes/index.md` | `recipes/index.md` | keep |
| en | `recipes/modifying-json.md` | `recipes/modifying-json.md` | import |
| en | `syntax/action.md` | `syntax/action.md` | keep |
| en | `syntax/commit.md` | `syntax/commit.md` | import |
| en | `syntax/declare.md` | `syntax/declare.md` | import |
| en | `syntax/discard.md` | `syntax/discard.md` | keep |
| en | `syntax/export_import.md` | `syntax/export_import.md` | keep |
| en | `syntax/export_import.md` | ru, en | Common examples require provider-specific SUBQUERY/file pragmas; diagnostic flags expose unpublished destinations. |
| `syntax/expressions.md` | `syntax/expressions.md` | keep |
| en | `syntax/flatten.md` | `syntax/select/flatten.md` | include |
| en | `syntax/group_by.md` | `syntax/select/group-by.md` | include |
| en | `syntax/index.md` | `syntax/index.md` | keep |
| en | `syntax/insert_into.md` | `syntax/insert_into.md` | keep |
| en | `syntax/into_result.md` | `syntax/into_result.md` | keep |
| en | `syntax/join.md` | `syntax/select/join.md` | import |
| en | `syntax/lexer.md` | `syntax/lexer.md` | keep |
| en | `syntax/not_yet_supported.md` | `syntax/not_yet_supported.md` | keep |
| en | `syntax/process.md` | `syntax/process.md` | keep |
| en | `syntax/reduce.md` | `syntax/reduce.md` | import |
| en | `syntax/select/assume_order_by.md` | `syntax/select/assume_order_by.md` | import |
| en | `syntax/select/concat.md` | `syntax/select/concat.md` | keep |
| en | `syntax/select/distinct.md` | `syntax/select/distinct.md` | import |
| en | `syntax/select/from.md` | `syntax/select/from.md` | keep |
| en | `syntax/select/from_as_table.md` | `syntax/select/from_as_table.md` | include |
| en | `syntax/select/from_select.md` | `syntax/select/from_select.md` | import |
| en | `syntax/select/index.md` | `syntax/select/index.md` | keep |
| en | `syntax/select/limit_offset.md` | `syntax/select/limit_offset.md` | import |
| en | `syntax/select/order_by.md` | `syntax/select/order_by.md` | import |
| en | `syntax/select/sample.md` | `syntax/select/sample.md` | keep |
| en | `syntax/select/union.md` | `syntax/select/union.md` | import |
| en | `syntax/select/unique_distinct_hints.md` | `syntax/select/unique_distinct_hints.md` | import |
| en | `syntax/select/where.md` | `syntax/select/where.md` | keep |
| en | `syntax/select/with.md` | `syntax/select/with.md` | include |
| en | `syntax/select/without.md` | `syntax/select/without.md` | import |
| en | `syntax/subquery.md` | `syntax/subquery.md` | keep |
| en | `syntax/use.md` | `syntax/use.md` | keep |
| en | `syntax/values.md` | `syntax/values.md` | import |
| en | `syntax/window.md` | `syntax/select/window.md` | import |
| en | `types/cast.md` | `types/cast.md` | import |
| en | `types/containers.md` | `types/containers.md` | include |
| en | `types/index.md` | `types/index.md` | keep |
| en | `types/json.md` | `types/json.md` | import |
| en | `types/optional.md` | `types/optional.md` | include |
| en | `types/primitive.md` | `types/primitive.md` | keep |
| en | `types/special.md` | `types/special.md` | keep |
| en | `types/type_string.md` | `types/type_string.md` | import |
| en | `udf/list/datetime.md` | `udf/list/datetime.md` | keep |
| en | `udf/list/digest.md` | `udf/list/digest.md` | keep |
| en | `udf/list/histogram.md` | `udf/list/histogram.md` | keep |
| en | `udf/list/hyperscan.md` | `udf/list/hyperscan.md` | import |
| en | `udf/list/index.md` | `udf/list/index.md` | keep |
| en | `udf/list/ip.md` | `udf/list/ip.md` | import |
| en | `udf/list/math.md` | `udf/list/math.md` | import |
| en | `udf/list/pcre.md` | `udf/list/pcre.md` | keep |
| en | `udf/list/pire.md` | `udf/list/pire.md` | import |
| en | `udf/list/re2.md` | `udf/list/re2.md` | import |
| en | `udf/list/string.md` | `udf/list/string.md` | import |
| en | `udf/list/unicode.md` | `udf/list/unicode.md` | import |
| en | `udf/list/url.md` | `udf/list/url.md` | import |
| en | `udf/list/yson.md` | `udf/list/yson.md` | keep |

## Validation

Diplodoc 5.47.3, identical baseline/candidate build configuration:

- Default build: all 1670 baseline pages remain, zero warnings/errors. Rendered
  content differs on 53 YQL reference pages (including transitive includes), as
  expected for common updates and regrouped additions. No changes outside that section.
- Incoming-anchor audit: no new broken internal anchors across the built site.
  One pre-existing broken anchor remains in both builds.
- Diagnostic feature preset: 1654 baseline pages compared, 53 changed; 104 warnings
  before and after, no new warning classes/targets. This preset exposes some
  unsupported/unpublished provider combinations and is not the production preset.
- Standalone common YQL: all 180 rendered pages identical without YDB files.
  Four pre-existing block/endblock warnings remain. This is not a production build
  of the internal YQL site.
- Eleven local preparer tests and six executions of the actual build/release shell
  stage pass, covering disabled, enabled and incomplete installations.
- SQL examples were not executed against a running YDB cluster. Documentation
  builds and shared-library provenance are not a substitute for API availability
  tests for the unresolved entries above.

Tests, generators and build artifacts are deliberately excluded from the PR.
