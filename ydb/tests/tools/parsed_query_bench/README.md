# Legacy YQL frontend benchmark

This offline tool measures the upstream KQP SQL frontend with YqlSelect forced for the RBO path. Each server sample starts with a serialized ExecuteQueryRequest and stops when the runner has constructed the RBO logical roots. Each client sample starts with SQL text and ends with a serialized request.

| Input | Client constructs | Server consumes |
| --- | --- | --- |
| `text` | Public YQL v1 QueryContent | PrepareGenericQuery receives the original SQL and runs the host SQL translator |
| `antlr_ast` | Generated TSQLv1ParserAST and complete TParsedTokenList | SqlASTToYql translates the grammar tree with its original tokens |
| `yql_ast` | Translated TAstNode tree | Import restores atom bytes, flags, source positions and ordered children in a memory pool |

The optional fourth stage, `kql_query_list`, carries the expression DAG produced by BuildKqlQuery. The receiver reconstructs shared children and lambda arguments, checks table and parameter bindings, and enters the runner with the restored query list. Add `kql.pb` after the three filenames on export and include it in server measurements. Add `kql_query_list` to the stage names for client timing.

```text
Decode ExecuteQueryRequest
  text           → PrepareGenericQuery(SQL) → SQL translator ──┐
  antlr_ast      → SqlASTToYql(grammar tree, tokens) ──────────┤
  yql_ast        → restore translated TAstNode ─────────────────┴→ CompileExpr → host preparation → BuildKqlQuery ─┐
  kql_query_list → restore the expression DAG and bind its tables and parameters ────────────────────────────────┴→ TKqlQueryList
TKqlQueryList → new RBO pipeline → PlanConverter::ConvertRoot → ComputeParents → stop before if (Roots.size() > 1)
```

The grammar input stores main's native token names, contents and positions, including comments, whitespace and EOF. The importer reconstructs the SQL source from token contents and applies leading directives. The parser fingerprint covers the SQL grammar, lexer build definitions and generated protobuf schema. The NewParser benchmark packages the grammar tree with the token fields needed by its own CST importer. Compare request sizes using the serialized files for each implementation.

`--verify TEXT.pb` compiles a public SQL text request through the stock host and prints its ordered result schema at the RBO boundary.

Run a complete four-stage comparison from the repository root. This input needs no table or parameter fixture.

```sh
./ya make --threads=10 --build relwithdebinfo -tA ydb/tests/tools/parsed_query_bench
bench=ydb/tests/tools/parsed_query_bench/parsed_query_legacy_bench
dir=$(mktemp -d)
printf '%s\n' 'SELECT 1 AS id;' > "$dir/query.sql"
set -- "$dir/text.pb" "$dir/grammar.pb" "$dir/yql.pb" "$dir/kql.pb"
"$bench" --export "$dir/query.sql" "$@"
"$bench" --verify "$@"
"$bench" --bench 30 300 "$@" > "$dir/server.csv"
"$bench" --client 30 300 "$dir/query.sql" text antlr_ast yql_ast kql_query_list > "$dir/client.csv"
```

Omit `--catalog` for table-free queries. For table reads, use the NewParser benchmark's offline JSON catalog to provide the same table names, column types, primary keys, nullability and path prefix to both frontends. A template is a binary public ExecuteQueryRequest. Export retains its session, execution options, transaction control and typed parameters and replaces query content. Without a template, export creates an execute-mode request.

`--export`, `--verify`, `--bench` and `--client` run equivalence checks before accepting a comparison. Verification regenerates each supplied protobuf from text, including source positions and query-list column hints. It also compares the lowered roots, ordered output column names and types. It also compares all request fields outside the query. Files can be passed in any order to `--verify` and `--bench`. All formats are local benchmark fixtures. The public Query Service accepts the text variant.

Server CSV records request bytes, wall time and calling-thread CPU time in nanoseconds. Decode covers the entire protobuf request. The text input follows the normal host SQL entry point, with its intermediate timestamp taken after translation. Grammar and YQL AST inputs use the translated-AST entry point. Parse or import ends with a translated YQL AST for syntax inputs or a restored query list for the fourth stage. Frontend covers the remainder through RBO root construction. Total is the sum of the three phases. Client time includes preparing the input and encoding the full envelope. Both modes rotate stage order across rounds and exclude warmup, equivalence checks, file I/O and CSV printing from samples. Server timing starts inside a fresh host session after actor dispatch. Client timing for query-list export also starts after host construction.
