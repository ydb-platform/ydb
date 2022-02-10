# libpg_query

C library for accessing the PostgreSQL parser outside of the server.

This library uses the actual PostgreSQL server source to parse SQL queries and return the internal PostgreSQL parse tree.

Note that this is mostly intended as a base library for [pg_query](https://github.com/pganalyze/pg_query) (Ruby), [pg_query.go](https://github.com/pganalyze/pg_query_go) (Go), [pgsql-parser](https://github.com/pyramation/pgsql-parser) (Node), [psqlparse](https://github.com/alculquicondor/psqlparse) (Python) and [pglast](https://pypi.org/project/pglast/) (Python 3).

You can find further background to why a query's parse tree is useful here: https://pganalyze.com/blog/parse-postgresql-queries-in-ruby.html


## Installation

```sh
git clone -b 13-latest git://github.com/pganalyze/libpg_query
cd libpg_query
make
```

Due to compiling parts of PostgreSQL, running `make` will take a bit. Expect up to 3 minutes.

For a production build, its best to use a specific git tag (see CHANGELOG).


## Usage: Parsing a query

A [full example](https://github.com/pganalyze/libpg_query/blob/master/examples/simple.c) that parses a query looks like this:

```c
#include <pg_query.h>
#include <stdio.h>

int main() {
  PgQueryParseResult result;

  result = pg_query_parse("SELECT 1");

  printf("%s\n", result.parse_tree);

  pg_query_free_parse_result(result);
}
```

Compile it like this:

```
cc -Ilibpg_query -Llibpg_query example.c -lpg_query
```

This will output the parse tree (whitespace adjusted here for better readability):

```json
{
    "version": 130003,
    "stmts": [
        {
            "stmt": {
                "SelectStmt": {
                    "targetList": [
                        {
                            "ResTarget": {
                                "val": {
                                    "A_Const": {
                                        "val": {
                                            "Integer": {
                                                "ival": 1
                                            }
                                        },
                                        "location": 7
                                    }
                                },
                                "location": 7
                            }
                        }
                    ],
                    "limitOption": "LIMIT_OPTION_DEFAULT",
                    "op": "SETOP_NONE"
                }
            }
        }
    ]
}
```

## Usage: Scanning a query into its tokens using the PostgreSQL scanner/lexer

pg_query also exposes the underlying scanner of Postgres, which is also used in
the very first part in the parsing process. It can be useful on its own for e.g.
syntax highlighting, where one is mostly concerned with differentiating keywords
from identifiers and other parts of the query:

```c
#include <stdio.h>

#include <pg_query.h>
#include "protobuf/pg_query.pb-c.h"

int main() {
  PgQueryScanResult result;
  PgQuery__ScanResult *scan_result;
  PgQuery__ScanToken *scan_token;
  const ProtobufCEnumValue *token_kind;
  const ProtobufCEnumValue *keyword_kind;
  const char *input = "SELECT update AS left /* comment */ FROM between";

  result = pg_query_scan(input);
  scan_result = pg_query__scan_result__unpack(NULL, result.pbuf.len, (void *) result.pbuf.data);

  printf("  version: %d, tokens: %ld, size: %d\n", scan_result->version, scan_result->n_tokens, result.pbuf.len);
  for (size_t j = 0; j < scan_result->n_tokens; j++) {
    scan_token = scan_result->tokens[j];
    token_kind = protobuf_c_enum_descriptor_get_value(&pg_query__token__descriptor, scan_token->token);
    keyword_kind = protobuf_c_enum_descriptor_get_value(&pg_query__keyword_kind__descriptor, scan_token->keyword_kind);
    printf("  \"%.*s\" = [ %d, %d, %s, %s ]\n", scan_token->end - scan_token->start, &(input[scan_token->start]), scan_token->start, scan_token->end, token_kind->name, keyword_kind->name);
  }

  pg_query__scan_result__free_unpacked(scan_result, NULL);
  pg_query_free_scan_result(result);

  return 0;
}
```

This will output the following:

```
  version: 130003, tokens: 7, size: 77
  "SELECT" = [ 0, 6, SELECT, RESERVED_KEYWORD ]
  "update" = [ 7, 13, UPDATE, UNRESERVED_KEYWORD ]
  "AS" = [ 14, 16, AS, RESERVED_KEYWORD ]
  "left" = [ 17, 21, LEFT, TYPE_FUNC_NAME_KEYWORD ]
  "/* comment */" = [ 22, 35, C_COMMENT, NO_KEYWORD ]
  "FROM" = [ 36, 40, FROM, RESERVED_KEYWORD ]
  "between" = [ 41, 48, BETWEEN, COL_NAME_KEYWORD ]
```

Where the each element in the token list has the following fields:

1. Start location in the source string
2. End location in the source string
3. Token value - see Token type in `protobuf/pg_query.proto`
4. Keyword type - see KeywordKind type in `protobuf/pg_query.proto`, possible values:
  `NO_KEYWORD`: Not a keyword
  `UNRESERVED_KEYWORD`: Unreserved keyword (available for use as any kind of unescaped name)
  `COL_NAME_KEYWORD`: Unreserved keyword (can be unescaped column/table/etc names, cannot be unescaped function or type name)
  `TYPE_FUNC_NAME_KEYWORD`: Reserved keyword (can be unescaped function or type name, cannot be unescaped column/table/etc names)
  `RESERVED_KEYWORD`: Reserved keyword (cannot be unescaped column/table/variable/type/function names)

Note that whitespace does not show as tokens.

## Usage: Fingerprinting a query

Fingerprinting allows you to identify similar queries that are different only because
of the specific object that is being queried for (i.e. different object ids in the WHERE clause),
or because of formatting.

Example:

```c
#include <pg_query.h>
#include <stdio.h>

int main() {
  PgQueryFingerprintResult result;

  result = pg_query_fingerprint("SELECT 1");

  printf("%s\n", result.fingerprint_str);

  pg_query_free_fingerprint_result(result);
}
```

This will output:

```
50fde20626009aba
```

See https://github.com/pganalyze/libpg_query/wiki/Fingerprinting for the full fingerprinting rules.

## Usage: Parsing a PL/pgSQL function (Experimental)

A [full example](https://github.com/pganalyze/libpg_query/blob/master/examples/simple_plpgsql.c) that parses a [PL/pgSQL](https://www.postgresql.org/docs/current/static/plpgsql.html) method looks like this:

```c
#include <pg_query.h>
#include <stdio.h>
#include <stdlib.h>

int main() {
  PgQueryPlpgsqlParseResult result;

  result = pg_query_parse_plpgsql(" \
  CREATE OR REPLACE FUNCTION cs_fmt_browser_version(v_name varchar, \
                                                  v_version varchar) \
RETURNS varchar AS $$ \
BEGIN \
    IF v_version IS NULL THEN \
        RETURN v_name; \
    END IF; \
    RETURN v_name || '/' || v_version; \
END; \
$$ LANGUAGE plpgsql;");

  if (result.error) {
    printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
  } else {
    printf("%s\n", result.plpgsql_funcs);
  }

  pg_query_free_plpgsql_parse_result(result);

  return 0;
}
```

This will output:

```json
[
{"PLpgSQL_function":{"datums":[{"PLpgSQL_var":{"refname":"found","datatype":{"PLpgSQL_type":{"typname":"UNKNOWN"}}}}],"action":{"PLpgSQL_stmt_block":{"lineno":1,"body":[{"PLpgSQL_stmt_if":{"lineno":1,"cond":{"PLpgSQL_expr":{"query":"SELECT v_version IS NULL"}},"then_body":[{"PLpgSQL_stmt_return":{"lineno":1,"expr":{"PLpgSQL_expr":{"query":"SELECT v_name"}}}}]}},{"PLpgSQL_stmt_return":{"lineno":1,"expr":{"PLpgSQL_expr":{"query":"SELECT v_name || '/' || v_version"}}}}]}}}}
]
```

## Versions

For stability, it is recommended you use individual tagged git versions, see CHANGELOG.

Each major version is maintained in a dedicated git branch. Only the latest Postgres stable release receives active updates.

| PostgreSQL Major Version | Branch     | Status              |
|--------------------------|------------|---------------------|
| 13                       | 13-latest  | Active development  |
| 12                       | (n/a)      | Not supported       |
| 11                       | (n/a)      | Not supported       |
| 10                       | 10-latest  | Critical fixes only |
| 9.6                      | (n/a)      | Not supported       |
| 9.5                      | 9.5-latest | No longer supported |
| 9.4                      | 9.4-latest | No longer supported |

## Resources

pg_query wrappers in other languages:

* Ruby: [pg_query](https://github.com/pganalyze/pg_query)
* Go: [pg_query_go](https://github.com/pganalyze/pg_query_go)
* Javascript (Node): [pgsql-parser](https://github.com/pyramation/pgsql-parser)
* Javascript (Browser): [pg-query-emscripten](https://github.com/pganalyze/pg-query-emscripten)
* Python: [psqlparse](https://github.com/alculquicondor/psqlparse), [pglast](https://github.com/lelit/pglast)
* OCaml: [pg_query-ocaml](https://github.com/roddyyaga/pg_query-ocaml)
* Rust: [pg_query.rs](https://github.com/paupino/pg_query.rs)

Products, tools and libraries built on pg_query:

* [pganalyze](https://pganalyze.com/)
* [hsql](https://github.com/JackDanger/hsql)
* [sqlint](https://github.com/purcell/sqlint)
* [pghero](https://github.com/ankane/pghero)
* [dexter](https://github.com/ankane/dexter)
* [pgscope](https://github.com/gjalves/pgscope)
* [pg_materialize](https://github.com/aanari/pg-materialize)
* [DuckDB](https://github.com/cwida/duckdb) ([details](https://github.com/cwida/duckdb/tree/master/third_party/libpg_query))
* and more

Please feel free to [open a PR](https://github.com/pganalyze/libpg_query/pull/new/master) to add yours! :)


## Authors

- [Lukas Fittl](mailto:lukas@fittl.com)


## License

PostgreSQL server source code, used under the [PostgreSQL license](https://www.postgresql.org/about/licence/).<br>
Portions Copyright (c) 1996-2017, The PostgreSQL Global Development Group<br>
Portions Copyright (c) 1994, The Regents of the University of California

All other parts are licensed under the 3-clause BSD license, see LICENSE file for details.<br>
Copyright (c) 2017, Lukas Fittl <lukas@fittl.com>
