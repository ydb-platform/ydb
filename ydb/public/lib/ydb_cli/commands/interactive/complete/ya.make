LIBRARY()

SRCS(
    ydb_schema.cpp
    yql_completer.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    yql/essentials/sql/v1/complete
    yql/essentials/sql/v1/complete/name/cache/local
    yql/essentials/sql/v1/complete/name/object
    yql/essentials/sql/v1/complete/name/object/simple
    yql/essentials/sql/v1/complete/name/object/simple/cached
    yql/essentials/sql/v1/complete/name/service/impatient
    yql/essentials/sql/v1/complete/name/service/schema
    yql/essentials/sql/v1/complete/name/service/static
    yql/essentials/sql/v1/complete/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    ydb/public/lib/ydb_cli/commands/interactive/highlight/color
    ydb/public/lib/ydb_cli/common
)

END()
