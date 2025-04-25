LIBRARY()

SRCS(
    ydb_schema_gateway.cpp
    yql_completer.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    yql/essentials/sql/v1/complete
    yql/essentials/sql/v1/complete/name/object
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    ydb/public/lib/ydb_cli/commands/interactive/highlight/color
    ydb/public/lib/ydb_cli/common
)

END()
