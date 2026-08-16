LIBRARY()

SRCS(
    ydb_schema.cpp
    yql_completer.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    yql/essentials/sql/v1/ide/completion
    yql/essentials/sql/v1/ide/completion/name/cache/local
    yql/essentials/sql/v1/ide/completion/name/object
    yql/essentials/sql/v1/ide/completion/name/object/simple
    yql/essentials/sql/v1/ide/completion/name/object/simple/cached
    yql/essentials/sql/v1/ide/completion/name/service/impatient
    yql/essentials/sql/v1/ide/completion/name/service/schema
    yql/essentials/sql/v1/ide/completion/name/service/static
    yql/essentials/sql/v1/ide/completion/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    ydb/public/lib/ydb_cli/commands/interactive/highlight/color
    ydb/public/lib/ydb_cli/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
