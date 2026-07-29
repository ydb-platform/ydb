UNITTEST_FOR(yql/essentials/sql/v1/ide/completion)

SRCS(
    sql_complete_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/sql/v1/ide/completion/name/cache/local
    yql/essentials/sql/v1/ide/completion/name/cluster/static
    yql/essentials/sql/v1/ide/completion/name/object/simple
    yql/essentials/sql/v1/ide/completion/name/object/simple/cached
    yql/essentials/sql/v1/ide/completion/name/object/simple/static
    yql/essentials/sql/v1/ide/completion/name/service/cluster
    yql/essentials/sql/v1/ide/completion/name/service/impatient
    yql/essentials/sql/v1/ide/completion/name/service/schema
    yql/essentials/sql/v1/ide/completion/name/service/static
    yql/essentials/sql/v1/ide/completion/name/service/union
)

END()
