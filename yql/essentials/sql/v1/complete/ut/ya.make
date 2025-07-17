UNITTEST_FOR(yql/essentials/sql/v1/complete)

SRCS(
    sql_complete_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/sql/v1/complete/name/cache/local
    yql/essentials/sql/v1/complete/name/cluster/static
    yql/essentials/sql/v1/complete/name/object/simple
    yql/essentials/sql/v1/complete/name/object/simple/cached
    yql/essentials/sql/v1/complete/name/object/simple/static
    yql/essentials/sql/v1/complete/name/service/cluster
    yql/essentials/sql/v1/complete/name/service/impatient
    yql/essentials/sql/v1/complete/name/service/schema
    yql/essentials/sql/v1/complete/name/service/static
    yql/essentials/sql/v1/complete/name/service/union
)

END()
