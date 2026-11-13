UNITTEST_FOR(ydb/library/workload/imdb)

SIZE(SMALL)

SRCS(
    imdb_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/import
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/translation
)

END()
