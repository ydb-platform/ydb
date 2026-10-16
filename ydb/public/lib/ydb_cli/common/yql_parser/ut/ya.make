UNITTEST_FOR(ydb/public/lib/ydb_cli/common/yql_parser)

REQUIREMENTS(cpu:1)
SRCS(
    yql_parser_ut.cpp
)

DATA(
    arcadia/yql/essentials/data/language/types.json
)

END()
