UNITTEST_FOR(ydb/core/formats/arrow)

SIZE(SMALL)

PEERDIR(
    ydb/core/tx/limiter/grouped_memory/usage
    yql/essentials/public/udf/service/stub
    yql/essentials/parser/pg_wrapper
)

SRCS(
    ut_manager.cpp
)

YQL_LAST_ABI_VERSION()

END()
