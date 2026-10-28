UNITTEST_FOR(ydb/core/formats/arrow/accessor/plain)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/plain
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_plain.cpp
)

END()
