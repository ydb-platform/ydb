UNITTEST_FOR(ydb/core/formats/arrow/accessor/sparsed)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/sparsed
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_composite.cpp
)

END()
