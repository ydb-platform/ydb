UNITTEST_FOR(ydb/core/formats/arrow/accessor/sub_columns)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/formats/arrow/accessor/sub_columns/ut_common
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/core/formats/arrow
)

SRCS(
    ut_dense_encoding.cpp
)

YQL_LAST_ABI_VERSION()

END()
