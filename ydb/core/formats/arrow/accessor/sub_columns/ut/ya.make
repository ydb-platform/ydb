UNITTEST_FOR(ydb/core/formats/arrow/accessor/sub_columns)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/sub_columns
    yql/essentials/public/udf/service/stub
    ydb/core/formats/arrow
)

SRCS(
    ut_sub_columns.cpp
)

YQL_LAST_ABI_VERSION()

END()
