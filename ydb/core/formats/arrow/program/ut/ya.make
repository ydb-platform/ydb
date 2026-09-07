UNITTEST_FOR(ydb/core/formats/arrow/program)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/formats/arrow/program
    ydb/core/formats/arrow/serializer
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_kernel_logic.cpp
)

YQL_LAST_ABI_VERSION()

END()
