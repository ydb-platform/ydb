UNITTEST_FOR(ydb/core/formats/arrow/program)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/accessor/composite
    ydb/core/formats/arrow/accessor/dictionary
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/accessor/sparsed
    ydb/core/formats/arrow/filter
    ydb/core/formats/arrow/program
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_kernel_logic.cpp
)

YQL_LAST_ABI_VERSION()

END()
