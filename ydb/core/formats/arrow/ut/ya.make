UNITTEST_FOR(ydb/core/formats/arrow)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/arrow_kernels
    ydb/library/formats/arrow/simple_builder
    ydb/core/base

    # for NYql::NUdf alloc stuff used in binary_json
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

ADDINCL(
    ydb/library/arrow_clickhouse
)

YQL_LAST_ABI_VERSION()

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    ut_arrow.cpp
    ut_program_step.cpp
    ut_dictionary.cpp
    ut_column_filter.cpp
    ut_hash.cpp
)

END()
