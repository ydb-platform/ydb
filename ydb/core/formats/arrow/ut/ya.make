UNITTEST_FOR(ydb/core/formats/arrow)

SIZE(MEDIUM)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/arrow_kernels
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
    ut_size_calcer.cpp
    ut_column_filter.cpp
    ut_hash.cpp
    ut_row_size_calculator.cpp
)

END()
