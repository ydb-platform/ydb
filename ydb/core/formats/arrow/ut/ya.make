UNITTEST_FOR(ydb/core/formats/arrow)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/arrow_kernels
    ydb/library/formats/arrow/simple_builder
    ydb/core/formats/arrow/program
    ydb/core/base
    ydb/library/formats/arrow

    # for NYql::NUdf alloc stuff used in binary_json
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper

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
