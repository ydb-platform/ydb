Y_BENCHMARK()

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/arrow_kernels
    ydb/library/formats/arrow/simple_builder
    ydb/core/base

    # for NYql::NUdf alloc stuff used in binary_json
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper

    ydb/core/formats/arrow
)

ADDINCL(
    ydb/library/arrow_clickhouse
    ydb/core/formats/arrow
)

YQL_LAST_ABI_VERSION()

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    bench_arrow.cpp
)

END()
