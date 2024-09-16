UNITTEST_FOR(ydb/library/formats/arrow)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/arrow_kernels

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
    ut_size_calcer.cpp
)

END()
