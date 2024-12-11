UNITTEST_FOR(ydb/core/io_formats/arrow/scheme)

SIZE(SMALL)

PEERDIR(
    # for NYql::NUdf alloc stuff used in binary_json
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    csv_arrow_ut.cpp
)

END()
