YQL_UDF_YDB(datetime2_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    datetime_udf.cpp
)

PEERDIR(
    util/draft
    ydb/library/yql/public/udf/arrow
    ydb/library/yql/minikql
    ydb/library/yql/minikql/datetime
    ydb/library/yql/public/udf/tz
)

END()

RECURSE_FOR_TESTS(
    test
)
