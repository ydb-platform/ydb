UNITTEST_FOR(ydb/library/yql/providers/s3/json)

YQL_LAST_ABI_VERSION()

SRCS(
    json_row_parser_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/arrow
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

END()
