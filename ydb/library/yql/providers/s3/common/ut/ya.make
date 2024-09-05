UNITTEST_FOR(ydb/library/yql/providers/s3/common)

SRCS(
    util_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

END()
