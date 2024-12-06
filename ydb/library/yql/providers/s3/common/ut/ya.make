UNITTEST_FOR(ydb/library/yql/providers/s3/common)

SRCS(
    util_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
