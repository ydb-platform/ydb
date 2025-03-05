UNITTEST_FOR(ydb/library/yql/providers/s3/object_listers)

SRCS(
    yql_s3_path_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
