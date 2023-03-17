UNITTEST_FOR(ydb/core/external_sources)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    object_storage_ut.cpp
)

END()
