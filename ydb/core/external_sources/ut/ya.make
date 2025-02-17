UNITTEST_FOR(ydb/core/external_sources)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    object_storage_ut.cpp
    external_data_source_ut.cpp
)

END()
