UNITTEST_FOR(ydb/core/external_sources)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    external_data_source_ut.cpp
    external_source_builder_ut.cpp
    iceberg_ddl_ut.cpp
    object_storage_ut.cpp
)

END()
