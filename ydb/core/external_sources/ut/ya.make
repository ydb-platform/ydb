UNITTEST_FOR(ydb/core/external_sources)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    external_data_source_ut.cpp
    external_source_builder_ut.cpp
    iceberg_ddl_ut.cpp
    object_storage_ut.cpp
)

END()
