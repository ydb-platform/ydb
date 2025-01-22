_ydb_sdk_add_library(client-ydb_table-impl)

target_link_libraries(client-ydb_table-impl
  PUBLIC
    yutil
    threading-future
    api-protos
    api-grpc
    library-operation_id
    client-impl-ydb_endpoints
    impl-ydb_internal-session_pool
    client-ydb_table-query_stats
  PRIVATE
    OpenSSL::SSL
)

target_sources(client-ydb_table-impl PRIVATE
  client_session.cpp
  data_query.cpp
  readers.cpp
  request_migrator.cpp
  table_client.cpp
  transaction.cpp
)

target_compile_options(client-ydb_table-impl PRIVATE
  -Wno-deprecated
)

_ydb_sdk_install_targets(TARGETS client-ydb_table-impl)
