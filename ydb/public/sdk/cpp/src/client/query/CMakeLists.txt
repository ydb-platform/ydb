add_subdirectory(impl)

_ydb_sdk_add_library(client-ydb_query)

target_link_libraries(client-ydb_query PUBLIC
  yutil
  impl-ydb_internal-make_request
  impl-ydb_internal-kqp_session_common
  impl-ydb_internal-session_pool
  impl-ydb_internal-retry
  client-ydb_common_client
  client-ydb_driver
  client-ydb_query-impl
  client-ydb_result
  client-ydb_types-operation
  api-protos
  api-grpc
)

target_sources(client-ydb_query PRIVATE
  client.cpp
  query.cpp
  stats.cpp
  tx.cpp
)

_ydb_sdk_make_client_component(Query client-ydb_query)
