_ydb_sdk_add_library(client-ydb_query-impl)

target_link_libraries(client-ydb_query-impl PUBLIC
  yutil
  api-grpc
  api-protos
  client-ydb_common_client-impl
  client-ydb_proto
)

target_sources(client-ydb_query-impl PRIVATE
  exec_query.cpp
  client_session.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_query-impl)
