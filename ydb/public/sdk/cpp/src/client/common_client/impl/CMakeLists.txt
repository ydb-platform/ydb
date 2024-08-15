_ydb_sdk_add_library(client-ydb_common_client-impl)

target_link_libraries(client-ydb_common_client-impl PUBLIC
  yutil
  impl-ydb_internal-grpc_connections
)

target_sources(client-ydb_common_client-impl PRIVATE
  client.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_common_client-impl)
