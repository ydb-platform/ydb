_ydb_sdk_add_library(impl-ydb_internal-retry)

target_link_libraries(impl-ydb_internal-retry PUBLIC
  yutil
  impl-ydb_internal-grpc_connections
)

target_sources(impl-ydb_internal-retry PRIVATE
  retry.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-retry)
