_ydb_sdk_add_library(impl-ydb_internal-db_driver_state)

target_link_libraries(impl-ydb_internal-db_driver_state PUBLIC
  yutil
  string_utils-quote
  threading-future
  client-impl-ydb_endpoints
  impl-ydb_internal-logger
  impl-ydb_internal-plain_status
  client-ydb_types-credentials
)

target_sources(impl-ydb_internal-db_driver_state PRIVATE
  authenticator.cpp
  endpoint_pool.cpp
  state.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-db_driver_state)
