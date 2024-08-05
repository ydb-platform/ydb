_ydb_sdk_add_library(client-ydb_types-status)

target_link_libraries(client-ydb_types-status PUBLIC
  yutil
  threading-future
  impl-ydb_internal-plain_status
  client-ydb_types
  client-ydb_types-fatal_error_handlers
  yql-public-issue
)

target_sources(client-ydb_types-status PRIVATE
  status.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_types-status)
