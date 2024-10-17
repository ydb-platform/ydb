_ydb_sdk_add_library(impl-ydb_internal-value_helpers)

target_link_libraries(impl-ydb_internal-value_helpers PUBLIC
  yutil
  api-protos
  client-ydb_types-fatal_error_handlers
)

target_sources(impl-ydb_internal-value_helpers PRIVATE
  helpers.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-value_helpers)
