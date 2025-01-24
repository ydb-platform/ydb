_ydb_sdk_add_library(client-ydb_types-fatal_error_handlers)

target_link_libraries(client-ydb_types-fatal_error_handlers PUBLIC
  yutil
  client-ydb_types-exceptions
)

target_sources(client-ydb_types-fatal_error_handlers PRIVATE
  handlers.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_types-fatal_error_handlers)
