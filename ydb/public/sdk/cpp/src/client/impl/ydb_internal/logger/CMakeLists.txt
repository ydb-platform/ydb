_ydb_sdk_add_library(impl-ydb_internal-logger)

target_link_libraries(impl-ydb_internal-logger PUBLIC
  yutil
  logger
)

target_sources(impl-ydb_internal-logger PRIVATE
  log.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-logger)
