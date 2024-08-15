add_subdirectory(impl)

_ydb_sdk_add_library(client-ydb_common_client)

target_link_libraries(client-ydb_common_client PUBLIC
  yutil
  impl-ydb_internal-common
  client-ydb_types-credentials
)

target_sources(client-ydb_common_client PRIVATE
  settings.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_common_client)
