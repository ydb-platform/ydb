_ydb_sdk_add_library(impl-ydb_internal-session_pool)

target_link_libraries(impl-ydb_internal-session_pool PUBLIC
  yutil
  threading-future
  api-protos
  client-impl-ydb_endpoints
  client-ydb_types-operation  
)

target_sources(impl-ydb_internal-session_pool PRIVATE
  session_pool.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-session_pool)
