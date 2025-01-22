_ydb_sdk_add_library(impl-ydb_internal-thread_pool)

target_link_libraries(impl-ydb_internal-thread_pool PUBLIC
  yutil
)

target_sources(impl-ydb_internal-thread_pool PRIVATE
  pool.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-thread_pool)
