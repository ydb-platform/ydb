_ydb_sdk_add_library(client-ydb_topic-common)

target_link_libraries(client-ydb_topic-common PUBLIC
  client-ydb_common_client-impl
  client-ydb_types
  monlib-dynamic_counters
  retry
)

target_sources(client-ydb_topic-common PRIVATE
  executor_impl.cpp
  retry_policy.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_topic-common)
