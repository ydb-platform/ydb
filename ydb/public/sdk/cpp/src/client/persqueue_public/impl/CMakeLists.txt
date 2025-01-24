_ydb_sdk_add_library(client-ydb_persqueue_public-impl)

target_link_libraries(client-ydb_persqueue_public-impl PUBLIC
  yutil
  containers-disjoint_interval_tree
  grpc-client
  monlib-dynamic_counters
  monlib-metrics
  string_utils-url
  persqueue-obfuscate
  api-grpc-draft
  impl-ydb_internal-make_request
  client-ydb_common_client-impl
  client-ydb_driver
  string_utils-misc
)

target_sources(client-ydb_persqueue_public-impl PRIVATE
  common.cpp
  persqueue_impl.cpp
  persqueue.cpp
  read_session.cpp
  read_session_messages.cpp
  write_session_impl.cpp
  write_session.cpp
)
