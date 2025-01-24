_ydb_sdk_add_library(client-ydb_federated_topic-impl)

target_link_libraries(client-ydb_federated_topic-impl PUBLIC
  yutil
  grpc-client
  monlib-dynamic_counters
  monlib-metrics
  string_utils-url
  persqueue-obfuscate
  api-grpc-draft
  api-grpc
  impl-ydb_internal-make_request
  client-ydb_common_client-impl
  client-ydb_driver
  client-ydb_proto
)

target_compile_options(client-ydb_federated_topic-impl PRIVATE
  -Wno-deprecated
)

target_sources(client-ydb_federated_topic-impl PRIVATE
  federated_read_session.cpp
  federated_read_session_event.cpp
  federated_write_session.cpp
  federated_topic_impl.cpp
  federated_topic.cpp
  federation_observer.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_federated_topic-impl)
