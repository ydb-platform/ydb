_ydb_sdk_add_library(client-ydb_topic-impl)

target_link_libraries(client-ydb_topic-impl PUBLIC
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
  client-ydb_topic-common
  client-ydb_proto
  proto_output
)

target_compile_options(client-ydb_topic-impl
  PRIVATE
    -Wno-deprecated
)

target_sources(client-ydb_topic-impl
  PRIVATE
    common.cpp
    deferred_commit.cpp
    event_handlers.cpp
    offsets_collector.cpp
    read_session_event.cpp
    read_session.cpp
    topic_impl.cpp
    topic.cpp
    transaction.cpp
    write_session_impl.cpp
    write_session.cpp
)

_ydb_sdk_install_targets(TARGETS client-ydb_topic-impl)
