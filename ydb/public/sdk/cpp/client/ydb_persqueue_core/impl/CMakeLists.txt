add_library(client-ydb_persqueue_core-impl)
target_link_libraries(client-ydb_persqueue_core-impl PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-disjoint_interval_tree
  cpp-grpc-client
  cpp-monlib-dynamic_counters
  cpp-string_utils-url
  library-persqueue-obfuscate
  api-grpc-draft
  impl-ydb_internal-make_request
  client-ydb_common_client-impl
  cpp-client-ydb_driver
)
target_sources(client-ydb_persqueue_core-impl PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session_messages.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/persqueue.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/persqueue_impl.cpp
)
