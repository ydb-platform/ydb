add_library(cpp-client-ydb_persqueue_core)
target_link_libraries(cpp-client-ydb_persqueue_core PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  library-cpp-retry
  client-ydb_persqueue_core-impl
  cpp-client-ydb_proto
  cpp-client-ydb_driver
)
target_sources(cpp-client-ydb_persqueue_core PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/proto_accessor.cpp
)
generate_enum_serilization(cpp-client-ydb_persqueue_core
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h
  INCLUDE_HEADERS
  ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h
)
