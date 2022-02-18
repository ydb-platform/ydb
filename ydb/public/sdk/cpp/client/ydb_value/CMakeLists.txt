add_library(cpp-client-ydb_value)
target_link_libraries(cpp-client-ydb_value PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  cpp-containers-stack_vector
  api-protos
  impl-ydb_internal-value_helpers
  cpp-client-ydb_proto
  client-ydb_types-fatal_error_handlers
  yql-public-decimal
)
target_sources(cpp-client-ydb_value PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_value/value.cpp
)
generate_enum_serilization(cpp-client-ydb_value
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_value/value.h
  INCLUDE_HEADERS
  ydb/public/sdk/cpp/client/ydb_value/value.h
)
