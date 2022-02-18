add_library(cpp-client-ydb_coordination)
target_link_libraries(cpp-client-ydb_coordination PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  api-grpc
  impl-ydb_internal-make_request
  cpp-client-ydb_common_client
  client-ydb_common_client-impl
  cpp-client-ydb_driver
  cpp-client-ydb_types
  client-ydb_types-status
)
target_sources(cpp-client-ydb_coordination PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_coordination/coordination.cpp
)
generate_enum_serilization(cpp-client-ydb_coordination
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_coordination/coordination.h
  INCLUDE_HEADERS
  ydb/public/sdk/cpp/client/ydb_coordination/coordination.h
)
