add_library(cpp-client-ydb_result)
target_link_libraries(cpp-client-ydb_result PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
  client-ydb_types-fatal_error_handlers
  cpp-client-ydb_value
)
target_sources(cpp-client-ydb_result PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_result/proto_accessor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_result/result.cpp
)
