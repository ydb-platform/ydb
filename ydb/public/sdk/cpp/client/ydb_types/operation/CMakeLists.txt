add_library(client-ydb_types-operation)
target_link_libraries(client-ydb_types-operation PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-threading-future
  public-lib-operation_id
  cpp-client-ydb_types
)
target_sources(client-ydb_types-operation PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_types/operation/operation.cpp
)
