add_library(client-ydb_common_client-impl)
target_link_libraries(client-ydb_common_client-impl PUBLIC
  contrib-libs-cxxsupp
  yutil
  impl-ydb_internal-grpc_connections
)
target_sources(client-ydb_common_client-impl PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_common_client/impl/client.cpp
)
