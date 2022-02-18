add_library(cpp-client-ydb_driver)
target_link_libraries(cpp-client-ydb_driver PUBLIC
  contrib-libs-cxxsupp
  yutil
  impl-ydb_internal-common
  impl-ydb_internal-grpc_connections
  cpp-client-resources
  cpp-client-ydb_common_client
  client-ydb_types-status
)
target_sources(cpp-client-ydb_driver PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_driver/driver.cpp
)
