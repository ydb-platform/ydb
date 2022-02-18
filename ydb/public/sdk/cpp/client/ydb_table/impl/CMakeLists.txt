add_library(client-ydb_table-impl)
target_link_libraries(client-ydb_table-impl PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-future
  api-protos
  lib-operation_id-protos
  client-impl-ydb_endpoints
  public-issue-protos
)
target_sources(client-ydb_table-impl PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_table/impl/client_session.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_table/impl/data_query.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_table/impl/request_migrator.cpp
)
