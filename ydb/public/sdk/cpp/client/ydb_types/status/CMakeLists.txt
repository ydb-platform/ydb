add_library(client-ydb_types-status)
target_link_libraries(client-ydb_types-status PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-future
  impl-ydb_internal-plain_status
  cpp-client-ydb_types
  client-ydb_types-fatal_error_handlers
  yql-public-issue
)
target_sources(client-ydb_types-status PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_types/status/status.cpp
)
