add_library(public-lib-experimental)
target_link_libraries(public-lib-experimental PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-scheme
  api-grpc-draft
  cpp-client-ydb_proto
  cpp-client-ydb_table
)
target_sources(public-lib-experimental PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/experimental/ydb_clickhouse_internal.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/experimental/ydb_experimental.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/experimental/ydb_logstore.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/experimental/ydb_s3_internal.cpp
)
