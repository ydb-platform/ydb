add_library(client-ydb_table-query_stats)
target_link_libraries(client-ydb_table-query_stats PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  api-protos
)
target_sources(client-ydb_table-query_stats PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_table/query_stats/stats.cpp
)
