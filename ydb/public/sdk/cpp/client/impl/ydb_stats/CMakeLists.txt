add_library(client-impl-ydb_stats)
target_link_libraries(client-impl-ydb_stats PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-client
  cpp-monlib-metrics
)
target_sources(client-impl-ydb_stats PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_stats/stats.cpp
)
