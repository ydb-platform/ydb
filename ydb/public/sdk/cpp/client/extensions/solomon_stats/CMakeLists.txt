add_library(client-extensions-solomon_stats)
target_link_libraries(client-extensions-solomon_stats PUBLIC
  contrib-libs-cxxsupp
  yutil
  monlib-encode-json
  cpp-monlib-metrics
  cpp-monlib-service
  monlib-service-pages
  cpp-client-ydb_extension
)
target_sources(client-extensions-solomon_stats PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_client.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/extensions/solomon_stats/pull_connector.cpp
)
