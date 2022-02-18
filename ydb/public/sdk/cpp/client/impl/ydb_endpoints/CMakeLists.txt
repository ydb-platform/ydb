add_library(client-impl-ydb_endpoints)
target_link_libraries(client-impl-ydb_endpoints PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-metrics
  api-grpc
)
target_sources(client-impl-ydb_endpoints PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_endpoints/endpoints.cpp
)
