add_library(cpp-client-ydb_rate_limiter)
target_link_libraries(cpp-client-ydb_rate_limiter PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-grpc
  client-ydb_common_client-impl
  cpp-client-ydb_driver
)
target_sources(cpp-client-ydb_rate_limiter PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_rate_limiter/rate_limiter.cpp
)
