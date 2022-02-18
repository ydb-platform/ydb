add_library(ydb-services-rate_limiter)
target_link_libraries(ydb-services-rate_limiter PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-grpc_services
  core-kesus-tablet
  api-grpc
  ydb-services-ydb
)
target_sources(ydb-services-rate_limiter PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/rate_limiter/grpc_service.cpp
)
