add_library(ydb-services-kesus)
target_link_libraries(ydb-services-kesus PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-base
  ydb-core-grpc_services
  ydb-core-grpc_streaming
  core-kesus-proxy
  core-kesus-tablet
  api-grpc
  api-grpc-draft
  public-lib-operation_id
)
target_sources(ydb-services-kesus PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/kesus/grpc_service.cpp
)
