add_library(ydb-services-monitoring)
target_link_libraries(ydb-services-monitoring PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-grpc_services
  ydb-core-protos
  api-grpc
  public-lib-operation_id
)
target_sources(ydb-services-monitoring PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/monitoring/grpc_service.cpp
)
