add_library(ydb-services-discovery)
target_link_libraries(ydb-services-discovery PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-grpc_services
  ydb-core-mind
  api-grpc
  public-lib-operation_id
)
target_sources(ydb-services-discovery PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/discovery/grpc_service.cpp
)
