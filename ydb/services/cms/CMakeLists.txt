add_library(ydb-services-cms)
target_link_libraries(ydb-services-cms PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-grpc_services
  ydb-core-mind
  ydb-library-aclib
  api-grpc
  public-lib-operation_id
  cpp-client-resources
)
target_sources(ydb-services-cms PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/cms/grpc_service.cpp
)
