add_library(ydb-services-auth)
target_link_libraries(ydb-services-auth PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  library-cpp-lwtrace
  ydb-core-grpc_services
  ydb-core-protos
  ydb-library-login
  api-grpc
  public-lib-operation_id
)
target_sources(ydb-services-auth PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/auth/grpc_service.cpp
)
