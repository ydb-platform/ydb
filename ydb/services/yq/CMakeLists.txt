add_library(ydb-services-yq)
target_link_libraries(ydb-services-yq PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  library-cpp-retry
  ydb-core-grpc_services
  core-grpc_services-base
  ydb-library-protobuf_printer
  api-grpc
)
target_sources(ydb-services-yq PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/yq/grpc_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/yq/private_grpc.cpp
)
