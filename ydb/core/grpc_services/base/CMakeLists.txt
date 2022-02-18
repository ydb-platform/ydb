add_library(core-grpc_services-base INTERFACE)
target_link_libraries(core-grpc_services-base INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  cpp-string_utils-quote
  ydb-core-base
  ydb-core-grpc_streaming
  api-protos
  cpp-client-resources
  yql-public-issue
)
