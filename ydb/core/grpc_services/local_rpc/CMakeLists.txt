add_library(core-grpc_services-local_rpc INTERFACE)
target_link_libraries(core-grpc_services-local_rpc INTERFACE
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-grpc_services-base
)
