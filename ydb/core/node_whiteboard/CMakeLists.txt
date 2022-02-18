add_library(ydb-core-node_whiteboard INTERFACE)
target_link_libraries(ydb-core-node_whiteboard INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-helpers
  cpp-actors-interconnect
  cpp-actors-protos
  cpp-deprecated-enum_codegen
  library-cpp-logger
  cpp-lwtrace-mon
  library-cpp-random_provider
  library-cpp-time_provider
  ydb-core-base
  core-base-services
  core-blobstorage-base
  core-blobstorage-groupinfo
  ydb-core-debug
  ydb-core-erasure
  ydb-core-protos
)
