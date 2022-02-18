add_library(ydb-core-client INTERFACE)
target_link_libraries(ydb-core-client INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-base
  core-client-scheme_cache_lib
  core-client-server
  ydb-core-engine
  lib-deprecated-kicli
)
