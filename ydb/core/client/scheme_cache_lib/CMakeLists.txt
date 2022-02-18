add_library(core-client-scheme_cache_lib)
target_compile_options(core-client-scheme_cache_lib PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-client-scheme_cache_lib PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-actors-core
  cpp-grpc-client
  cpp-threading-future
  ydb-core-base
  core-client-minikql_compile
  ydb-core-protos
  ydb-core-scheme
  ydb-core-tablet
  ydb-core-tx
)
target_sources(core-client-scheme_cache_lib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/client/scheme_cache_lib/yql_db_scheme_resolver.cpp
)
