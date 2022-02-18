add_library(providers-ydb-actors)
target_compile_options(providers-ydb-actors PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-ydb-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-scheme
  yql-minikql-computation
  common-token_accessor-client
  yql-public-types
  yql-utils-log
  public-lib-experimental
  cpp-client-ydb_driver
  dq-actors-compute
  providers-ydb-proto
)
target_sources(providers-ydb-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/ydb/actors/yql_ydb_read_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.cpp
)
