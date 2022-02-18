add_library(ydb-core-yql_testlib)
target_compile_options(ydb-core-yql_testlib PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-core-yql_testlib PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-client
  cpp-regex-pcre
  cpp-testing-unittest
  ydb-core-base
  ydb-core-client
  core-client-minikql_compile
  core-client-server
  ydb-core-engine
  ydb-core-keyvalue
  ydb-core-mind
  ydb-core-protos
  ydb-core-testlib
  core-testlib-actors
  core-testlib-basics
  yql-core-facade
  yql-public-udf
  public-lib-base
  library-yql-core
  providers-common-provider
  providers-common-udf_resolve
)
target_sources(ydb-core-yql_testlib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yql_testlib/yql_testlib.cpp
)
