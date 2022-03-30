
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(external_function-cloud_function-actors)
target_compile_options(external_function-cloud_function-actors PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(external_function-cloud_function-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  library-cpp-retry
  yql-dq-runtime
  yql-dq-proto
  dq-actors-transform
  providers-common-http_gateway
  providers-common-codec
  common-schema-mkql
  library-yql-minikql
  yql-minikql-computation
  yql-public-issue
  yql-sql-pg_dummy
  common-token_accessor-client
  client-ydb_types-credentials
)
target_sources(external_function-cloud_function-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/external_function/cloud_function/actors/cloud_function_transform.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/external_function/cloud_function/actors/cloud_function_transform_factory.cpp
)
