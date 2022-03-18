
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(providers-s3-actors)
target_compile_options(providers-s3-actors PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-s3-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-minikql-computation
  common-token_accessor-client
  common-schema-mkql
  yql-public-types
  dq-actors-compute
  providers-common-http_gateway
  providers-s3-proto
)
target_sources(providers-s3-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/actors/yql_s3_read_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/actors/yql_s3_source_factory.cpp
)
