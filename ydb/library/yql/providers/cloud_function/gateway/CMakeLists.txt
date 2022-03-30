
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(providers-cloud_function-gateway)
target_compile_options(providers-cloud_function-gateway PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-cloud_function-gateway PUBLIC
  contrib-libs-cxxsupp
  yutil
  common-token_accessor-client
  client-ydb_types-credentials
  cpp-string_utils-quote
  cpp-threading-future
  cpp-grpc-client
  contrib-libs-fmt
  serverless-functions-v1
)
target_sources(providers-cloud_function-gateway PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/cloud_function/gateway/cloud_function_gateway.cpp
)
