
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(providers-external_function-gateway)
target_compile_options(providers-external_function-gateway PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-external_function-gateway PUBLIC
  contrib-libs-cxxsupp
  yutil
  common-token_accessor-client
  providers-external_function-common
  cpp-threading-future
)
target_sources(providers-external_function-gateway PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/external_function/gateway/dq_function_gateway.cpp
)
