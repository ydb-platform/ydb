
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(providers-external_function-provider)
target_compile_options(providers-external_function-provider PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-external_function-provider PUBLIC
  contrib-libs-cxxsupp
  yutil
  common-token_accessor-client
  providers-common-provider
  providers-external_function-expr_nodes
  providers-external_function-common
  providers-external_function-gateway
  library-yql-core
  yql-core-expr_nodes
)
target_sources(providers-external_function-provider PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/external_function/provider/dq_function_load_meta.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/external_function/provider/dq_function_intent_transformer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/external_function/provider/dq_function_provider.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/external_function/provider/dq_function_datasource.cpp
)
