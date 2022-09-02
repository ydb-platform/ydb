
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yql-providers-config)
target_compile_options(yql-providers-config PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-providers-config PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
  library-yql-ast
  library-yql-utils
  yql-utils-fetch
  yql-utils-log
  library-yql-core
  yql-core-expr_nodes
  providers-common-proto
  providers-common-provider
  providers-common-activation
)
target_sources(yql-providers-config PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/config/yql_config_provider.cpp
)
