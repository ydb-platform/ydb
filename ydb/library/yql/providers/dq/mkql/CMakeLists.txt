
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(providers-dq-mkql)
target_compile_options(providers-dq-mkql PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-mkql PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-core
  providers-common-mkql
  providers-dq-expr_nodes
  providers-dq-interface
)
target_sources(providers-dq-mkql PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/mkql/dqs_mkql_compiler.cpp
)
