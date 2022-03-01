
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yql-core-common_opt)
target_compile_options(yql-core-common_opt PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-core-common_opt PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-core
  yql-core-expr_nodes
)
target_sources(yql-core-common_opt PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_extr_members.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_finalizers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_flow1.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_flow2.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_last.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_simple1.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_simple2.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/common_opt/yql_co_transformer.cpp
)
