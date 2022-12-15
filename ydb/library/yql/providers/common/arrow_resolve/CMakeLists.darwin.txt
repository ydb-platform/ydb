
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(providers-common-arrow_resolve)
target_compile_options(providers-common-arrow_resolve PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-common-arrow_resolve PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-minikql-arrow
  yql-public-udf
  providers-common-mkql
)
target_sources(providers-common-arrow_resolve PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/arrow_resolve/yql_simple_arrow_resolver.cpp
)
