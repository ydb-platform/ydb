
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(common-schema-mkql)
target_compile_options(common-schema-mkql PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(common-schema-mkql PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-yson
  cpp-yson-node
  library-yql-minikql
  yql-public-udf
  library-yql-utils
  providers-common-codec
  common-schema-parser
  yql-parser-pg_catalog
)
target_sources(common-schema-mkql PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.cpp
)
