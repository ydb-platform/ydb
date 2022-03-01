
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(library-yql-sql)
target_link_libraries(library-yql-sql PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-split
  yql-sql-settings
  yql-sql-v0
  sql-v0-lexer
  yql-sql-v1
  sql-v1-lexer
  library-yql-utils
)
target_sources(library-yql-sql PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/cluster_mapping.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/sql.cpp
)
