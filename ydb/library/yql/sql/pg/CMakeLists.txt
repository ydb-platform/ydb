
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yql-sql-pg)
target_include_directories(yql-sql-pg PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_query_wrapper/postgresql/src/include
)
target_link_libraries(yql-sql-pg PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  yql-parser-pg_query_wrapper
  yql-sql-settings
)
target_sources(yql-sql-pg PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/pg/pg_sql.cpp
)
