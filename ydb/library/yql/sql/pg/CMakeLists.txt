add_library(yql-sql-pg)
target_include_directories(yql-sql-pg PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_query_wrapper/contrib/src/postgres/include
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
