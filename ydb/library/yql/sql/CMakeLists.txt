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
