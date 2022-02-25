add_library(yql-sql-pg_dummy)
target_compile_options(yql-sql-pg_dummy PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-sql-pg_dummy PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  yql-sql-settings
  providers-common-codec
)
target_sources(yql-sql-pg_dummy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/pg_dummy/pg_sql_dummy.cpp
)
