add_library(yql-core-sql_types)
target_link_libraries(yql-core-sql_types PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yql-core-sql_types PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/sql_types/simple_types.cpp
)
