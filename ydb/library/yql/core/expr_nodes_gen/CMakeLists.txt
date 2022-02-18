add_library(yql-core-expr_nodes_gen)
target_link_libraries(yql-core-expr_nodes_gen PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  yql-public-udf
)
target_sources(yql-core-expr_nodes_gen PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.cpp
)
