add_library(yql-ast-serialize)
target_link_libraries(yql-ast-serialize PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  yql-core-issue
  library-yql-minikql
)
target_sources(yql-ast-serialize PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/serialize/yql_expr_serialize.cpp
)
