add_library(library-yql-ast)
target_link_libraries(library-yql-ast PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-colorizer
  cpp-containers-sorted_vector
  cpp-containers-stack_vector
  cpp-deprecated-enum_codegen
  library-cpp-enumbitset
  cpp-string_utils-levenshtein_diff
  library-cpp-yson
  yql-public-udf
  library-yql-utils
  yql-core-issue
  yql-parser-pg_catalog
)
target_sources(library-yql-ast PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_ast.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_constraint.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_ast_annotation.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_ast_escaping.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_errors.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_expr.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_expr_builder.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_expr_types.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_gc_nodes.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/ast/yql_type_string.cpp
)
