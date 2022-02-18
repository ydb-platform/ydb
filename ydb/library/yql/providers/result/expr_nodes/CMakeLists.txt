add_library(providers-result-expr_nodes)
target_link_libraries(providers-result-expr_nodes PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-core-expr_nodes
  providers-common-provider
)
target_sources(providers-result-expr_nodes INTERFACE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.defs.inl.h
)
target_sources(providers-result-expr_nodes PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.cpp
)
add_custom_command(
  OUTPUT
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.defs.inl.h
  DEPENDS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  COMMAND
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.defs.inl.h
)
