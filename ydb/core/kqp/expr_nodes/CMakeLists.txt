add_library(core-kqp-expr_nodes)
target_link_libraries(core-kqp-expr_nodes PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-dq-expr_nodes
)
target_sources(core-kqp-expr_nodes INTERFACE
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.defs.inl.h
)
target_sources(core-kqp-expr_nodes PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.cpp
)
add_custom_command(
  OUTPUT
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.defs.inl.h
  DEPENDS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  COMMAND
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/expr_nodes/kqp_expr_nodes.defs.inl.h
)
