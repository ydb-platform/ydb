
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yql-dq-expr_nodes STATIC)
set_property(TARGET yql-dq-expr_nodes PROPERTY
  LINKER_LANGUAGE CXX
)
target_link_libraries(yql-dq-expr_nodes PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-core-expr_nodes
)
target_sources(yql-dq-expr_nodes PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.defs.inl.h
)
add_custom_command(
  OUTPUT
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.defs.inl.h
  DEPENDS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  COMMAND
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/library/yql/dq/expr_nodes/dq_expr_nodes.defs.inl.h
)
