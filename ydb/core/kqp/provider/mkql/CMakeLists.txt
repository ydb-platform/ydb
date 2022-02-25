add_library(kqp-provider-mkql STATIC)
set_property(TARGET kqp-provider-mkql PROPERTY
  LINKER_LANGUAGE CXX
)
target_link_libraries(kqp-provider-mkql PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(kqp-provider-mkql PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.defs.inl.h
)
add_custom_command(
  OUTPUT
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.defs.inl.h
  DEPENDS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  COMMAND
  ${CMAKE_BINARY_DIR}/ydb/library/yql/core/expr_nodes_gen/gen/gen
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.jnj
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.json
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.gen.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.decl.inl.h
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.defs.inl.h
)
