set(GRAMMAR_STRING_CORE_SINGLE "~(['\\]) | (BACKSLASH .)")
set(GRAMMAR_STRING_CORE_DOUBLE "~([\"\\]) | (BACKSLASH .)")
set(GRAMMAR_MULTILINE_COMMENT_CORE ".")

configure_file(
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/interactive/gen/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
  ${CMAKE_BINARY_DIR}/ydb/public/lib/ydb_cli/commands/interactive/gen/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
  COPYONLY
)

configure_file(
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/ydb_cli/commands/interactive/gen/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
  ${CMAKE_BINARY_DIR}/ydb/public/lib/ydb_cli/commands/interactive/gen/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
  COPYONLY
)

configure_file(
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/v1/SQLv1Antlr4.g.in
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4.g
)
