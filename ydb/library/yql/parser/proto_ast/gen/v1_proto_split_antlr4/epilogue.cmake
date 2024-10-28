set(GRAMMAR_STRING_CORE_SINGLE "~(['\\\\]) | (BACKSLASH .)")
set(GRAMMAR_STRING_CORE_DOUBLE "~([\"\\\\]) | (BACKSLASH .)")
set(GRAMMAR_MULTILINE_COMMENT_CORE ".")

configure_file(
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/v1/SQLv1Antlr4.g.in
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4.g
)

