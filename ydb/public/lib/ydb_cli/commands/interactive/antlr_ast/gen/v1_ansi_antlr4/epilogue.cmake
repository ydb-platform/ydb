set(GRAMMAR_STRING_CORE_SINGLE "~([']) | (QUOTE_SINGLE QUOTE_SINGLE)")
set(GRAMMAR_STRING_CORE_DOUBLE "~([\"]) | (QUOTE_DOUBLE QUOTE_DOUBLE)")
set(GRAMMAR_MULTILINE_COMMENT_CORE "MULTILINE_COMMENT | .")

configure_file(
  ${CMAKE_SOURCE_DIR}/yql/essentials/sql/v1/SQLv1Antlr4.g.in
  ${CMAKE_BINARY_DIR}/ydb/public/ydb_cli/commands/interactive/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4.g
)
