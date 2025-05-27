set(GRAMMAR_STRING_CORE_SINGLE "~(['\\\\]) | (BACKSLASH .)")
set(GRAMMAR_STRING_CORE_DOUBLE "~([\"\\\\]) | (BACKSLASH .)")
set(GRAMMAR_MULTILINE_COMMENT_CORE ".")

configure_file(
  ${CMAKE_SOURCE_DIR}/yql/essentials/sql/v1/SQLv1Antlr4.g.in
  ${CMAKE_BINARY_DIR}/yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4.g
)
