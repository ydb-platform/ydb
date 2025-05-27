set(GRAMMAR_STRING_CORE_SINGLE "~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .)")
set(GRAMMAR_STRING_CORE_DOUBLE "~(QUOTE_DOUBLE | BACKSLASH) | (BACKSLASH .)")
set(GRAMMAR_MULTILINE_COMMENT_CORE ".")

configure_file(
  ${CMAKE_SOURCE_DIR}/yql/essentials/sql/v0/SQL.g
  ${CMAKE_BINARY_DIR}/yql/essentials/parser/proto_ast/gen/v0_proto_split/SQL.g
)

