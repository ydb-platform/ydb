set(GRAMMAR_STRING_CORE_SINGLE "~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .)")
set(GRAMMAR_STRING_CORE_DOUBLE "~(QUOTE_DOUBLE | BACKSLASH) | (BACKSLASH .)")
set(GRAMMAR_MULTILINE_COMMENT_CORE       ".")

configure_file(
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/v1/SQLv1.g.in
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto_split/SQLv1.g
)

