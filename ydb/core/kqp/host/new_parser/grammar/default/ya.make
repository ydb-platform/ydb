LIBRARY()

# Use YQL backslash escaping and non-nested comments for the default dialect.
# Keep its generated symbols separate from both ANSI and legacy parser targets.
SET(KQP_PARSER_NAMESPACE NKqpDefaultAntlr4)
SET(GRAMMAR_STRING_CORE_SINGLE "\"~(['#BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE##BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE "\".\"")

INCLUDE(${ARCADIA_ROOT}/ydb/core/kqp/host/new_parser/grammar/ya.make.inc)

END()
