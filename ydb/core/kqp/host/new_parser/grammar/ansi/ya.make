LIBRARY()

# Generate the ANSI quoting/comment dialect in its own C++ namespace so both
# lexers can coexist in one process and be selected by per-query directives.
SET(KQP_PARSER_NAMESPACE NKqpAnsiAntlr4)
SET(GRAMMAR_STRING_CORE_SINGLE "\"~([']) | (QUOTE_SINGLE QUOTE_SINGLE)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE#]) | (QUOTE_DOUBLE QUOTE_DOUBLE)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE "\"MULTILINE_COMMENT | .\"")

INCLUDE(${ARCADIA_ROOT}/ydb/core/kqp/host/new_parser/grammar/ya.make.inc)

END()
