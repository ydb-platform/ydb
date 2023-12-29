LIBRARY()

PEERDIR (
    ydb/library/yql/parser/proto_ast/gen/v1_proto_split
)

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/codegen/templates)
SET(sql_grammar ${antlr_output}/SQLv1.g)

SET(ANTLR_PACKAGE_NAME NSQLv1Generated)
SET(PROTOBUF_HEADER_PATH ydb/library/yql/parser/proto_ast/gen/v1_proto_split)
SET(PROTOBUF_SUFFIX_PATH .pb.main.h)

SET(LEXER_PARSER_NAMESPACE NALPAnsi)

SET(GRAMMAR_STRING_CORE_SINGLE "\"~QUOTE_SINGLE | (QUOTE_SINGLE QUOTE_SINGLE)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~QUOTE_DOUBLE | (QUOTE_DOUBLE QUOTE_DOUBLE)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE       "\"MULTILINE_COMMENT | .\"")

CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${sql_grammar})
ELSE()
    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/sql/v1/SQLv1.g.in ${sql_grammar})
ENDIF()

NO_COMPILER_WARNINGS()

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/ya.make.incl)

RUN_ANTLR(
    ${sql_grammar}
    -lib .
    -fo ${antlr_output}
    IN ${sql_grammar} ${antlr_templates}/Cpp/Cpp.stg
    OUT SQLv1Parser.cpp SQLv1Lexer.cpp SQLv1Parser.h SQLv1Lexer.h
    OUTPUT_INCLUDES
    ${PROTOBUF_HEADER_PATH}/SQLv1Parser.pb.main.h
    ${STG_INCLUDES}
    CWD ${antlr_output}
)

END()
