LIBRARY()

PEERDIR (
    ydb/library/yql/parser/proto_ast/gen/v1_proto_split
)

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/v4/tool/templates/codegen)
SET(sql_grammar ${antlr_output}/SQLv4.g)

SET(ANTLR_PACKAGE_NAME NSQLv4Generated)
SET(PROTOBUF_HEADER_PATH ydb/library/yql/parser/proto_ast/gen/v1_proto_split)
SET(PROTOBUF_SUFFIX_PATH .pb.main.h)

SET(LEXER_PARSER_NAMESPACE NALPAnsi)

SET(GRAMMAR_STRING_CORE_SINGLE "\"~([']) | (QUOTE_SINGLE QUOTE_SINGLE)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE#]) | (QUOTE_DOUBLE QUOTE_DOUBLE)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE       "\"MULTILINE_COMMENT | .\"")

CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)
CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg.in ${antlr_templates}/Cpp/Files.stg)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${sql_grammar})
ELSE()
    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/sql/v1/SQLv4.g.in ${sql_grammar})
ENDIF()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/libs/antlr4_cpp_runtime/src
)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/ya.make.incl)

RUN_ANTLR4(
    ${sql_grammar}
    -no-listener
    -package NALPAnsi
    -lib .
    -o ${antlr_output}
    IN ${sql_grammar} ${antlr_templates}/Cpp/Cpp.stg ${antlr_templates}/Cpp/Files.stg
    OUT SQLv4Parser.cpp SQLv4Lexer.cpp SQLv4Parser.h SQLv4Lexer.h
    OUTPUT_INCLUDES
    ${PROTOBUF_HEADER_PATH}/SQLv4Parser.pb.main.h
    ${STG_INCLUDES}
    CWD ${antlr_output}
)

END()
