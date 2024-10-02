LIBRARY()

PEERDIR (
    ydb/library/yql/parser/proto_ast/gen/v1_proto_split
    ydb/library/yql/parser/proto_ast/antlr4
)

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/v4/tool/templates/codegen)
SET(sql_grammar ${antlr_output}/SQLv1Antlr4.g)

SET(ANTLR_PACKAGE_NAME NSQLv1Generated)
SET(PROTOBUF_HEADER_PATH ydb/library/yql/parser/proto_ast/gen/v1_proto_split)
SET(PROTOBUF_SUFFIX_PATH .pb.main.h)

SET(LEXER_PARSER_NAMESPACE NALPDefaultAntlr4)

SET(GRAMMAR_STRING_CORE_SINGLE "\"~(['#BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE##BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE       "\".\"")

CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)
CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg.in ${antlr_templates}/Cpp/Files.stg)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${sql_grammar})
ELSE()
    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/sql/v1/SQLv1Antlr4.g.in ${sql_grammar})
ENDIF()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/libs/antlr4_cpp_runtime/src
)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/ya.make.incl)

RUN_ANTLR4(
    ${sql_grammar}
    -no-listener
    -package NALPDefaultAntlr4
    -lib .
    -o ${antlr_output}
    IN ${sql_grammar} ${antlr_templates}/Cpp/Cpp.stg ${antlr_templates}/Cpp/Files.stg
    OUT SQLv1Antlr4Parser.cpp SQLv1Antlr4Lexer.cpp SQLv1Antlr4Parser.h SQLv1Antlr4Lexer.h
    OUTPUT_INCLUDES
    ${PROTOBUF_HEADER_PATH}/SQLv1Parser.pb.main.h
    ${STG_INCLUDES}
    CWD ${antlr_output}
)

END()
