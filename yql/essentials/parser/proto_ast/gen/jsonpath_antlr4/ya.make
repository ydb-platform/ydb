LIBRARY()

PEERDIR (
    yql/essentials/parser/proto_ast/gen/jsonpath_proto_split_antlr4
    yql/essentials/parser/proto_ast/antlr4
)

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/v4/tool/templates/codegen)
SET(jsonpath_grammar ${antlr_output}/JsonPathAntlr4.g)

SET(ANTLR_PACKAGE_NAME NJsonPathGenerated)
SET(PARSER_AST_NAME JsonPathAntlr4Parser)
SET(PARSER_PROTO_NAME JsonPathAntlr4Parser)
SET(PROTOBUF_HEADER_PATH yql/essentials/parser/proto_ast/gen/jsonpath_proto_split_antlr4)
SET(PROTOBUF_SUFFIX_PATH .pb.main.h)

SET(LEXER_PARSER_NAMESPACE NALPJsonPathAntlr4)

CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/minikql/jsonpath/JsonPathAntlr4.g ${jsonpath_grammar})
CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)
CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg.in ${antlr_templates}/Cpp/Files.stg)

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/libs/antlr4_cpp_runtime/src
)

INCLUDE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/ya.make.incl)

RUN_ANTLR4(
    ${jsonpath_grammar}
    -no-listener
    -Dlanguage=Cpp
    -package NALPJsonPathAntlr4
    -lib .
    -o ${antlr_output}
    IN ${jsonpath_grammar} ${antlr_templates}/Cpp/Cpp.stg ${antlr_templates}/Cpp/Files.stg
    OUT JsonPathAntlr4Parser.cpp JsonPathAntlr4Lexer.cpp JsonPathAntlr4Parser.h JsonPathAntlr4Lexer.h
    OUTPUT_INCLUDES
    ${PROTOBUF_HEADER_PATH}/JsonPathAntlr4Parser.pb.main.h
    ${STG_INCLUDES}
    CWD ${antlr_output}
)

END()
