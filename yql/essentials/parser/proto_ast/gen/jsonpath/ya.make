LIBRARY()

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/v4/tool/templates/codegen)
SET(jsonpath_grammar ${ARCADIA_ROOT}/yql/essentials/minikql/jsonpath/JsonPath.g)

SET(ANTLR_PACKAGE_NAME NJsonPathGenerated)
SET(ANTLR_AST_NAME TJsonPathParserAST)
SET(ANTLR_PARSER_NAME JsonPathParser)
SET(PROTOBUF_HEADER_PATH ${MODDIR})
SET(PROTOBUF_SUFFIX_PATH .pb.h)
SET(LEXER_PARSER_NAMESPACE NALP)

CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Java/Java.stg.in ${antlr_templates}/Java/Java.stg)
CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)
CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg.in ${antlr_templates}/Cpp/Files.stg)

# Generate JsonPathParser.proto from the grammar via the Java.stg ANTLR4 proto codegen.
RUN_ANTLR4(
    ${jsonpath_grammar}
    -lib .
    -no-listener
    -o ${antlr_output}
    -Dlanguage=Java
    IN ${jsonpath_grammar} ${antlr_templates}/Java/Java.stg
    OUT_NOAUTO JsonPathParser.proto
    CWD ${antlr_output}
)

IF (USE_VANILLA_PROTOC)
    SET(PROTOC_PATH contrib/tools/protoc_std)
    PEERDIR(contrib/libs/protobuf_std)
ELSE()
    SET(PROTOC_PATH contrib/tools/protoc/bin)
    PEERDIR(contrib/libs/protobuf)
ENDIF()

RUN_PROGRAM(
    $PROTOC_PATH -I=${CURDIR} -I=${ARCADIA_ROOT} -I=${ARCADIA_BUILD_ROOT} -I=${ARCADIA_ROOT}/contrib/libs/protobuf/src
    --cpp_out=${ARCADIA_BUILD_ROOT} --cpp_styleguide_out=${ARCADIA_BUILD_ROOT}
    --plugin=protoc-gen-cpp_styleguide=contrib/tools/protoc/plugins/cpp_styleguide
    JsonPathParser.proto
    IN JsonPathParser.proto
    TOOL contrib/tools/protoc/plugins/cpp_styleguide
    OUT_NOAUTO JsonPathParser.pb.h JsonPathParser.pb.cc
    CWD ${antlr_output}
)

NO_COMPILER_WARNINGS()

# Generate the C++ parser/lexer from the grammar via Cpp.stg + Files.stg.
# The generated sources #include JsonPathParser.pb.h (the protoc output
# above); OUTPUT_INCLUDES orders this codegen after the protoc step.
INCLUDE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/ya.make.incl)
RUN_ANTLR4(
    ${jsonpath_grammar}
    -no-listener
    -package NALP
    -lib .
    -o ${antlr_output}
    IN ${jsonpath_grammar} ${antlr_templates}/Cpp/Cpp.stg ${antlr_templates}/Cpp/Files.stg
    OUT JsonPathParser.cpp JsonPathLexer.cpp JsonPathParser.h JsonPathLexer.h
    OUTPUT_INCLUDES
    ${PROTOBUF_HEADER_PATH}/JsonPathParser.pb.h
    ${STG_INCLUDES}
    CWD ${antlr_output}
)

SRCS(JsonPathParser.pb.cc)

# proto_ast/antlr4 transitively PEERDIRs contrib/libs/antlr4_cpp_runtime, whose
# GLOBAL CFLAGS (-DANTLR4CPP_USING_ABSEIL etc.) and GLOBAL ADDINCL match the
# runtime's ABI. Getting these defines through the normal PEERDIR chain -- the
# same way gen/v1_antlr4 does -- avoids the fragile duplicated CFLAGS block.
PEERDIR(
    yql/essentials/parser/proto_ast/antlr4
    yql/essentials/public/issue/protos
)

END()
