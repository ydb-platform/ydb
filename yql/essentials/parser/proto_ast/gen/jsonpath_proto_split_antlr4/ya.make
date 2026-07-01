LIBRARY()

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/v4/tool/templates/codegen)
SET(jsonpath_grammar ${ARCADIA_ROOT}/yql/essentials/minikql/jsonpath/JsonPathAntlr4.g)

SET(ANTLR_PACKAGE_NAME NJsonPathGenerated)
SET(PARSER_AST_NAME JsonPathAntlr4Parser)
SET(PARSER_PROTO_NAME JsonPathAntlr4Parser)

CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Java/Java.stg.in ${antlr_templates}/Java/Java.stg)

RUN_ANTLR4(
    ${jsonpath_grammar}
    -lib .
    -no-listener
    -o ${antlr_output}
    -Dlanguage=Java
    IN ${jsonpath_grammar} ${antlr_templates}/Java/Java.stg
    OUT_NOAUTO JsonPathAntlr4Parser.proto
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
    JsonPathAntlr4Parser.proto
    IN JsonPathAntlr4Parser.proto
    TOOL contrib/tools/protoc/plugins/cpp_styleguide
    OUT_NOAUTO JsonPathAntlr4Parser.pb.h JsonPathAntlr4Parser.pb.cc
    CWD ${antlr_output}
)

RUN_PYTHON3(
    ${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/gen/multiproto.py JsonPathAntlr4Parser
    IN JsonPathAntlr4Parser.pb.h
    IN JsonPathAntlr4Parser.pb.cc
    OUT_NOAUTO
    JsonPathAntlr4Parser.pb.code0.cc
    JsonPathAntlr4Parser.pb.code1.cc
    JsonPathAntlr4Parser.pb.code2.cc
    JsonPathAntlr4Parser.pb.code3.cc
    JsonPathAntlr4Parser.pb.code4.cc
    JsonPathAntlr4Parser.pb.code5.cc
    JsonPathAntlr4Parser.pb.code6.cc
    JsonPathAntlr4Parser.pb.code7.cc
    JsonPathAntlr4Parser.pb.code8.cc
    JsonPathAntlr4Parser.pb.code9.cc
    JsonPathAntlr4Parser.pb.data.cc
    JsonPathAntlr4Parser.pb.classes.h
    JsonPathAntlr4Parser.pb.main.h
    CWD ${antlr_output}
)

SRCS(
    JsonPathAntlr4Parser.pb.code0.cc
    JsonPathAntlr4Parser.pb.code1.cc
    JsonPathAntlr4Parser.pb.code2.cc
    JsonPathAntlr4Parser.pb.code3.cc
    JsonPathAntlr4Parser.pb.code4.cc
    JsonPathAntlr4Parser.pb.code5.cc
    JsonPathAntlr4Parser.pb.code6.cc
    JsonPathAntlr4Parser.pb.code7.cc
    JsonPathAntlr4Parser.pb.code8.cc
    JsonPathAntlr4Parser.pb.code9.cc
    JsonPathAntlr4Parser.pb.data.cc
)

END()
