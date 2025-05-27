LIBRARY()

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/v4/tool/templates/codegen)
SET(sql_grammar ${antlr_output}/SQLv1Antlr4.g)

SET(ANTLR_PACKAGE_NAME NSQLv1Generated)

CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Java/Java.stg.in ${antlr_templates}/Java/Java.stg)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${sql_grammar})
ELSE()
    # For exporting CMake this vars fill in epilogue.cmake
    SET(GRAMMAR_STRING_CORE_SINGLE "\"~(['#BACKSLASH#]) | (BACKSLASH .)\"")
    SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE##BACKSLASH#]) | (BACKSLASH .)\"")
    SET(GRAMMAR_MULTILINE_COMMENT_CORE "\".\"")

    CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/sql/v1/SQLv1Antlr4.g.in ${sql_grammar})
ENDIF()

RUN_ANTLR4(
    ${sql_grammar}
    -lib .
    -no-listener
    -o ${antlr_output}
    -Dlanguage=Java
    IN ${sql_grammar} ${antlr_templates}/Java/Java.stg
    OUT_NOAUTO SQLv1Antlr4Parser.proto
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
    SQLv1Antlr4Parser.proto
    IN SQLv1Antlr4Parser.proto
    TOOL contrib/tools/protoc/plugins/cpp_styleguide
    OUT_NOAUTO SQLv1Antlr4Parser.pb.h SQLv1Antlr4Parser.pb.cc
    CWD ${antlr_output}
)

RUN_PYTHON3(
    ${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/gen/multiproto.py SQLv1Antlr4Parser
    IN SQLv1Antlr4Parser.pb.h
    IN SQLv1Antlr4Parser.pb.cc
    OUT_NOAUTO
    SQLv1Antlr4Parser.pb.code0.cc
    SQLv1Antlr4Parser.pb.code1.cc
    SQLv1Antlr4Parser.pb.code2.cc
    SQLv1Antlr4Parser.pb.code3.cc
    SQLv1Antlr4Parser.pb.code4.cc
    SQLv1Antlr4Parser.pb.code5.cc
    SQLv1Antlr4Parser.pb.code6.cc
    SQLv1Antlr4Parser.pb.code7.cc
    SQLv1Antlr4Parser.pb.code8.cc
    SQLv1Antlr4Parser.pb.code9.cc
    SQLv1Antlr4Parser.pb.data.cc
    SQLv1Antlr4Parser.pb.classes.h
    SQLv1Antlr4Parser.pb.main.h
    CWD ${antlr_output}
)

SRCS(
    SQLv1Antlr4Parser.pb.code0.cc
    SQLv1Antlr4Parser.pb.code1.cc
    SQLv1Antlr4Parser.pb.code2.cc
    SQLv1Antlr4Parser.pb.code3.cc
    SQLv1Antlr4Parser.pb.code4.cc
    SQLv1Antlr4Parser.pb.code5.cc
    SQLv1Antlr4Parser.pb.code6.cc
    SQLv1Antlr4Parser.pb.code7.cc
    SQLv1Antlr4Parser.pb.code8.cc
    SQLv1Antlr4Parser.pb.code9.cc
    SQLv1Antlr4Parser.pb.data.cc
)

END()
