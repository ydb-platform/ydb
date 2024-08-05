LIBRARY()

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/v4/tool/templates/codegen)
SET(sql_grammar ${antlr_output}/SQLv4.g)

SET(ANTLR_PACKAGE_NAME NSQLv1Generated)

SET(GRAMMAR_STRING_CORE_SINGLE "\"~(['#BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE##BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE       "\".\"")

CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/v4/tool/templates/codegen/Java/Java.stg.in ${antlr_templates}/Java/Java.stg)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${sql_grammar})
ELSE()
    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/sql/v1/SQLv4.g.in ${sql_grammar})
ENDIF()

RUN_ANTLR4(
    ${sql_grammar}
    -lib .
    -no-listener
    -o ${antlr_output}
    -Dlanguage=Java
    IN ${sql_grammar} ${antlr_templates}/Java/Java.stg
    OUT_NOAUTO SQLv4Parser.proto
    CWD ${antlr_output}
)

IF (USE_VANILLA_PROTOC)
    SET(PROTOC_PATH contrib/tools/protoc_std)
ELSE()
    SET(PROTOC_PATH contrib/tools/protoc/bin)
ENDIF()


RUN_PROGRAM(
    $PROTOC_PATH -I=$CURDIR -I=$ARCADIA_ROOT -I=$ARCADIA_BUILD_ROOT -I=$ARCADIA_ROOT/contrib/libs/protobuf/src
    --cpp_out=$ARCADIA_BUILD_ROOT --cpp_styleguide_out=$ARCADIA_BUILD_ROOT
    --plugin=protoc-gen-cpp_styleguide=contrib/tools/protoc/plugins/cpp_styleguide
    SQLv4Parser.proto
    IN SQLv4Parser.proto
    TOOL contrib/tools/protoc/plugins/cpp_styleguide
    OUT_NOAUTO SQLv4Parser.pb.h SQLv4Parser.pb.cc
    CWD $ARCADIA_BUILD_ROOT
)

RUN_PYTHON3(
    ${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/gen/multiproto.py SQLv4Parser
    IN SQLv4Parser.pb.h
    IN SQLv4Parser.pb.cc
    OUT_NOAUTO 
    SQLv4Parser.pb.code0.cc
    SQLv4Parser.pb.code1.cc
    SQLv4Parser.pb.code2.cc
    SQLv4Parser.pb.code3.cc
    SQLv4Parser.pb.code4.cc
    SQLv4Parser.pb.code5.cc
    SQLv4Parser.pb.code6.cc
    SQLv4Parser.pb.code7.cc
    SQLv4Parser.pb.code8.cc
    SQLv4Parser.pb.code9.cc
    SQLv4Parser.pb.data.cc
    SQLv4Parser.pb.classes.h
    SQLv4Parser.pb.main.h
    CWD $ARCADIA_BUILD_ROOT/ydb/library/yql/parser/proto_ast/gen/v1_proto_split
)

PEERDIR(contrib/libs/protobuf)

SRCS(
    SQLv4Parser.pb.code0.cc
    SQLv4Parser.pb.code1.cc
    SQLv4Parser.pb.code2.cc
    SQLv4Parser.pb.code3.cc
    SQLv4Parser.pb.code4.cc
    SQLv4Parser.pb.code5.cc
    SQLv4Parser.pb.code6.cc
    SQLv4Parser.pb.code7.cc
    SQLv4Parser.pb.code8.cc
    SQLv4Parser.pb.code9.cc
    SQLv4Parser.pb.data.cc
)

END()
