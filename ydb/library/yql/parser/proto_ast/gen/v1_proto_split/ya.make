LIBRARY()

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/codegen/templates)
SET(sql_grammar ${antlr_output}/SQLv1.g)

SET(ANTLR_PACKAGE_NAME NSQLv1Generated)

SET(GRAMMAR_STRING_CORE_SINGLE "\"~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~(QUOTE_DOUBLE | BACKSLASH) | (BACKSLASH .)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE       "\".\"")

CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/protobuf/protobuf.stg.in ${antlr_templates}/protobuf/protobuf.stg)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${sql_grammar})
ELSE()
    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/sql/v1/SQLv1.g.in ${sql_grammar})
ENDIF()

RUN_ANTLR(
    ${sql_grammar}
    -lib .
    -fo ${antlr_output}
    -language protobuf
    IN ${sql_grammar} ${antlr_templates}/protobuf/protobuf.stg
    OUT_NOAUTO SQLv1Parser.proto
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
    SQLv1Parser.proto
    IN SQLv1Parser.proto
    TOOL contrib/tools/protoc/plugins/cpp_styleguide
    OUT_NOAUTO SQLv1Parser.pb.h SQLv1Parser.pb.cc
    CWD $ARCADIA_BUILD_ROOT
)

RUN_PYTHON3(
    ${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/gen/multiproto.py SQLv1Parser
    IN SQLv1Parser.pb.h
    IN SQLv1Parser.pb.cc
    OUT_NOAUTO 
    SQLv1Parser.pb.code0.cc
    SQLv1Parser.pb.code1.cc
    SQLv1Parser.pb.code2.cc
    SQLv1Parser.pb.code3.cc
    SQLv1Parser.pb.code4.cc
    SQLv1Parser.pb.code5.cc
    SQLv1Parser.pb.code6.cc
    SQLv1Parser.pb.code7.cc
    SQLv1Parser.pb.code8.cc
    SQLv1Parser.pb.code9.cc
    SQLv1Parser.pb.data.cc
    SQLv1Parser.pb.classes.h
    SQLv1Parser.pb.main.h
    CWD $ARCADIA_BUILD_ROOT/ydb/library/yql/parser/proto_ast/gen/v1_proto_split
)

PEERDIR(contrib/libs/protobuf)

SRCS(
    SQLv1Parser.pb.code0.cc
    SQLv1Parser.pb.code1.cc
    SQLv1Parser.pb.code2.cc
    SQLv1Parser.pb.code3.cc
    SQLv1Parser.pb.code4.cc
    SQLv1Parser.pb.code5.cc
    SQLv1Parser.pb.code6.cc
    SQLv1Parser.pb.code7.cc
    SQLv1Parser.pb.code8.cc
    SQLv1Parser.pb.code9.cc
    SQLv1Parser.pb.data.cc
)

END()
