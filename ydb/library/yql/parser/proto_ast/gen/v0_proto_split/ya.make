LIBRARY()

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/codegen/templates)
SET(sql_grammar ${ARCADIA_ROOT}/ydb/library/yql/sql/v0/SQL.g)

SET(ANTLR_PACKAGE_NAME NSQLGenerated)

CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/protobuf/protobuf.stg.in ${antlr_templates}/protobuf/protobuf.stg)

RUN_ANTLR(
    ${sql_grammar}
    -lib .
    -fo ${antlr_output}
    -language protobuf
    IN ${sql_grammar} ${antlr_templates}/protobuf/protobuf.stg
    OUT_NOAUTO SQLParser.proto
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
    SQLParser.proto
    IN SQLParser.proto
    TOOL contrib/tools/protoc/plugins/cpp_styleguide
    OUT_NOAUTO SQLParser.pb.h SQLParser.pb.cc
    CWD $ARCADIA_BUILD_ROOT
)

RUN_PYTHON3(
    ${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/gen/multiproto.py SQLParser
    IN SQLParser.pb.h
    IN SQLParser.pb.cc
    OUT_NOAUTO
    SQLParser.pb.code0.cc
    SQLParser.pb.code1.cc
    SQLParser.pb.code2.cc
    SQLParser.pb.code3.cc
    SQLParser.pb.code4.cc
    SQLParser.pb.code5.cc
    SQLParser.pb.code6.cc
    SQLParser.pb.code7.cc
    SQLParser.pb.code8.cc
    SQLParser.pb.code9.cc
    SQLParser.pb.data.cc
    SQLParser.pb.classes.h
    SQLParser.pb.main.h
    CWD $ARCADIA_BUILD_ROOT/ydb/library/yql/parser/proto_ast/gen/v0_proto_split
)

PEERDIR(contrib/libs/protobuf)

SRCS(
    SQLParser.pb.code0.cc
    SQLParser.pb.code1.cc
    SQLParser.pb.code2.cc
    SQLParser.pb.code3.cc
    SQLParser.pb.code4.cc
    SQLParser.pb.code5.cc
    SQLParser.pb.code6.cc
    SQLParser.pb.code7.cc
    SQLParser.pb.code8.cc
    SQLParser.pb.code9.cc
    SQLParser.pb.data.cc
)

END()

