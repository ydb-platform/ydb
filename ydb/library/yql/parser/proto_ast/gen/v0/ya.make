LIBRARY()

PEERDIR (
    ydb/library/yql/parser/proto_ast/gen/v0_proto_split
)

SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(antlr_templates ${antlr_output}/org/antlr/codegen/templates)
SET(sql_grammar ${ARCADIA_ROOT}/ydb/library/yql/sql/v0/SQL.g)

SET(ANTLR_PACKAGE_NAME NSQLGenerated)
SET(PROTOBUF_HEADER_PATH ydb/library/yql/parser/proto_ast/gen/v0_proto_split)
SET(PROTOBUF_SUFFIX_PATH .pb.main.h)
SET(LEXER_PARSER_NAMESPACE NALP)


CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)

NO_COMPILER_WARNINGS()

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/ya.make.incl)

RUN_ANTLR(
    ${sql_grammar}
    -lib .
    -fo ${antlr_output}
    IN ${sql_grammar} ${antlr_templates}/Cpp/Cpp.stg
    OUT SQLParser.cpp SQLLexer.cpp SQLParser.h SQLLexer.h
    OUTPUT_INCLUDES
    ${PROTOBUF_HEADER_PATH}/SQLParser.pb.main.h
    ${STG_INCLUDES}
    CWD ${antlr_output}
)

END()

