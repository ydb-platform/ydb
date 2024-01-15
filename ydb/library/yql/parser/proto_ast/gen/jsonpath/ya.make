PROTO_LIBRARY()

IF (CPP_PROTO)
    SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
    SET(antlr_templates ${antlr_output}/org/antlr/codegen/templates)
    SET(jsonpath_grammar ${ARCADIA_ROOT}/ydb/library/yql/minikql/jsonpath/JsonPath.g)

    SET(ANTLR_PACKAGE_NAME NJsonPathGenerated)
    SET(PROTOBUF_HEADER_PATH ${MODDIR})
    SET(PROTOBUF_SUFFIX_PATH .pb.h)
    SET(LEXER_PARSER_NAMESPACE NALP)


    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)
    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/protobuf/protobuf.stg.in ${antlr_templates}/protobuf/protobuf.stg)

    RUN_ANTLR(
        ${jsonpath_grammar}
        -lib .
        -fo ${antlr_output}
        -language protobuf
        IN ${jsonpath_grammar} ${antlr_templates}/protobuf/protobuf.stg
        OUT_NOAUTO JsonPathParser.proto
        CWD ${antlr_output}
    )

    EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

    NO_COMPILER_WARNINGS()

    INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/parser/proto_ast/org/antlr/codegen/templates/ya.make.incl)

    RUN_ANTLR(
        ${jsonpath_grammar}
        -lib .
        -fo ${antlr_output}
        IN ${jsonpath_grammar} ${antlr_templates}/Cpp/Cpp.stg
        OUT JsonPathParser.cpp JsonPathLexer.cpp JsonPathParser.h JsonPathLexer.h
        OUTPUT_INCLUDES
        JsonPathParser.pb.h
        ${STG_INCLUDES}
        CWD ${antlr_output}
    )
ENDIF()

SRCS(JsonPathParser.proto)


END()
