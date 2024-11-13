PROTO_LIBRARY()

IF (GEN_PROTO)
    SET(antlr_output ${ARCADIA_BUILD_ROOT}/${MODDIR})
    SET(antlr_templates ${antlr_output}/org/antlr/codegen/templates)
    SET(jsonpath_grammar ${ARCADIA_ROOT}/yql/essentials/minikql/jsonpath/JsonPath.g)

    SET(ANTLR_PACKAGE_NAME NJsonPathGenerated)
    SET(PROTOBUF_HEADER_PATH ${MODDIR})
    SET(PROTOBUF_SUFFIX_PATH .pb.h)
    SET(LEXER_PARSER_NAMESPACE NALP)

    CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/codegen/templates/Cpp/Cpp.stg.in ${antlr_templates}/Cpp/Cpp.stg)
    CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/codegen/templates/protobuf/protobuf.stg.in ${antlr_templates}/protobuf/protobuf.stg)

    RUN_ANTLR(
        ${jsonpath_grammar}
        -lib .
        -fo ${antlr_output}
        -language protobuf
        IN ${jsonpath_grammar} ${antlr_templates}/protobuf/protobuf.stg
        OUT_NOAUTO JsonPathParser.proto
        CWD ${antlr_output}
    )

    NO_COMPILER_WARNINGS()

    ADDINCL(
        # TODO Please check RUN_ANTLR with version 3, but ADDINCL for version 4
        GLOBAL contrib/libs/antlr4_cpp_runtime/src
    )

    INCLUDE(${ARCADIA_ROOT}/yql/essentials/parser/proto_ast/org/antlr/codegen/templates/ya.make.incl)

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

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()
