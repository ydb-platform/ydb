PROTO_LIBRARY()

IF (GEN_PROTO)
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
ENDIF()

SRCS(SQLv1Parser.proto)

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()
