LIBRARY()

SET(ANTLR_OUTPUT ${ARCADIA_BUILD_ROOT}/${MODDIR})
SET(ANTLR_TEMPLATES ${ANTLR_OUTPUT}/org/antlr/v4/tool/templates/codegen)

SET(LANGUAGE_NAME SQLv1Antlr4)
SET(GEN_NAMESPACE "NYdb::NConsoleClient")

SET(YQL_GRAMMAR ${ANTLR_OUTPUT}/${LANGUAGE_NAME}.g)

SET(GRAMMAR_STRING_CORE_SINGLE "\"~(['#BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE##BACKSLASH#]) | (BACKSLASH .)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE "\".\"")

COPY_FILE(
    ydb/public/lib/ydb_cli/commands/interactive/gen/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
    ${ANTLR_TEMPLATES}/Cpp/Cpp.stg
)

COPY_FILE(
    ydb/public/lib/ydb_cli/commands/interactive/gen/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
    ${ANTLR_TEMPLATES}/Cpp/Files.stg
)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${YQL_GRAMMAR})
ELSE()
    CONFIGURE_FILE(${ARCADIA_ROOT}/ydb/library/yql/sql/v1/${LANGUAGE_NAME}.g.in ${YQL_GRAMMAR})
ENDIF()

RUN_ANTLR4(
    ${YQL_GRAMMAR}

    -no-listener
    -no-visitor
    -package ${GEN_NAMESPACE}
    -lib .
    -o ${ANTLR_OUTPUT}

    IN ${YQL_GRAMMAR} 
       ${ANTLR_TEMPLATES}/Cpp/Cpp.stg 
       ${ANTLR_TEMPLATES}/Cpp/Files.stg

    OUT ${LANGUAGE_NAME}Parser.cpp ${LANGUAGE_NAME}Lexer.cpp 
        ${LANGUAGE_NAME}Parser.h   ${LANGUAGE_NAME}Lexer.h

    CWD ${ANTLR_OUTPUT}
)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
)

END()