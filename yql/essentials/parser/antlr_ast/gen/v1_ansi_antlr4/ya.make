LIBRARY()

SET(SQL_GRAMMAR ${BINDIR}/SQLv1Antlr4.g4)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${SQL_GRAMMAR})
ELSE()
    SET(GRAMMAR_STRING_CORE_SINGLE "\"~([']) | (QUOTE_SINGLE QUOTE_SINGLE)\"")
    SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE#]) | (QUOTE_DOUBLE QUOTE_DOUBLE)\"")
    SET(GRAMMAR_MULTILINE_COMMENT_CORE "\"MULTILINE_COMMENT | .\"")
    
    CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/sql/v1/SQLv1Antlr4.g.in ${SQL_GRAMMAR})
ENDIF()

COPY_FILE(
    ${ARCADIA_ROOT}/yql/essentials/parser/antlr_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
    ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
)

COPY_FILE(
    ${ARCADIA_ROOT}/yql/essentials/parser/antlr_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
    ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
)

RUN_ANTLR4_CPP(
    ${SQL_GRAMMAR}
    -package NALAAnsiAntlr4
    -lib ${BINDIR}
    IN
        ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
        ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
)

END()
