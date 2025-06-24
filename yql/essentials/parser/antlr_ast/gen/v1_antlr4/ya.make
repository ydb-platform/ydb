LIBRARY()

SET(SQL_GRAMMAR ${ARCADIA_BUILD_ROOT}/${MODDIR}/SQLv1Antlr4.g)

IF(EXPORT_CMAKE)
    MANUAL_GENERATION(${SQL_GRAMMAR})
ELSE()
    SET(GRAMMAR_STRING_CORE_SINGLE "\"~(['#BACKSLASH#]) | (BACKSLASH .)\"")
    SET(GRAMMAR_STRING_CORE_DOUBLE "\"~([#DOUBLE_QUOTE##BACKSLASH#]) | (BACKSLASH .)\"")
    SET(GRAMMAR_MULTILINE_COMMENT_CORE "\".\"")
    
    CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/sql/v1/SQLv1Antlr4.g.in ${SQL_GRAMMAR})
ENDIF()

COPY_FILE(
    ${ARCADIA_ROOT}/yql/essentials/parser/antlr_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
    ${ARCADIA_BUILD_ROOT}/${MODDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
)

COPY_FILE(
    ${ARCADIA_ROOT}/yql/essentials/parser/antlr_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
    ${ARCADIA_BUILD_ROOT}/${MODDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
)

RUN_ANTLR4(
    ${SQL_GRAMMAR}
    -no-listener
    -visitor
    -package NALADefaultAntlr4
    -lib .
    IN 
        ${SQL_GRAMMAR} 
        ${ARCADIA_BUILD_ROOT}/${MODDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
        ${ARCADIA_BUILD_ROOT}/${MODDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
    OUT SQLv1Antlr4Parser.cpp SQLv1Antlr4Parser.h
        SQLv1Antlr4Lexer.cpp SQLv1Antlr4Lexer.h
        SQLv1Antlr4Visitor.cpp SQLv1Antlr4Visitor.h 
        SQLv1Antlr4BaseVisitor.cpp SQLv1Antlr4BaseVisitor.h
    OUTPUT_INCLUDES contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h
    CWD ${ARCADIA_BUILD_ROOT}/${MODDIR}
)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
)

END()
