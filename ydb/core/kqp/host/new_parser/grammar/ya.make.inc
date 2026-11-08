# Generate native ANTLR context classes from the existing YQL grammar.
SET(SQL_GRAMMAR ${BINDIR}/SQLv1Antlr4.g4)
CONFIGURE_FILE(${ARCADIA_ROOT}/yql/essentials/sql/v1/SQLv1Antlr4.g.in ${SQL_GRAMMAR})

COPY_FILE(
    ${ARCADIA_ROOT}/yql/essentials/parser/antlr_ast/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
    ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
)
COPY_FILE(
    ${ARCADIA_ROOT}/yql/essentials/parser/antlr_ast/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
    ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
)

# The runtime exports a thread-local-cache define via GLOBAL CFLAGS. Local
# CXXFLAGS follow those flags, selecting call_once and process-wide DFA caches
# for these generated libraries only. Do not change the shared parser targets.
CXXFLAGS(-UANTLR4_USE_THREAD_LOCAL_CACHE -DANTLR4_USE_THREAD_LOCAL_CACHE=0)

# Use the repository C++ templates copied above, including their cache
# initialization support. The grammar is shared with SQL v1; only lexer
# configuration and generated C++ namespace differ between the two callers.
RUN_ANTLR4_CPP(
    ${SQL_GRAMMAR}
    VISITOR
    -package ${KQP_PARSER_NAMESPACE}
    -lib ${BINDIR}
    IN
        ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Cpp.stg
        ${BINDIR}/org/antlr/v4/tool/templates/codegen/Cpp/Files.stg
)
