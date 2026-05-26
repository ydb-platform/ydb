DECLARE_IN_DIRS(DOCS *.*m? DIRS docs RECURSIVE SRCDIR ${ARCADIA_ROOT}/ydb) # *.md or *.yaml
RUN_PROGRAM(
    ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate ${ARCADIA_ROOT}/ydb/docs/ ${BINDIR}/ydb_docs.archive
    IN ${DOCS_FILES}
    OUT_NOAUTO ${BINDIR}/ydb_docs.archive
)
RESOURCE(ydb_docs.archive ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate/docs.archive)
