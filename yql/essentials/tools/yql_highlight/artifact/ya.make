UNION()

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="json"
    STDOUT sql_highlighting.json
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="tmlanguage"
    STDOUT YQL.tmLanguage.json
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --language="yqls" --generate="tmlanguage"
    STDOUT YQLs.tmLanguage.json
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --language="yql" --generate="monarch"
    STDOUT YQL.monarch.json
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --language="yqls" --generate="monarch"
    STDOUT YQLs.monarch.json
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="vim"
    STDOUT yql_new.vim
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --language="yqls" --generate="vim"
    STDOUT yqls.vim
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="tmbundle" --output="${BINDIR}/YQL.tmbundle.zip"
    OUT ${BINDIR}/YQL.tmbundle.zip
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight -l "yqls" -g "tmbundle" -o "${BINDIR}/YQLs.tmbundle.zip"
    OUT ${BINDIR}/YQLs.tmbundle.zip
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="tmbundle" --output="${BINDIR}/YQL.tmbundle.tar"
    OUT ${BINDIR}/YQL.tmbundle.tar
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight -l "yqls" -g "tmbundle" -o "${BINDIR}/YQLs.tmbundle.tar"
    OUT ${BINDIR}/YQLs.tmbundle.tar
)

END()
