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
    yql/essentials/tools/yql_highlight --generate="vim"
    STDOUT yql_new.vim
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="tmbundle" --output="${BINDIR}/YQL.tmbundle.zip"
    OUT ${BINDIR}/YQL.tmbundle.zip
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="tmbundle" --output="${BINDIR}/YQL.tmbundle.tar"
    OUT ${BINDIR}/YQL.tmbundle.tar
)

END()
