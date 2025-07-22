UNION()

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="json"
    STDOUT sql_highlighting.json
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="textmate"
    STDOUT yql.tmLanguage.json
)

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="vim"
    STDOUT yql_test.vim
)

END()
