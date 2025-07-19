UNION()

RUN_PROGRAM(
    yql/essentials/tools/yql_highlight --generate="json"
    STDOUT sql_highlighting.json
)

END()
