LIBRARY()

OWNER(g:yql g:yql_ydb_core)

PEERDIR(
    ydb/library/yql/public/issue
    ydb/library/yql/parser/proto_ast
)

SRCS(
    collect_issues.h
)

END()
