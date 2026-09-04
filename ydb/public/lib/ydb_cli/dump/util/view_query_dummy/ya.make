LIBRARY()

PROVIDES(
    ydb_cli_dump_view_query
)

SRCS(
    view_query_dummy.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_iface
    yql/essentials/public/issue
)

END()
