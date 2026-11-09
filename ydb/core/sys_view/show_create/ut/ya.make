UNITTEST_FOR(ydb/core/sys_view/show_create)

SRCS(
    create_table_formatter_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util
)

YQL_LAST_ABI_VERSION()

END()
