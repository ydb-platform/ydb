PROGRAM(ydb_query_replay)

ALLOCATOR(LF)

YQL_LAST_ABI_VERSION()
INCLUDE(${ARCADIA_ROOT}/ydb/tools/query_replay/common_deps.inc)

SRCS(${YDB_REPLAY_SRCS})

PEERDIR(
    ${YDB_REPLAY_PEERDIRS}
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    yql/essentials/sql/v1
)

END()
