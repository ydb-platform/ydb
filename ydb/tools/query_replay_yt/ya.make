PROGRAM(query_replay_yt)

OWNER(
    shumkovnd
    g:kikimr
)

ALLOCATOR(LF)

YQL_LAST_ABI_VERSION()
INCLUDE(${ARCADIA_ROOT}/ydb/tools/query_replay_yt/common_deps.inc)

SRCS(${YDB_REPLAY_SRCS})

PEERDIR(${YDB_REPLAY_PEERDIRS})

END()
