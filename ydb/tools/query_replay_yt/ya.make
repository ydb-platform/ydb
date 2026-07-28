PROGRAM(query_replay_yt)

ALLOCATOR(LF)

YQL_LAST_ABI_VERSION()
INCLUDE(${ARCADIA_ROOT}/ydb/tools/query_replay_yt/common_deps.inc)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/tools/query_replay_yt/lib
)

END()
