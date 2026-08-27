LIBRARY(query_replay_yt_lib)

YQL_LAST_ABI_VERSION()
INCLUDE(${ARCADIA_ROOT}/ydb/tools/query_replay_yt/common_deps.inc)

SRCS(
    ${YDB_REPLAY_LIB_SRCS}
    ../plan_check.h
    ../metadata.h
    ../replay_runner.h
    ../query_replay.h
)

PEERDIR(${YDB_REPLAY_PEERDIRS})

END()

RECURSE_FOR_TESTS(
    ../ut
)
