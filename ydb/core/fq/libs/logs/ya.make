LIBRARY()

SRCS(
    log.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/protos
    ydb/library/yql/utils/actor_log
)

YQL_LAST_ABI_VERSION()

END()
