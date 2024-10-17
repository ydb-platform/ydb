LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    task.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/ss_tasks
)

END()
