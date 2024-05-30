LIBRARY()

SRCS(
    common.cpp
    interface.cpp
    scheduler.cpp
    activity.cpp
    task.cpp
    state.cpp
)

PEERDIR(
    ydb/library/accessor
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/services/bg_tasks/protos
    ydb/library/conclusion
    ydb/core/base
)

END()
