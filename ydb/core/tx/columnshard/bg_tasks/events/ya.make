LIBRARY()

SRCS(
    events.cpp
    local.cpp
    global.cpp
    common.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/columnshard/bg_tasks/protos
)

END()
