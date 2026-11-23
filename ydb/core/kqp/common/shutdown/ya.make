LIBRARY()

SRCS(
    controller.cpp
    state.cpp
    events.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/actors/core
)

END()
