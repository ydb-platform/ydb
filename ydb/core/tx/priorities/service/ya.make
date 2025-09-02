LIBRARY()

SRCS(
    service.cpp
    manager.cpp
    counters.cpp
)

PEERDIR(
    ydb/core/tx/priorities/usage
    ydb/core/protos
)

END()
