LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/tx/tiering/tier
)

END()
