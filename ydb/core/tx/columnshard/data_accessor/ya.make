LIBRARY()

SRCS(
    actor.cpp
    events.cpp
    controller.cpp
    request.cpp
    manager.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/tx/columnshard/engines/portions
)

END()
