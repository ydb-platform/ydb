LIBRARY()

SRCS(
    manager.cpp
    events.cpp
    common.cpp
    private_events.cpp
    context.cpp
    executor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
