LIBRARY()

SRCS(
    manager.cpp
    events.cpp
    merge.cpp
    interval_borders.cpp
    common.cpp
    private_events.cpp
    splitter.cpp
    context.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
