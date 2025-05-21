LIBRARY()

SRCS(
    manager.cpp
    events.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
