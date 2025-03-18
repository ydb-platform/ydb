LIBRARY()

SRCS(
    manager.cpp
    events.cpp
    merge.cpp
    subscriber.cpp
    fetching.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
