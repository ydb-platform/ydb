RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    manager.cpp
    events.cpp
    merge.cpp
    common.cpp
    subscriber.cpp
    fetching.cpp
    interval_index.cpp
    interval_counter.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
