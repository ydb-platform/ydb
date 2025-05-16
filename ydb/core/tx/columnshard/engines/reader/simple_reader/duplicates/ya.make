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
    interval_tree.cpp
    source_cache.cpp
    splitter.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
