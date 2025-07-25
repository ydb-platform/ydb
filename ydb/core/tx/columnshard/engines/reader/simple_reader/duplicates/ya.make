RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    manager.cpp
    events.cpp
    merge.cpp
    common.cpp
    private_events.cpp
    splitter.cpp
    context.cpp
    filter_cache.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
