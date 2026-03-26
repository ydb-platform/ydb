LIBRARY()

SRCS(
    manager.cpp
    events.cpp
    merge.cpp
    common.cpp
    private_events.cpp
    splitter.cpp
    context.cpp
    executor.cpp
    pk_fetcher.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
