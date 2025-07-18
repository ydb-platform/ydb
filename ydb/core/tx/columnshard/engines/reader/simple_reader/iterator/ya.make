LIBRARY()

SRCS(
    scanner.cpp
    source.cpp
    fetched_data.cpp
    plain_read_data.cpp
    context.cpp
    fetching.cpp
    iterator.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/collections
    ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sync_points
    ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view
    ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates
    ydb/core/tx/conveyor/usage
    ydb/core/tx/limiter/grouped_memory/usage
)

END()
