LIBRARY()

SRCS(
    constructors.cpp
    scanner.cpp
    source.cpp
    interval.cpp
    fetched_data.cpp
    plain_read_data.cpp
    merge.cpp
    context.cpp
    fetching.cpp
    iterator.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/hash
    ydb/core/formats/arrow/program
    ydb/core/formats/arrow/reader
    ydb/core/formats/arrow/serializer
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/conveyor/usage
    ydb/core/tx/limiter/grouped_memory/usage
)

END()
