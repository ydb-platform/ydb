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
    ydb/core/tx/conveyor/usage
    ydb/core/tx/limiter/grouped_memory/usage
    yql/essentials/minikql
)

END()
