LIBRARY()

SRCS(
    abstract.cpp
    result.cpp
    limit.cpp
    aggr.cpp
    distinct_limit.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/filter
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/reader/abstract
)

END()
