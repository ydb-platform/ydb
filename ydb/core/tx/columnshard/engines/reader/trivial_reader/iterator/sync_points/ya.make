LIBRARY()

SRCS(
    abstract.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    result.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    limit.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    aggr.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    distinct_limit.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/filter
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/reader/abstract
)

END()
