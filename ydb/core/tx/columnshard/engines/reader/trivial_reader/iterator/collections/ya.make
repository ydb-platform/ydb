LIBRARY()

SRCS(
    abstract.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    constructors.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    unordered_result.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    ordered_result_no_limit.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
    ordered_result_with_limit.cpp  # gn: into ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/tx/columnshard/engines/predicate
)

END()
