LIBRARY()

SRCS(
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/trivial_reader/constructor
    ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator
)

END()

RECURSE_FOR_TESTS(
    duplicates
)
