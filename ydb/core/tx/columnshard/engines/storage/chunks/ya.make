LIBRARY()

SRCS(
    data.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
    column.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
    chunked_array_serialized.cpp  # gn: into ydb/core/tx/columnshard/engines/portions
)

PEERDIR(
    ydb/core/tx/columnshard/splitter/abstract
    ydb/core/tx/columnshard/splitter
    ydb/core/tx/columnshard/engines/scheme/versions
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/counters
    ydb/library/formats/arrow/splitter
)

END()

RECURSE_FOR_TESTS(
    ut
)
