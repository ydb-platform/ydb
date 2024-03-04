LIBRARY()

SRCS(
    normalizer.cpp
    min_max.cpp
    chunks.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
    ydb/core/tx/columnshard/blobs_reader
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/conveyor/usage
)

END()
