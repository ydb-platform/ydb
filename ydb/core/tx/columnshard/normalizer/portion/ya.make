LIBRARY()

SRCS(
    normalizer.cpp
    GLOBAL portion.cpp
    GLOBAL chunks.cpp
    GLOBAL clean.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
    ydb/core/tx/columnshard/blobs_reader
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/conveyor/usage
)

END()
