LIBRARY()

SRCS(
    normalizer.cpp
    GLOBAL portion.cpp
    GLOBAL chunks.cpp
    GLOBAL clean.cpp
    GLOBAL clean_empty.cpp
    GLOBAL broken_blobs.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
    ydb/core/tx/columnshard/blobs_reader
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/conveyor/usage
)

END()
