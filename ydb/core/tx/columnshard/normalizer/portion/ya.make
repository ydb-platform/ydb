LIBRARY()

SRCS(
    normalizer.cpp
    GLOBAL portion.cpp
    GLOBAL chunks.cpp
    GLOBAL clean.cpp
    GLOBAL clean_empty.cpp
    GLOBAL broken_blobs.cpp
    GLOBAL special_cleaner.cpp
    GLOBAL chunks_actualization.cpp
    GLOBAL restore_portion_from_chunks.cpp
    GLOBAL restore_v1_chunks.cpp
    GLOBAL restore_v2_chunks.cpp
    GLOBAL snapshot_from_chunks.cpp
    GLOBAL leaked_blobs.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
    ydb/core/tx/columnshard/blobs_reader
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/conveyor/usage
)

END()
