LIBRARY()

SRCS(
    GLOBAL broken_dedup.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
)

END()
