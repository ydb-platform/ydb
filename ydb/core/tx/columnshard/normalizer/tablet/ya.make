LIBRARY()

SRCS(
    GLOBAL gc_counters.cpp
    GLOBAL broken_txs.cpp
    GLOBAL broken_insertion_dedup.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/normalizer/abstract
)

END()
