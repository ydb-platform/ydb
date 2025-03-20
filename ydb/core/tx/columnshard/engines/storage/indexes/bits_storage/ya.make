LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL bitset.cpp
    GLOBAL string.cpp
)

PEERDIR(
    ydb/library/conclusion
    ydb/library/actors/core
    ydb/core/protos
)

END()
