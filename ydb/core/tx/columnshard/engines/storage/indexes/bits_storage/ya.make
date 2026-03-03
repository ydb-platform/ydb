LIBRARY()

SRCS(
    abstract.cpp
    bitset.cpp
    string.cpp
)

PEERDIR(
    ydb/library/conclusion
    ydb/library/actors/core
    ydb/core/protos
)

END()
