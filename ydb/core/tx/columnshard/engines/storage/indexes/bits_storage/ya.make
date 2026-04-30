LIBRARY()

SRCS(
    abstract.cpp
    array_power2.cpp
    GLOBAL bitset.cpp
    GLOBAL fix_string.cpp
)

PEERDIR(
    ydb/library/conclusion
    ydb/library/actors/core
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
