UNITTEST_FOR(ydb/core/erasure)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

IF (WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/digest/crc32c
)

SRCS(
    erasure_ut.cpp
)

END()
