UNITTEST_FOR(ydb/core/erasure)

FORK_SUBTESTS()

SPLIT_FACTOR(30)

IF (WITH_VALGRIND OR SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/digest/crc32c
    ydb/core/erasure
)

SRCS(
    erasure_rope_ut.cpp
)

END()
