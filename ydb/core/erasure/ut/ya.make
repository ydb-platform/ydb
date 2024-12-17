UNITTEST_FOR(ydb/core/erasure)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/digest/crc32c
)

SET(_YASM_PREDEFINED_FLAGS_VALUE "")

SRCS(
    erasure_ut.cpp
    erasure_new_ut.cpp
)

END()
