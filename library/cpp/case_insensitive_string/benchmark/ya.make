G_BENCHMARK()

SIZE(MEDIUM)

IF (NOT AUTOCHECK)
    CFLAGS(-DBENCHMARK_ALL_IMPLS)
ENDIF()

SRCS(
    compare.cpp
    hash.cpp
)

PEERDIR(
    library/cpp/case_insensitive_string
    library/cpp/case_insensitive_string/ut_gtest/util
    library/cpp/digest/murmur
)

END()
