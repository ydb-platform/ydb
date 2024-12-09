UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/tx/datashard/ut_common
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_ext_blobs_multiple_channels.cpp
)

END()
