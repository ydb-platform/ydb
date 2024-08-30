LIBRARY()

SRCS(
    block_events.cpp
    block_events.h
    test_runtime.cpp
    test_runtime.h
)

PEERDIR(
    ydb/apps/version
    ydb/library/actors/testlib
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/mon
    ydb/core/mon_alloc
    ydb/core/scheme
    ydb/core/tablet
)

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
