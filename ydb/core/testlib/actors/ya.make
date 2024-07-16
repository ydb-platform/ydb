LIBRARY()

SRCS(
    test_runtime.cpp
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
