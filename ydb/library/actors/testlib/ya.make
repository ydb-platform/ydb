LIBRARY()

SRCS(
    test_runtime.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect/mock
    ydb/library/actors/protos
    ydb/library/actors/testlib/common
    library/cpp/random_provider
    library/cpp/time_provider
)

IF (GCC)
    CFLAGS(-fno-devirtualize-speculatively)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
