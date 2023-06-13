LIBRARY()

SRCS(
    test_runtime.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect/mock
    library/cpp/actors/protos
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
