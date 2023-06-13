LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    clock.cpp
)

PEERDIR(
    library/cpp/yt/assert
)

END()

RECURSE(
    benchmark
)

RECURSE_FOR_TESTS(
    unittests
)
