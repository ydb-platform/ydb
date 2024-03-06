LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    sensors_owner.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/profiling
)

END()

RECURSE_FOR_TESTS(unittests)
