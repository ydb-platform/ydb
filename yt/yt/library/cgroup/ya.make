LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    process.cpp
    statistics.cpp
)

PEERDIR(
    library/cpp/yt/logging
    library/cpp/yt/error
)

END()

RECURSE_FOR_TESTS(
    unittests
)
