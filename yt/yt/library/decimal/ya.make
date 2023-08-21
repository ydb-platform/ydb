LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    decimal.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/int128
)

END()

RECURSE_FOR_TESTS(
    unittests
)
