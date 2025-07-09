LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    procfs.cpp
)

PEERDIR(
    library/cpp/yt/error
    util
)

END()

RECURSE_FOR_TESTS(
    unittests
)
