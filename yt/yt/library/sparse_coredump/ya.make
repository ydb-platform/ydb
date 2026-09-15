LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    sparse_coredump.cpp
)

PEERDIR(
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
