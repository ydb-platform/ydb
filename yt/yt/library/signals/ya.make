LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    signal_registry.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/error
    library/cpp/yt/misc
    library/cpp/yt/system
    util
    yt/yt/library/procfs
)

END()

RECURSE_FOR_TESTS(
    unittests
)
