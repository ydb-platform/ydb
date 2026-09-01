LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/string
)

END()

RECURSE_FOR_TESTS(
    unittests
)
