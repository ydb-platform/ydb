LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/misc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
