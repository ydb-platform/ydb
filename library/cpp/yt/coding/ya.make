LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
)

PEERDIR(
    library/cpp/yt/exception
)

END()

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmark
    )
ENDIF()
