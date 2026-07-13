LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/exception
    library/cpp/yt/memory
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
