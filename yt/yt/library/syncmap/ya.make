LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

END()

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmark
    )
ENDIF()
