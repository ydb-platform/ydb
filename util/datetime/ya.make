IF (NOT OS_EMSCRIPTEN)
    RECURSE(
        benchmark
    )
ENDIF()

RECURSE_FOR_TESTS(
    ut
)
