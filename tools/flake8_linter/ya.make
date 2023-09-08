IF (USE_PREBUILT_TOOLS OR OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/build/prebuilt/tools/flake8_linter/ya.make.prebuilt)
ENDIF()

IF (NOT PREBUILT)
    INCLUDE(${ARCADIA_ROOT}/tools/flake8_linter/bin/ya.make)
ENDIF()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )

    RECURSE_FOR_TESTS(
        bin/tests
    )
ENDIF()
