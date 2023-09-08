IF (USE_PREBUILT_TOOLS OR OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/build/prebuilt/tools/black_linter/ya.make.prebuilt)
ENDIF()

IF (NOT PREBUILT)
    INCLUDE(${ARCADIA_ROOT}/tools/black_linter/bin/ya.make)
ENDIF()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()
