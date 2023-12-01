SET(IDE_FOLDER "_Builders")

IF (USE_PREBUILT_TOOLS)
    INCLUDE(${ARCADIA_ROOT}/build/prebuilt/tools/event2cpp/ya.make.prebuilt)
ENDIF()

IF (NOT PREBUILT)
    INCLUDE(${ARCADIA_ROOT}/tools/event2cpp/bin/ya.make)
ENDIF()

RECURSE(
    bin
)
