IF (USE_PREBUILT_TOOLS)
    INCLUDE(ya.make.prebuilt)
ENDIF()

IF (NOT PREBUILT)
    INCLUDE(bin/ya.make)
ENDIF()

RECURSE(
    bin
)
