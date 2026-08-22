IF (USE_PREBUILT_TOOLS AND NOT USE_PYTHON3_PREV)
    INCLUDE(ya.make.prebuilt)
ENDIF()

IF (NOT PREBUILT)
    INCLUDE(bin/ya.make)
ENDIF()

RECURSE(
    bin
    slow
)
