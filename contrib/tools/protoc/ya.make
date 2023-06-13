# WARN:
#   The Piglet sync service (abc:cc-piglet) relies on prebuiltness of protoc.
#   DO NOT REMOVE ya.make.prebuilt.

IF (USE_PREBUILT_TOOLS)
    INCLUDE(ya.make.prebuilt)
ENDIF()

IF (NOT PREBUILT)
    INCLUDE(bin/ya.make)
ENDIF()

RECURSE(
    bin
    plugins
)
