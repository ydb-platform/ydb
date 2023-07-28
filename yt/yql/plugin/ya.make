LIBRARY()

SRCS(
    plugin.cpp
)

END()

RECURSE(
    bridge
)

IF (NOT OPENSOURCE)
    # We do not bring YQL with us into open source.
    RECURSE(
        dynamic
        native
    )
ENDIF()
