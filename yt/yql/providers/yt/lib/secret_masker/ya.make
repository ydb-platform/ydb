LIBRARY()
END()

RECURSE(
    dummy
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()
