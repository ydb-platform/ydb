LIBRARY()
END()

RECURSE(
    dummy
    full
    proto
)

IF (NOT OPENSOURCE)
    RECURSE(
        yandex
    )
ENDIF()
