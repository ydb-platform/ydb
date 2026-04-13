LIBRARY()

PEERDIR(
    yql/essentials/providers/common/proto
)

END()

RECURSE(
    dummy
    full
)

IF (NOT OPENSOURCE)
    RECURSE(
        yandex
    )
ENDIF()
