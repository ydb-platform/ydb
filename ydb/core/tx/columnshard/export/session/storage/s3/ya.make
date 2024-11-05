LIBRARY()

SRCS(
    GLOBAL storage.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/session/selector/abstract
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/wrappers
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ELSE()
    PEERDIR(
        ydb/core/tx/columnshard/blobs_action/tier
    )
ENDIF()

END()
