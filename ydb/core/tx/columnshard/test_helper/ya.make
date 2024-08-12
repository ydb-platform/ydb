LIBRARY()

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow
    contrib/libs/apache/arrow
    ydb/library/actors/core
    ydb/core/tx/columnshard/blobs_action/bs
    ydb/core/tx/columnshard
    ydb/core/wrappers
)

SRCS(
    helper.cpp
    controllers.cpp
    columnshard_ut_common.cpp
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

YQL_LAST_ABI_VERSION()

END()

