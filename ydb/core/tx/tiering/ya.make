LIBRARY()

SRCS(
    common.cpp
    manager.cpp
    GLOBAL external_data.cpp
    snapshot.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

PEERDIR(
    ydb/library/actors/core
    library/cpp/json/writer
    ydb/core/blobstorage
    ydb/core/protos
    ydb/core/tx/schemeshard
    ydb/core/tx/tiering/rule
    ydb/core/tx/tiering/tier
    ydb/core/tablet_flat/protos
    ydb/core/wrappers
    ydb/public/api/protos
    ydb/services/bg_tasks/abstract
    ydb/services/metadata
)

END()

RECURSE_FOR_TESTS(
    ut
)