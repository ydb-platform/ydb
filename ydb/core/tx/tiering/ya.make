LIBRARY()

SRCS(
    common.cpp
    manager.cpp
    fetcher.cpp
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
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/schemeshard
    ydb/core/tx/tiering/tier
    ydb/core/tablet_flat/protos
    ydb/core/wrappers
    ydb/public/api/protos
    ydb/services/bg_tasks/abstract
    ydb/services/metadata
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)