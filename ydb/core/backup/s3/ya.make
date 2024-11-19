LIBRARY()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ELSE()
    SRCS(
        extstorage_usage_config.cpp
    )
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/random_provider
    ydb/core/backup/common
    ydb/core/base
    ydb/core/protos
    ydb/core/wrappers
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/services
)

END()
