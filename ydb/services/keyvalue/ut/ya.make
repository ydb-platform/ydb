UNITTEST_FOR(ydb/services/keyvalue)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/blobstorage/base
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/wrappers/ut_helpers
    ydb/library/aws_init
    ydb/services/keyvalue
)

YQL_LAST_ABI_VERSION()

END()
