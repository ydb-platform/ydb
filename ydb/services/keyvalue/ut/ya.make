UNITTEST_FOR(ydb/services/keyvalue)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

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
    library/cpp/testing/hook
    library/cpp/threading/future
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/keyvalue
    ydb/core/protos
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/core/wrappers/ut_helpers
    ydb/library/aws_init
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/library/grpc/client
)

YQL_LAST_ABI_VERSION()

END()
