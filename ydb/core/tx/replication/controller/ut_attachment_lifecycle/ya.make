UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/core/wrappers/ut_helpers
    ydb/library/aws_init
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/import
    ydb/public/sdk/cpp/src/client/operation
    library/cpp/testing/unittest
)

SRCS(
    attachment_lifecycle_ut.cpp
    bootstrap_ut.cpp
    dst_delivery_retry_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
