UNITTEST_FOR(ydb/core/grpc_services)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/persqueue/public/schema
    ydb/core/protos
    ydb/core/testlib
    ydb/core/tx/replication/ut_helpers
    ydb/core/tx/scheme_cache
    ydb/public/sdk/cpp/src/client/secret
)

SRCS(
    export_secret_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
