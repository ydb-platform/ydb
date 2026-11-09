UNITTEST_FOR(ydb/services/scheme_secret/export)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/protos
    ydb/core/testlib
    ydb/core/tx/replication/ut_helpers
    ydb/public/sdk/cpp/src/client/secret
)

SRCS(
    export_secret_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
