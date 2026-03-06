UNITTEST_FOR(ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/core/tx/replication/ydb_proxy
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    worker_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
