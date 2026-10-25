UNITTEST_FOR(ydb/core/tx/replication/ydb_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
        REQUIREMENTS(cpu:1)
    ELSE()
        REQUIREMENTS(cpu:2)
    ENDIF()
ENDIF()

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/core/tx/replication/ydb_proxy/local_proxy
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    partition_end_watcher_ut.cpp
    ydb_proxy_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
