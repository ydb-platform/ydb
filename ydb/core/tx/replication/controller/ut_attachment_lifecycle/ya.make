UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    attachment_lifecycle_ut.cpp
    dst_delivery_retry_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
