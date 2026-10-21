UNITTEST_FOR(ydb/services/persqueue_v1)

ADDINCL(
    ydb/public/sdk/cpp
)

CFLAGS(
    -DYDB_SDK_USE_STD_STRING
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()
TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/persqueue/events
    ydb/core/testlib/default
    ydb/core/testlib/actors
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/services/persqueue_v1
)

SRCS(
    tablet_restart_session_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
