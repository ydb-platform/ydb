UNITTEST_FOR(ydb/core/kafka_proxy)

ADDINCL(
    ydb/core/kafka_proxy/ut
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ../kafka_test_client.cpp
    ../kafka_test_client.h
    ../test_server.cpp
    ../ut_auth.cpp
    ../ut_authz.cpp
    ../ut_protocol.cpp
)

PEERDIR(
    ydb/core/kafka_proxy
    ydb/core/security/certificate_check/test_utils
    ydb/core/persqueue/public/schema
    ydb/core/persqueue/ut/common
    ydb/core/testlib/actors
    ydb/core/testlib/default
    ydb/library/testlib/service_mocks
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    library/cpp/threading/future
)
YQL_LAST_ABI_VERSION()

ENV(INSIDE_YDB="1")

END()
