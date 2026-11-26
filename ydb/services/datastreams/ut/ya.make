UNITTEST_FOR(ydb/services/datastreams)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/json
    ydb/core/persqueue/ut/common
    ydb/core/security/certificate_check/test_utils
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/datastreams
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/library/kafka
    ydb/services/lib/sharding
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
