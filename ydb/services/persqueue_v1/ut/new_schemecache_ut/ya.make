UNITTEST_FOR(ydb/services/persqueue_v1)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    persqueue_new_schemecache_ut.cpp
    persqueue_common_new_schemecache_ut.cpp
    ut/api_test_setup.h
    ut/test_utils.h
    ut/pq_data_writer.h
    ut/rate_limiter_test_setup.h
    ut/rate_limiter_test_setup.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/json
    library/cpp/logger
    ydb/core/mon
    ydb/core/persqueue/public/cluster_tracker
    ydb/core/tablet
    ydb/core/testlib/default
    ydb/library/persqueue/tests
    ydb/library/persqueue/topic_parser
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    ydb/services/persqueue_v1/actors
)

YQL_LAST_ABI_VERSION()

END()
