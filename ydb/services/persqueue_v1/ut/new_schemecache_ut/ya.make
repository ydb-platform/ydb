UNITTEST_FOR(ydb/services/persqueue_v1)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
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
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/persqueue/tests
    ydb/core/testlib/default
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/table
    ydb/services/persqueue_v1
)

YQL_LAST_ABI_VERSION()

END()
