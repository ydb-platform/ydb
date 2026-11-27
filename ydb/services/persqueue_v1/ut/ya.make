UNITTEST_FOR(ydb/services/persqueue_v1)

ADDINCL(
    ydb/public/sdk/cpp
)

CFLAGS(
    -DYDB_SDK_USE_STD_STRING
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    persqueue_ut.cpp
    persqueue_common_ut.cpp
    persqueue_compat_ut.cpp
    first_class_src_ids_ut.cpp
    topic_yql_ut.cpp

    test_utils.h
    pq_data_writer.h
    api_test_setup.h
    rate_limiter_test_setup.h
    rate_limiter_test_setup.cpp
    functions_executor_wrapper.h
    functions_executor_wrapper.cpp
    topic_service_ut.cpp
    topic_deferred_publish_ut.cpp
    persqueue_v1_gaps_ut.cpp
    read_init_auth_ut.cpp
    read_session_multi_topic_ut.cpp
    read_init_auth_actor_ut.cpp
    demo_tx.cpp

    fill_batched_data_offset_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/digest/md5
    library/cpp/json
    library/cpp/logger
    library/cpp/monlib/dynamic_counters
    ydb/core/base
    ydb/core/cms/console
    ydb/core/grpc_services/local_rpc
    ydb/core/mon
    ydb/core/persqueue/deferred_publish
    ydb/core/persqueue/public/cluster_tracker
    ydb/core/persqueue/ut/common
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/tablet
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/library/persqueue
    ydb/library/persqueue/tests
    ydb/library/persqueue/topic_parser
    ydb/library/services
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    ydb/services/persqueue_v1/actors
)

YQL_LAST_ABI_VERSION()

END()
