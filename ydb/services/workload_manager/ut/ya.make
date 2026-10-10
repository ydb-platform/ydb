UNITTEST_FOR(ydb/services/workload_manager)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    action_reject_ut.cpp
    classifier_representation_ut.cpp
    gateway_ut.cpp
    has_app_name_ut.cpp
    has_full_scan_matcher_ut.cpp
    has_full_scan_ut.cpp
    has_path_ddl_ut.cpp
    has_path_matcher_ut.cpp
    has_path_ut.cpp
    has_shared_reading_matcher_ut.cpp
    has_shared_reading_ut.cpp
    has_stream_matcher_ut.cpp
    has_stream_ut.cpp
    member_name_ut.cpp
    query_classifier_match_ut.cpp
    query_classifier_ut.cpp
    stream_query_classification_ut.cpp
    workload_manager_state_actor_ut.cpp
    workload_service_actors_ut.cpp
    workload_service_query_sessions_ut.cpp
    workload_service_tables_ut.cpp
    workload_service_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    ydb/core/testlib/basics
    ydb/public/lib/ut_helpers
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/services/workload_manager/service
    ydb/services/workload_manager/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
