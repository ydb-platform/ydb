UNITTEST_FOR(ydb/services/workload_manager)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    has_app_name_ut.cpp
    action_reject_ut.cpp
    has_full_scan_matcher_ut.cpp
    has_full_scan_ut.cpp
    has_path_ddl_ut.cpp
    has_path_matcher_ut.cpp
    has_path_ut.cpp
    stream_query_classification_ut.cpp
    has_stream_matcher_ut.cpp
    has_stream_ut.cpp
    member_name_ut.cpp
    query_classifier_match_ut.cpp
    query_classifier_ut.cpp
    workload_service_actors_ut.cpp
    workload_service_query_sessions_ut.cpp
    workload_service_tables_ut.cpp
    workload_service_ut.cpp
)

PEERDIR(
    ydb/services/workload_manager/ut/common

    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
