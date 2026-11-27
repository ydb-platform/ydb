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
    library/cpp/json
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/runtime/scheduler
    ydb/core/kqp/ut/common
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/core/resource_pools
    ydb/core/testlib/basics
    ydb/core/tx/scheme_cache
    ydb/core/tx/tx_proxy
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/proto
    ydb/public/lib/ut_helpers
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/services/metadata
    ydb/services/metadata/abstract
    ydb/services/metadata/ds_table
    ydb/services/workload_manager/actors
    ydb/services/workload_manager/common
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
    ydb/services/workload_manager/service
    ydb/services/workload_manager/tables
    ydb/services/workload_manager/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
