UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(200)

REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    common.cpp
    datastreams_checkpoints_ut.cpp
    datastreams_ut.cpp
    datastreams_table_mode_ut.cpp
    datastreams_queries_restart_ut.cpp
    kqp_has_path_ut.cpp
    streaming_ddl_ut.cpp
    streaming_deferrd_commit_write_ut.cpp
    streaming_sys_view_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/threading/local_executor
    ydb/core/cms/console
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/core/sys_view/common
    ydb/core/protos
    ydb/core/testlib
    ydb/library/testlib/common
    ydb/library/testlib/pq_helpers
    ydb/library/testlib/s3_recipe_helper
    ydb/library/testlib/solomon_helpers
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/services/workload_manager/ut/common
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/udfs/common/yson2
)

ENV(YDB_FEATURE_FLAGS="enable_topic_deferred_publish")

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

END()
