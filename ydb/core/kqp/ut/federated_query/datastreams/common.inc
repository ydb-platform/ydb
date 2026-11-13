REQUIREMENTS(cpu:1)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCDIR(ydb/core/kqp/ut/federated_query/datastreams)

SRCS(common.cpp)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/threading/local_executor
    ydb/core/base
    ydb/core/cms/console
    ydb/core/fq/libs/checkpoint_storage/events
    ydb/core/fq/libs/ydb
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/core/protos
    ydb/core/sys_view/common
    ydb/core/testlib
    ydb/library/grpc/server/actors
    ydb/library/table_creator
    ydb/library/testlib/common
    ydb/library/testlib/pq_helpers
    ydb/library/testlib/s3_recipe_helper
    ydb/library/testlib/solomon_helpers
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/services/metadata/abstract
    ydb/services/workload_manager/ut/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/udfs/common/stat
    yql/essentials/udfs/common/yson2
)

ENV(YDB_FEATURE_FLAGS="enable_topic_deferred_publish")

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
YQL_LAST_ABI_VERSION()
