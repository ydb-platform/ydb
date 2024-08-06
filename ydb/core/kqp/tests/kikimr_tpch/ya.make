UNITTEST()

#INCLUDE(${ARCADIA_ROOT}/kikimr/public/tools/ydb_recipe/recipe.inc)

DEPENDS(
    kikimr/driver
    kikimr/public/tools/ydb_recipe
    tools/python
    ydb/library/yql/udfs/common/datetime
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/pire
    ydb/library/yql/udfs/common/re2
    ydb/library/yql/udfs/common/string
)

USE_RECIPE(
    kikimr/public/tools/ydb_recipe/ydb_recipe
    --suppress-version-check
    #        --debug-logging KQP_YQL KQP_GATEWAY KQP_COMPUTE KQP_TASKS_RUNNER KQP_EXECUTER KQP_WORKER KQP_PROXY TABLET_EXECUTOR
)

SRCS(
    kqp_tpch_ut.cpp
)

PEERDIR(
    ydb/core/kqp/tests/tpch/lib
    library/cpp/testing/unittest
    ydb/core/protos
    ydb/library/grpc/client
    ydb/library/yql/public/udf/service/stub
    ydb/public/lib/yson_value
)

SIZE(MEDIUM)

END()
