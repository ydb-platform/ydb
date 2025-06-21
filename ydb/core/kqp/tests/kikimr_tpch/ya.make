UNITTEST()

ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

DEPENDS(
    ydb/public/tools/ydb_recipe
    ydb/library/yql/udfs/common/datetime
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/pire
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/string
)

SRCS(
    kqp_tpch_ut.cpp
)

PEERDIR(
    ydb/core/kqp/tests/tpch/lib
    library/cpp/testing/unittest
    ydb/core/protos
    ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/public/udf/service/stub
    ydb/public/lib/yson_value
)

SIZE(MEDIUM)

END()
