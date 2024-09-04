UNITTEST()

ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

DEPENDS(
    ydb/apps/ydbd
    ydb/public/tools/ydb_recipe
    ydb/library/yql/udfs/common/datetime
    ydb/library/yql/udfs/common/datetime2
    ydb/library/yql/udfs/common/pire
    ydb/library/yql/udfs/common/re2
    ydb/library/yql/udfs/common/string
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
