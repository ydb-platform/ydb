LIBRARY()

PEERDIR(
    ydb/library/conclusion
    ydb/library/actors/core
    ydb/library/services
    ydb/core/formats/arrow/accessor/sub_columns

    yql/essentials/core/arrow_kernels/registry
    yql/essentials/core/arrow_kernels/request
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins/llvm16

)

IF (OS_WINDOWS)
    ADDINCL(
        ydb/library/yql/udfs/common/clickhouse/client/base
        ydb/library/arrow_clickhouse
    )
ELSE()
    PEERDIR(
        ydb/library/arrow_clickhouse
    )
    ADDINCL(
        ydb/library/arrow_clickhouse
    )
ENDIF()

SRCS(
    abstract.cpp
    stream_logic.cpp
    visitor.cpp
    index.cpp
    header.cpp
    execution.cpp
    graph_optimization.cpp
    graph_execute.cpp
    original.cpp
    collection.cpp
    functions.cpp
    aggr_keys.cpp
    aggr_common.cpp
    filter.cpp
    projection.cpp
    assign_const.cpp
    assign_internal.cpp
    custom_registry.cpp
    GLOBAL kernel_logic.cpp
)

GENERATE_ENUM_SERIALIZATION(abstract.h)
GENERATE_ENUM_SERIALIZATION(aggr_common.h)
GENERATE_ENUM_SERIALIZATION(execution.h)

YQL_LAST_ABI_VERSION()

END()
