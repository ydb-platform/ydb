LIBRARY()

PEERDIR(
    ydb/library/conclusion
    ydb/library/actors/core
    ydb/library/services
    ydb/core/formats/arrow/accessor/sub_columns
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
    collection.cpp
    functions.cpp
    aggr_keys.cpp
    aggr_common.cpp
    filter.cpp
    projection.cpp
    assign_const.cpp
    assign_internal.cpp
    chain.cpp
    custom_registry.cpp
    GLOBAL kernel_logic.cpp
)

GENERATE_ENUM_SERIALIZATION(abstract.h)
GENERATE_ENUM_SERIALIZATION(aggr_common.h)

END()
