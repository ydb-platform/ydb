RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/scheme
    ydb/library/arrow_kernels
    ydb/library/binary_json
    ydb/library/dynumber
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

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_batch_builder.cpp
    arrow_batch_builder.h
    arrow_helpers.cpp
    arrow_helpers.h
    clickhouse_block.h
    clickhouse_block.cpp
    custom_registry.cpp
    input_stream.h
    merging_sorted_input_stream.cpp
    merging_sorted_input_stream.h
    one_batch_input_stream.h
    sort_cursor.h
    factory.h
    program.cpp
    ssa_program_optimizer.cpp
)

END()
