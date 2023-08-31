RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/scheme
    ydb/core/formats/arrow/serializer
    ydb/core/formats/arrow/simple_builder
    ydb/core/formats/arrow/dictionary
    ydb/core/formats/arrow/transformer
    library/cpp/actors/core
    ydb/library/arrow_kernels
    ydb/library/binary_json
    ydb/library/dynumber
    ydb/library/services
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
    arrow_filter.cpp
    arrow_helpers.cpp
    converter.cpp
    converter.h
    custom_registry.cpp
    input_stream.h
    merging_sorted_input_stream.cpp
    merging_sorted_input_stream.h
    one_batch_input_stream.h
    permutations.cpp
    program.cpp
    replace_key.cpp
    size_calcer.cpp
    sort_cursor.h
    ssa_program_optimizer.cpp
    special_keys.cpp
)

END()
