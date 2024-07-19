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
    ydb/core/formats/arrow/reader
    ydb/core/formats/arrow/modifier
    ydb/core/formats/arrow/scalar
    ydb/core/formats/arrow/hash
    ydb/library/actors/core
    ydb/library/arrow_kernels
    ydb/library/binary_json
    ydb/library/dynumber
    ydb/library/services
    ydb/library/yql/core/arrow_kernels/request
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
    permutations.cpp
    program.cpp
    replace_key.cpp
    size_calcer.cpp
    ssa_program_optimizer.cpp
    special_keys.cpp
    simple_arrays_cache.cpp
    process_columns.cpp
)

END()
