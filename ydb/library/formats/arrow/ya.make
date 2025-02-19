RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    accessor
    common
    switch
    csv
    validation
    hash    
    modifier
    scalar
    simple_builder
    splitter
    transformer
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/formats/arrow/simple_builder
    ydb/library/formats/arrow/transformer
    ydb/library/formats/arrow/splitter
    ydb/library/formats/arrow/modifier
    ydb/library/formats/arrow/scalar
    ydb/library/formats/arrow/hash
    ydb/library/actors/core
    ydb/library/arrow_kernels
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    ydb/library/services
    yql/essentials/core/arrow_kernels/request
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

CFLAGS(
    -Wno-unused-parameter
)

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_helpers.cpp
    input_stream.h
    permutations.cpp
    replace_key.cpp
    size_calcer.cpp
    simple_arrays_cache.cpp
)

END()
