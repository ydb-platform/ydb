RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/scheme
    ydb/core/formats/arrow/accessor
    ydb/core/formats/arrow/serializer
    ydb/core/formats/arrow/dictionary
    ydb/core/formats/arrow/transformer
    ydb/core/formats/arrow/reader
    ydb/core/formats/arrow/save_load
    ydb/core/formats/arrow/splitter
    ydb/core/formats/arrow/hash
    ydb/library/actors/core
    ydb/library/arrow_kernels
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    ydb/library/formats/arrow
    ydb/library/services
    yql/essentials/core/arrow_kernels/request
)

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_batch_builder.cpp
    arrow_helpers.cpp
    arrow_filter.cpp
    converter.cpp
    converter.h
    permutations.cpp
    size_calcer.cpp
    special_keys.cpp
    process_columns.cpp
)

END()
