LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/hash
    ydb/core/formats/arrow/serializer
    ydb/core/kqp/common/result_set_format
    ydb/library/actors/core
    ydb/library/formats/arrow
    ydb/library/services
    yql/essentials/minikql
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
)

YQL_LAST_ABI_VERSION()

SRCS(
    arrow_batch_builder.cpp
    arrow_helpers.cpp
    arrow_helpers_minikql.cpp
    arrow_filter.cpp
    converter.cpp
    converter.h
    permutations.cpp
    size_calcer.cpp
    special_keys.cpp
    process_columns.cpp
)

END()

RECURSE(
    accessor
    dictionary
    printer
    reader
    rows
    save_load
    splitter
    transformer
)

RECURSE_FOR_TESTS(
    ut
)
