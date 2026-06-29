LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/serializer
    ydb/core/kqp/common/result_set_format
    ydb/core/scheme
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
    converter.cpp
    converter.h
    permutations.cpp
    process_columns.cpp
    size_calcer.cpp
    special_keys.cpp
)

END()

RECURSE(
    accessor
    container
    dictionary
    filter
    hash
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
