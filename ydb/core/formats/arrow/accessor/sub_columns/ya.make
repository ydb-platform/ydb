LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/accessor/abstract
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/accessor/sparsed
    ydb/core/formats/arrow/accessor/composite_serial
    ydb/core/formats/arrow/save_load
    ydb/core/formats/arrow/common
    ydb/library/formats/arrow
    ydb/library/formats/arrow/protos
    yql/essentials/types/binary_json
)

SRCS(
    GLOBAL constructor.cpp
    GLOBAL request.cpp
    data_extractor.cpp
    accessor.cpp
    direct_builder.cpp
    settings.cpp
    stats.cpp
    others_storage.cpp
    columns_storage.cpp
    iterators.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
