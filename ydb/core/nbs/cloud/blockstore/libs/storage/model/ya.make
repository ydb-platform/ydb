LIBRARY()

GENERATE_ENUM_SERIALIZATION(channel_data_kind.h)

SRCS(
    channel_data_kind.cpp
    counters_helpers.cpp
    disk_description.cpp
    log_prefix.cpp
    log_title.cpp
    volume_label.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/storage/core/libs/common

    library/cpp/string_utils/quote
)

END()

RECURSE_FOR_TESTS(
    ut
)
