LIBRARY()

SRCS(
    bloom_filter_defaults.h
    flat_row_eggs.h
)

GENERATE_ENUM_SERIALIZATION(flat_row_eggs.h)

PEERDIR(
    ydb/core/base
)

END()
