LIBRARY()

SRCS(
    split.h
    key_access.cpp
    data_size.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
