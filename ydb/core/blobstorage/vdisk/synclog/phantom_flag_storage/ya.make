LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/util
)

SRCS(
    phantom_flag_storage_builder.cpp
    phantom_flag_storage_state.cpp
)

END()

# TODO: tests
# RECURSE_FOR_TESTS(
#     ut
# )
