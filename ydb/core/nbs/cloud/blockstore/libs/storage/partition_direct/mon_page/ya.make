LIBRARY()

SRCS(
    mon_render.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model

    ydb/core/base/services
    ydb/core/mind/bscontroller

    library/cpp/monlib/service/pages
    library/cpp/string_utils/quote
)

END()

RECURSE_FOR_TESTS(
    ut
)
