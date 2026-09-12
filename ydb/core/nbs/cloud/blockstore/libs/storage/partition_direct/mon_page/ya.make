LIBRARY()

SRCS(
    mon_render.cpp
<<<<<<< HEAD
)

PEERDIR(
=======
    mon_render_chaos.cpp
    mon_render_memory.cpp
    mon_render_overview.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common/memory
    ydb/core/nbs/cloud/blockstore/libs/diagnostics
>>>>>>> 78a20188184 (Field map set optimization (#52565))
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    library/cpp/monlib/service/pages
)

END()

RECURSE_FOR_TESTS(
    ut
)
