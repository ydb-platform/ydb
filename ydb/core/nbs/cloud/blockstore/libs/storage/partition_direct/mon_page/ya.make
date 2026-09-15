LIBRARY()

SRCS(
<<<<<<< HEAD
    mon_render.cpp
<<<<<<< HEAD
)

PEERDIR(
=======
=======
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    mon_render_chaos.cpp
    mon_render_dbg.cpp
    mon_render_local_db.cpp
    mon_render_memory.cpp
    mon_render_overview.cpp
    mon_render.cpp
    mon_util.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common/memory
    ydb/core/nbs/cloud/blockstore/libs/diagnostics
<<<<<<< HEAD
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
    library/cpp/monlib/service/pages
)

END()

RECURSE_FOR_TESTS(
    ut
)
