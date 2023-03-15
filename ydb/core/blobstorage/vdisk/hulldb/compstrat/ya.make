LIBRARY()

PEERDIR(
    ydb/core/blobstorage/vdisk/hulldb/base
    ydb/core/blobstorage/vdisk/hulldb/generic
    ydb/core/protos
)

SRCS(
    defs.h
    hulldb_compstrat_balance.h
    hulldb_compstrat_defs.cpp
    hulldb_compstrat_defs.h
    hulldb_compstrat_delsst.h
    hulldb_compstrat_lazy.h
    hulldb_compstrat_promote.h
    hulldb_compstrat_ratio.h
    hulldb_compstrat_selector.cpp
    hulldb_compstrat_selector.h
    hulldb_compstrat_space.h
    hulldb_compstrat_squeeze.h
    hulldb_compstrat_utils.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
