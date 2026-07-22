LIBRARY()

SRCS(
    dbs_controller.cpp
    dbs_controller_actor.cpp
    dbs_controller_database.cpp
    dbs_initschema.cpp
    dbs_loadstate.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/core
    ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/library/actors/core
    ydb/library/services
)

END()

RECURSE(
    protos
)

RECURSE_FOR_TESTS(
    ut
)
