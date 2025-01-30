LIBRARY()

    SRCS(
        blob_depot.cpp
        blob_depot.h
        defs.h
        types.h
        events.h
        schema.h

        agent.cpp
        assimilator.cpp
        assimilator.h
        blob_depot_tablet.h
        blocks.cpp
        blocks.h
        coro_tx.cpp
        coro_tx.h
        data.cpp
        data.h
        data_decommit.cpp
        data_gc.cpp
        data_load.cpp
        data_mon.cpp
        data_resolve.cpp
        data_resolve.h
        data_trash.cpp
        data_uncertain.cpp
        data_uncertain.h
        garbage_collection.cpp
        garbage_collection.h
        given_id_range.cpp
        group_metrics_exchange.cpp
        mon_main.cpp
        mon_main.h
        space_monitor.cpp
        space_monitor.h
        testing.cpp
        testing.h

        # operations
        op_apply_config.cpp
        op_init_schema.cpp
        op_load.cpp
        op_commit_blob_seq.cpp
    )

    PEERDIR(
        ydb/core/blobstorage/vdisk/common
        ydb/core/tablet_flat
        ydb/core/protos
    )

    GENERATE_ENUM_SERIALIZATION(schema.h)

END()

RECURSE(
    agent
)

RECURSE_FOR_TESTS(
    ut
)
