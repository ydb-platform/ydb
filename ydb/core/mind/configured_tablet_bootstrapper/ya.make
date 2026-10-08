YQL_LIBRARY()

SRCS(
    configured_tablet_bootstrapper.cpp
    configured_tablet_bootstrapper.h
)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/backup/controller
    ydb/core/base
    ydb/core/blob_depot
    ydb/core/cms
    ydb/core/cms/console
    ydb/core/control
    ydb/core/kesus/tablet
    ydb/core/keyvalue
    ydb/core/mind
    ydb/core/mind/bscontroller
    ydb/core/mind/hive
    ydb/core/mon
    ydb/core/protos
    ydb/core/statistics/aggregator
    ydb/core/sys_view/processor
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/test_tablet
    ydb/core/tx/coordinator
    ydb/core/tx/datashard
    ydb/core/tx/mediator
    ydb/core/tx/replication/controller
    ydb/core/tx/schemeshard
    ydb/core/tx/sequenceshard
    ydb/core/tx/tx_allocator
    ydb/library/actors/core
)

DEFAULT(YDB_EMBEDDED_NBS_ENABLED yes)

IF (OS_LINUX AND YDB_EMBEDDED_NBS_ENABLED)
    CFLAGS(
        -DYDB_EMBEDDED_NBS_ENABLED
    )
    PEERDIR(
        ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller
    )
ENDIF()

END()
