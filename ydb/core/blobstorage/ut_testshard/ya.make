UNITTEST()

    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    TIMEOUT(600)

    REQUIREMENTS(
        ram:32
    )

    SRCS(
        main.cpp
    )

    PEERDIR(
        ydb/apps/version
        ydb/core/base
        ydb/core/blob_depot
        ydb/core/blobstorage/backpressure
        ydb/core/blobstorage/dsproxy/mock
        ydb/core/blobstorage/nodewarden
        ydb/core/blobstorage/pdisk/mock
        ydb/core/blobstorage/testing/group_overseer
        ydb/core/blobstorage/vdisk/common
        ydb/core/mind
        ydb/core/mind/bscontroller
        ydb/core/mind/hive
        ydb/core/sys_view/service
        ydb/core/test_tablet
        ydb/core/tx/scheme_board
        ydb/core/tx/tx_allocator
        ydb/core/tx/mediator
        ydb/core/tx/coordinator
        ydb/core/tx/scheme_board
        ydb/core/util
        ydb/library/yql/public/udf/service/stub
        ydb/library/yql/sql/pg_dummy
        library/cpp/testing/unittest
    )

END()
