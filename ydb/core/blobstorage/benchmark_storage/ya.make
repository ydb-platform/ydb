IF (OS_LINUX)
    PROGRAM(blobstorage-benchmark-storage)

    SRCS(main.cpp)

    PEERDIR(
        library/cpp/json
        library/cpp/protobuf/util
        ydb/core/blobstorage/dsproxy
        ydb/core/blobstorage/storagepoolmon
        ydb/core/blobstorage/ut_vdisk/lib
        ydb/core/load_test
        ydb/library/keys
        ydb/public/lib/ydb_cli/dump/util/view_query_dummy
        yql/essentials/sql/pg_dummy
    )

    END()
ENDIF()
