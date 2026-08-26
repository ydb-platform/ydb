UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct)

SRCS(
    base_test_fixture.cpp
    ddisk_data_copier_ut.cpp
    direct_block_group_impl_ut.cpp
    direct_block_group_mock.cpp
    direct_block_group_test_fixture.cpp
    direct_session_registry_ut.cpp
    erase_request_ut.cpp
    fast_path_service_ut.cpp
    flush_request_ut.cpp
    ic_direct_storage_transport_ut.cpp
    read_request_ut.cpp
    vchunk_ut.cpp
    write_request_test_fixture.cpp
    write_request_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib
    ydb/core/nbs/cloud/blockstore/libs/storage/testlib
    ydb/core/protos
    ydb/core/testlib
)

END()
