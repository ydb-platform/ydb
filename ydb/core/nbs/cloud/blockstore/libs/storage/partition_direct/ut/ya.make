UNITTEST_FOR(ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct)

SRCS(
    base_test_fixture.cpp
    ddisk_data_copier_ut.cpp
    direct_block_group_impl_ut.cpp
    read_request_ut.cpp
    write_request_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/nbs/cloud/blockstore/libs/storage/testlib
    ydb/core/protos
    ydb/core/testlib
)

END()
