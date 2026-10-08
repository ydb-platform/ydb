#include "volume_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using NYdb::NBS::NBlockStore::NStorage::TTestExecutor;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeDatabaseTest)
{
    Y_UNIT_TEST(ShouldReadStoreAndOverwritePartitionTabletId)
    {
        TTestExecutor executor;

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                volumeDb.InitSchema();
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                ui64 partitionTabletId = 1;
                UNIT_ASSERT(volumeDb.ReadPartitionTabletId(&partitionTabletId));
                UNIT_ASSERT_VALUES_EQUAL(0u, partitionTabletId);
            });

        constexpr ui64 StoredId = 42;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                volumeDb.StorePartitionTabletId(StoredId);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                ui64 partitionTabletId = 0;
                UNIT_ASSERT(volumeDb.ReadPartitionTabletId(&partitionTabletId));
                UNIT_ASSERT_VALUES_EQUAL(StoredId, partitionTabletId);
            });

        constexpr ui64 OverwrittenId = 77;
        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                volumeDb.StorePartitionTabletId(OverwrittenId);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                ui64 partitionTabletId = 0;
                UNIT_ASSERT(volumeDb.ReadPartitionTabletId(&partitionTabletId));
                UNIT_ASSERT_VALUES_EQUAL(OverwrittenId, partitionTabletId);
            });
    }
}

}   // namespace NYdb::NBS::NStorage
