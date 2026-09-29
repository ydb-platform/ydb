#include "volume_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeDatabaseTest)
{
    Y_UNIT_TEST(ShouldStoreAndReadPartitionTabletId)
    {
        NBlockStore::NStorage::TTestExecutor executor;
        const ui64 written = 72075186224037888;

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
                ui64 read = 0;
                UNIT_ASSERT(volumeDb.ReadPartitionTabletId(&read));
                UNIT_ASSERT_VALUES_EQUAL(0u, read);
            });

        executor.WriteTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                volumeDb.StorePartitionTabletId(written);
            });

        executor.ReadTx(
            [&](NKikimr::NTable::TDatabase& db)
            {
                TVolumeDatabase volumeDb(db);
                ui64 read = 0;
                UNIT_ASSERT(volumeDb.ReadPartitionTabletId(&read));
                UNIT_ASSERT_VALUES_EQUAL(written, read);
            });
    }
}

}   // namespace NYdb::NBS::NStorage
